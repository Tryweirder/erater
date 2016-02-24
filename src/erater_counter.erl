-module(erater_counter).
-behavior(gen_server).

-export([start_link/3, run/3]).
-export([acquire/2, acquire/3]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-export([set_config/2, set_config/3, config_fingerprint/1]).

-type skewed_time() :: {Time::integer(), Skew::integer()}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Group, Name, Config) ->
    State0 = seed_state(Group, Name, Config),
    gen_server:start_link(?MODULE, State0, []).

run(Group, Name, Config) ->
    State0 = seed_state(Group, Name, Config),
    {ok, State0, Timeout} = init(State0),
    gen_server:enter_loop(erater_counter, [], State0, Timeout).


acquire(Counter, MaxWait) ->
    sync_acquire(Counter, MaxWait, []).

acquire(Counter, MaxWait, Options) ->
    case proplists:get_value(async, Options) of
        undefined -> sync_acquire(Counter, MaxWait, Options);
        Async -> async_acquire(Counter, MaxWait, Async, Options)
    end.

sync_acquire(Counter, MaxWait, Options) ->
    case gen_server:call(Counter, {schedule, MaxWait, Options}) of
        {ok, Wait} -> % OK, just convert server slots to milliseconds
            {ok, Wait};
        {error, overflow} -> % Unable to acquire free slot in reasonable time
            {error, overflow}
    end.

async_acquire(Counter, MaxWait, {Pid, _Ref} = ReturnPath, Options) when is_pid(Pid) ->
    do_async_acquire(Counter, MaxWait, ReturnPath, Options);
async_acquire(Counter, MaxWait, {mfa, _, _, _} = ReturnPath, Options) ->
    do_async_acquire(Counter, MaxWait, ReturnPath, Options).

do_async_acquire(Counter, MaxWait, ReturnPath, Options) ->
    Counter ! {schedule, MaxWait, ReturnPath, Options}.

%% On-the-fly counter reconfiguration
set_config(Counter, Config) ->
    set_config(Counter, Config, sync).

set_config(Counter, Config, sync) ->
    set_config(Counter, Config, 5000);
set_config(Counter, Config, Timeout) when is_integer(Timeout) ->
    gen_server:call(Counter, {set_config, Config}, Timeout);
set_config(Counter, Config, async) ->
    Counter ! {set_config, Config},
    ok.

%% Let the manager determine if it needs to reconfigure all counters
config_fingerprint(Config) ->
    extract_config(Config).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(timecfg, {
        rps,
        ref_micros,
        slot_micros }).
-type timecfg() :: #timecfg{}.

-record(counter, {
        mode = group :: group | {adhoc, timecfg()},
        group,
        name, 
        last_sk_time,
        last_value,
        burst,          % max possible counter value
        ttl             % max time to live since last activity, milliseconds
        }).

seed_state(Group, Name, Config) ->
    State0 = #counter{
        mode = seed_mode(Config),
        group = Group,
        name = Name,
        last_sk_time = {0, 0},
        last_value = 0
        },
    #counter{} = do_set_config(Config, State0).

seed_mode(Config) ->
    case proplists:get_value(mode, Config, group) of
        group ->
            group;
        adhoc ->
            {adhoc, seed_timecfg(Config)}
    end.

seed_timecfg(Config) ->
    RPS = proplists:get_value(rps, Config),
    #timecfg{
        rps = RPS,
        ref_micros = erlang:system_time(micro_seconds),
        slot_micros = round(1000000/RPS)
        }.

set_rps(RPS, #timecfg{rps = RPS} = OldCfg) ->
    % Rps matches the old value, no need to do math
    OldCfg;
set_rps(RPS, #timecfg{ref_micros = OldRef, slot_micros = OldSlot}) ->
    CurTime = erlang:system_time(micro_seconds),
    CurSlots = (CurTime - OldRef) div OldSlot,
    CurSlotBase = OldRef + CurSlots*OldSlot,

    NewSlot = round(1000000/RPS),
    NewRef = CurSlotBase - NewSlot*CurSlots,
    #timecfg{
        rps = RPS,
        ref_micros = NewRef,
        slot_micros = NewSlot }.


init(#counter{name = Name, group = Group, ttl = TTL} = State) ->
    put(erater_counter, {Group, Name}),
    % Construct initial state
    {ok, State, TTL}.


handle_call({set_config, Config}, _From, #counter{} = State) ->
    reply_ttl(ok, do_set_config(Config, State));

handle_call({schedule, MaxWait, Options}, _From, #counter{last_sk_time = SkTime, last_value = Value, burst = Burst} = State0) ->
    State = update_config(Options, State0),
    {CurrentTime, MaxTime, Tick_ms, CurrentSkew} = get_time_info(MaxWait, State),
    CurrentSkTime = {CurrentTime, CurrentSkew},
    MaxSkTime = {MaxTime, CurrentSkew},
    case handle_schedule(CurrentSkTime, MaxSkTime, SkTime, Value, Burst) of
        {ok, NewSkTime, NewValue} ->
            WaitTime = sk_time_diff_nonneg(NewSkTime, CurrentSkTime, Tick_ms),
            NewState = State#counter{last_sk_time = NewSkTime, last_value = NewValue},
            reply_ttl({ok, WaitTime}, NewState);
        {error, _} = Error ->
            reply_ttl(Error, State)
    end.

handle_cast(_, #counter{} = State) ->
    noreply_ttl(State).

handle_info({schedule, MaxWait, {Pid, Ref} = ReturnPath, Options}, #counter{} = State) when is_pid(Pid) ->
    { reply, Response, NextState, _} = handle_call({schedule, MaxWait, Options}, ReturnPath, State),
    Pid ! {erater_response, Ref, Response},
    noreply_ttl(NextState);

handle_info({schedule, MaxWait, {mfa, Mod, Fun, Args}}, #counter{} = State) ->
    { reply, Response, NextState, _} = handle_call({schedule, MaxWait}, {undefined, undefined}, State),
    erlang:apply(Mod, Fun, [Response|Args]),
    noreply_ttl(NextState);

handle_info({set_config, Config}, #counter{} = State) ->
    noreply_ttl(do_set_config(Config, State));

handle_info(timeout, #counter{} = State) ->
    {stop, {shutdown, ttl_exceeded}, State};
handle_info(_, State) ->
    noreply_ttl(State).


reply_ttl(Reply, #counter{ttl = TTL} = State) ->
    {reply, Reply, State, TTL}.

noreply_ttl(#counter{ttl = TTL} = State) ->
    {noreply, State, TTL}.


terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% {CurrentTime, MaxTime, Tick_ms, CurrentSkew} = get_time_info(MaxWait, State),
get_time_info(MaxWait, #counter{mode = group, group = Group}) ->
    erater_timeserver:get_time_range_tickms_skew(Group, MaxWait);
get_time_info(MaxWait, #counter{mode = {adhoc, #timecfg{} = TimeCfg}}) ->
    #timecfg{
        slot_micros = SlotMicros,
        ref_micros = RefMicros
        } = TimeCfg,
    CurMicros = erlang:system_time(micro_seconds),
    RelMicros = CurMicros - RefMicros,

    CurrentTime = RelMicros div SlotMicros,
    MaxTime = (RelMicros + MaxWait*1000) div SlotMicros,
    Tick_ms = SlotMicros div 1000,
    CurrentSkew = (RelMicros - CurrentTime*SlotMicros) div 1000,

    {CurrentTime, MaxTime, Tick_ms, CurrentSkew}.


sk_time_diff({Time1, Skew1}, {Time2, Skew2}, Tick_ms) ->
    Tick_ms * (Time1 - Time2) + (Skew1 - Skew2).

sk_time_diff_nonneg({_, _} = SkTime1, {_, _} = SkTime2, _) when SkTime1 < SkTime2 ->
    0;
sk_time_diff_nonneg(SkTime1, SkTime2, Tick_ms) ->
    sk_time_diff(SkTime1, SkTime2, Tick_ms).

sk_time_slotdiff({Time1, Skew1}, {Time2, Skew2}) when Skew1 >= Skew2 ->
    Time1 - Time2;
sk_time_slotdiff({Time1, _Skew1}, {Time2, _Skew2}) ->
    Time1 - Time2 - 1.

do_set_config(Config, #counter{} = State) ->
    % Construct initial state
    {Burst, TTL} = extract_config(Config),
    State#counter{
        burst = Burst,
        ttl = TTL
        }.

%% Extract important config values from provided config
extract_config(Config) ->
    Burst = erater_config:burst(Config),
    TTL = erater_config:ttl(Config),
    {Burst, TTL}.

update_config(Options, #counter{} = State) ->
    lists:foldl(fun update_config_value/2, State, Options).

update_config_value({ttl, TTL}, State) ->
    State#counter{ttl = TTL};
update_config_value({burst, Burst}, State) ->
    State#counter{burst = Burst};
update_config_value({rps, RPS}, #counter{mode = {adhoc, TimeCfg}} = State) ->
    NewTimeCfg = set_rps(RPS, TimeCfg),
    State#counter{mode = {adhoc, NewTimeCfg}};
update_config_value(_, State) ->
    State.


%% @doc Counter extrapolation and update
%% Counter is stored as pair (Time, Value)
%% Each hit increments Value by 1
%% Each time slot decrements Value by 1
%% Time goes only forward
%% Value may have range of [0; Burst], 0 means completely fresh counter, Burst — counter exausted at stored Time

-spec handle_schedule(CurrentSkTime::skewed_time(), MaxSkTime::skewed_time(), SkTime::skewed_time(), Value::integer(), Burst::integer()) ->
    {ok, NewSkTime::skewed_time(), NewValue::integer()} | {error, any()}.
% Handle fully replenished counter
handle_schedule({_, _} = CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _Burst)
        when {Time+Value, MySkew} =< CurrentSkTime -> % MyTime + Val * SlotTime =< CurTime -- time to fully reset the counter
    {ok, CurrentSkTime, 1};

% Handle outdated counter
handle_schedule({_, _} = CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _Burst)
        when {Time+1, MySkew} =< CurrentSkTime -> % Incremented previous time is still less than current one
    % How much full slots elapsed since last update?
    TimeSlots = sk_time_slotdiff(CurrentSkTime, {Time, MySkew}),
    % Add slots to time, substract slots from value, increment value
    MyNewSkTime = {Time + TimeSlots, MySkew},
    NewValue = Value - TimeSlots + 1,
    {ok, MyNewSkTime, NewValue};

% Catch overflows
handle_schedule({_, _} = _CurrentSkTime, {_, _} = MaxSkTime, {_, _} = SkTime, _Value, _Burst) when SkTime > MaxSkTime ->
    % Cannot acces counter beyond given time range
    {error, overflow};
handle_schedule({_, _} = _CurrentSkTime, {_, _} = MaxSkTime, {Time, MySkew}, Value, Burst)
        when ({Time+1, MySkew} > MaxSkTime) andalso Value >= Burst ->
    % Counter is full at max time, unable to increment
    {error, overflow};

% Here we are sure that we can schedule counter update
handle_schedule({_, _} = _CurrentSkTime, {_, _} = _MaxSkTime, {_, _} = SkTime, Value, Burst) when Value < Burst ->
    % Counter time is up-to-date or in future, but we can increment Value at that time
    {ok, SkTime, Value + 1};
handle_schedule({_, _} = _CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _Burst) ->
    % Counter time is up-to-date or in future, we cannot increment Value, so increment time
    {ok, {Time + 1, MySkew}, Value}.

