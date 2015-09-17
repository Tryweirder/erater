-module(erater_counter).
-behavior(gen_server).

-export([start_link/3, run/3]).
-export([acquire/2, async_acquire/3]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-export([set_config/2, set_config/3, config_fingerprint/1]).

-type skewed_time() :: {Time::integer(), Skew::integer()}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Group, Name, Config) ->
    gen_server:start_link(?MODULE, [Group, Name, Config], []).

run(Group, Name, Config) ->
    {ok, State0, Timeout} = init([Group, Name, Config]),
    gen_server:enter_loop(erater_counter, [], State0, Timeout).


acquire(Counter, MaxWait) ->
    case gen_server:call(Counter, {schedule, MaxWait}) of
        {ok, Wait} -> % OK, just convert server slots to milliseconds
            {ok, Wait};
        {error, overflow} -> % Unable to acquire free slot in reasonable time
            {error, overflow}
    end.

async_acquire(Counter, MaxWait, {Pid, _Ref} = ReturnPath) when is_pid(Pid) ->
    do_async_acquire(Counter, MaxWait, ReturnPath);
async_acquire(Counter, MaxWait, {mfa, _, _, _} = ReturnPath) ->
    do_async_acquire(Counter, MaxWait, ReturnPath).

do_async_acquire(Counter, MaxWait, ReturnPath) ->
    Counter ! {schedule, MaxWait, ReturnPath}.

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
-record(counter, {
        group,
        name, 
        last_sk_time,
        last_value,
        max_value,      % burst capacity
        ttl             % max time to live since last activity, milliseconds
        }).

init([Group, Name, Config]) ->
    put(erater_counter, {Group, Name}),
    % Construct initial state
    State0 = #counter{
            group = Group,
            name = Name,
            last_sk_time = {0, 0},
            last_value = 0
            },
    State = #counter{ttl = TTL} = do_set_config(Config, State0),
    {ok, State, TTL}.


handle_call({set_config, Config}, _From, #counter{} = State) ->
    reply_ttl(ok, do_set_config(Config, State));

handle_call({schedule, MaxWait}, _From, #counter{group = Group, last_sk_time = SkTime, last_value = Value, max_value = MaxValue} = State) ->
    {CurrentTime, MaxTime, Tick_ms, CurrentSkew} = erater_timeserver:get_time_range_tickms_skew(Group, MaxWait),
    CurrentSkTime = {CurrentTime, CurrentSkew},
    MaxSkTime = {MaxTime, CurrentSkew},
    case handle_schedule(CurrentSkTime, MaxSkTime, SkTime, Value, MaxValue) of
        {ok, NewSkTime, NewValue} ->
            WaitTime = sk_time_diff_nonneg(NewSkTime, CurrentSkTime, Tick_ms),
            NewState = State#counter{last_sk_time = NewSkTime, last_value = NewValue},
            reply_ttl({ok, WaitTime}, NewState);
        {error, _} = Error ->
            reply_ttl(Error, State)
    end.

handle_cast(_, #counter{} = State) ->
    noreply_ttl(State).

handle_info({schedule, MaxWait, {Pid, Ref} = ReturnPath}, #counter{} = State) when is_pid(Pid) ->
    { reply, Response, NextState, _} = handle_call({schedule, MaxWait}, ReturnPath, State),
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
    {MaxValue, TTL} = extract_config(Config),
    State#counter{
        max_value = MaxValue,
        ttl = TTL
        }.

%% Extract important config values from provided config
extract_config(Config) ->
    MaxValue = erater_config:capacity(Config),
    TTL = erater_config:ttl(Config),
    {MaxValue, TTL}.

%% @doc Counter extrapolation and update
%% Counter is stored as pair (Time, Value)
%% Each hit increments Value by 1
%% Each time slot decrements Value by 1
%% Time goes only forward
%% Value may have range of [0; MaxValue], 0 means completely fresh counter, MaxValue — counter exausted at stored Time

-spec handle_schedule(CurrentSkTime::skewed_time(), MaxSkTime::skewed_time(), SkTime::skewed_time(), Value::integer(), MaxValue::integer()) ->
    {ok, NewSkTime::skewed_time(), NewValue::integer()} | {error, any()}.
% Handle fully replenished counter
handle_schedule({_, _} = CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _MaxValue)
        when {Time+Value, MySkew} =< CurrentSkTime -> % MyTime + Val * SlotTime =< CurTime -- time to fully reset the counter
    {ok, CurrentSkTime, 1};

% Handle outdated counter
handle_schedule({_, _} = CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _MaxValue)
        when {Time+1, MySkew} =< CurrentSkTime -> % Incremented previous time is still less than current one
    % How much full slots elapsed since last update?
    TimeSlots = sk_time_slotdiff(CurrentSkTime, {Time, MySkew}),
    % Add slots to time, substract slots from value, increment value
    MyNewSkTime = {Time + TimeSlots, MySkew},
    NewValue = Value - TimeSlots + 1,
    {ok, MyNewSkTime, NewValue};

% Catch overflows
handle_schedule({_, _} = _CurrentSkTime, {_, _} = MaxSkTime, {_, _} = SkTime, _Value, _MaxValue) when SkTime > MaxSkTime ->
    % Cannot acces counter beyond given time range
    {error, overflow};
handle_schedule({_, _} = _CurrentSkTime, {_, _} = MaxSkTime, {Time, MySkew}, Value, MaxValue)
        when ({Time+1, MySkew} > MaxSkTime) andalso Value >= MaxValue ->
    % Counter is full at max time, unable to increment
    {error, overflow};

% Here we are sure that we can schedule counter update
handle_schedule({_, _} = _CurrentSkTime, {_, _} = _MaxSkTime, {_, _} = SkTime, Value, MaxValue) when Value < MaxValue ->
    % Counter time is up-to-date or in future, but we can increment Value at that time
    {ok, SkTime, Value + 1};
handle_schedule({_, _} = _CurrentSkTime, {_, _} = _MaxSkTime, {Time, MySkew}, Value, _MaxValue) ->
    % Counter time is up-to-date or in future, we cannot increment Value, so increment time
    {ok, {Time + 1, MySkew}, Value}.

