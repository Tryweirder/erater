-module(erater_counter).
-behavior(gen_server).

-export([start_link/3, run/3]).
-export([acquire/2, async_acquire/3]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(counter, {
        group,
        name, 
        last_time,
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
            last_time = 0,
            last_value = 0
            },
    State = #counter{ttl = TTL} = set_config(Config, State0),
    {ok, State, TTL}.


handle_call({set_config, Config}, _From, #counter{} = State) ->
    reply_ttl(ok, set_config(Config, State));

handle_call({schedule, MaxWait}, _From, #counter{group = Group, last_time = Time, last_value = Value, max_value = MaxValue} = State) ->
    {CurrentTime, MaxTime, Tick_ms} = erater_timeserver:get_time_range_tickms(Group, MaxWait),
    case handle_schedule(CurrentTime, MaxTime, Time, Value, MaxValue) of
        {ok, NewTime, NewValue} ->
            WaitTime = Tick_ms * (NewTime - CurrentTime),
            NewState = State#counter{last_time = NewTime, last_value = NewValue},
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
set_config(Config, #counter{} = State) ->
    MaxValue = erater_config:capacity(Config),
    TTL = erater_config:die_after(Config),
    % Construct initial state
    State#counter{
        max_value = MaxValue,
        ttl = TTL
        }.

%% @doc Counter extrapolation and update
%% Counter is stored as pair (Time, Value)
%% Each hit increments Value by 1
%% Each time slot decrements Value by 1
%% Time goes only forward
%% Value may have range of [0; MaxValue], 0 means completely fresh counter, MaxValue — counter exausted at stored Time

-spec handle_schedule(CurrentTime::integer(), MaxTime::integer(), Time::integer(), Value::integer(), MaxValue::integer()) ->
    {ok, NewTime::integer(), NewValue::integer()} | {error, any()}.
% Handle outdated counter
handle_schedule(CurrentTime, _MaxTime, Time, Value, _MaxValue) when Time < CurrentTime ->
    % First, extrapolate current value
    TimeSteps = CurrentTime - Time,
    CurrentValue = max(0, Value - TimeSteps),
    % Then increment current value by 1. Even if it becomes larger than MaxValue it will not increase
    {ok, CurrentTime, CurrentValue + 1};

% Catch overflows
handle_schedule(_CurrentTime, MaxTime, Time, _Value, _MaxValue) when Time > MaxTime ->
    % Cannot acces counter beyond given time range
    {error, overflow};
handle_schedule(_CurrentTime, MaxTime, Time, Value, MaxValue) when Time == MaxTime andalso Value >= MaxValue ->
    % Counter is full at max time, unable to increment
    {error, overflow};

% Here we are sure that we can schedule counter update
handle_schedule(_CurrentTime, _MaxTime, Time, Value, MaxValue) when Value < MaxValue ->
    % Counter time is up-to-date or in future, but we can increment Value at that time
    {ok, Time, Value + 1};
handle_schedule(_CurrentTime, _MaxTime, Time, Value, _MaxValue) ->
    % Counter time is up-to-date or in future, we cannot increment Value, so increment time
    {ok, Time + 1, Value}.

