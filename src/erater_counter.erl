-module(erater_counter).
-behavior(gen_server).

-export([start_link/2, run/2]).
-export([acquire/3, async_acquire/4]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Name, Config) ->
    gen_server:start_link(?MODULE, [Name, Config], []).

run(Name, Config) ->
    {ok, State0} = init([Name, Config]),
    gen_server:enter_loop(erater_counter, [], State0).


acquire(Counter, RPS, MaxWait) ->
    % Here we do much computations on client side so that server is not overloaded
    Timestamp = os:timestamp(),
    Time = get_time(Timestamp, RPS),
    SlotMillis = 1000 div RPS,
    MaxWaitSlots = MaxWait div SlotMillis,
    MaxTime = Time + MaxWaitSlots,

    case gen_server:call(Counter, {schedule, epoch(Timestamp), RPS, Time, MaxTime}) of
        {ok, SlotsToWait} -> % OK, just convert server slots to milliseconds
            {ok, SlotsToWait * SlotMillis};
        {error, overflow} -> % Unable to acquire free slot in reasonable time
            {error, overflow};
        {error, {epoch_mismatch, _}} -> % This happens when Megaseconds increase, so retry
            timer:sleep(5),
            acquire(Counter, RPS, MaxWait);
        {error, {rps_mismatch, ServerRPS}} -> % User has specified wrong RPS, re-run with right one
            acquire(Counter, ServerRPS, MaxWait)
    end.

async_acquire(Counter, RPS, MaxWait, {Pid, _Ref} = ReturnPath) when is_pid(Pid) ->
    do_async_acquire(Counter, RPS, MaxWait, ReturnPath);
async_acquire(Counter, RPS, MaxWait, {mfa, _, _, _} = ReturnPath) ->
    do_async_acquire(Counter, RPS, MaxWait, ReturnPath).

do_async_acquire(Counter, RPS, MaxWait, ReturnPath) ->
    Timestamp = os:timestamp(),
    Time = get_time(Timestamp, RPS),
    SlotMillis = 1000 div RPS,
    MaxWaitSlots = MaxWait div SlotMillis,
    MaxTime = Time + MaxWaitSlots,

    Counter ! {schedule, epoch(Timestamp), RPS, Time, MaxTime, ReturnPath}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
epoch() ->
    epoch(os:timestamp()).
epoch({MegaSec, _, _}) ->
    MegaSec.
epoch_size(RPS) ->
    1000000 * RPS.

%get_time(RPS) ->
%    get_time(os:timestamp(), RPS).
get_time({_, Seconds, USeconds}, RPS) ->
    UStep = 1000000 div RPS,
    (Seconds * RPS) + (USeconds div UStep).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(counter, {
        name, 
        last_time,
        last_value,
        rps,
        epoch,          % Epoch for smaller counters, actually megaseconds from timestamp
        max_value,      % burst capacity
        ttl,            % max time to live since last activity, slots
        check_interval, % interval to check ttl, milliseconds
        check_timer     % check timer ref
        }).

init([Name, Config]) ->
    RPS = erater_config:rps(Config),
    MaxValue = erater_config:capacity(Config),
    {TTL, CheckInterval} = case erater_config:die_after(Config) of
        infinity ->
            % Infinite lifetime, bad option, but possible
            {infinity, undefined};
        DieAfter when is_integer(DieAfter) ->
            % TTL is (DieAfter ms)/(1000 ms/s)*(RPS slots/s)
            % CheckInterval is DieAfter/10, but not less than 1 second
            {(DieAfter*RPS) div 1000, max(DieAfter div 10, 1000)};
        undefined ->
            % By default, timer lives 100 times full replenish time (100*MaxValue slots)
            {100*MaxValue, (10000*MaxValue) div RPS}
    end,
    % Construct initial state
    State = #counter{
            name = Name,
            last_time = 0,
            last_value = 0,
            rps = RPS,
            epoch = epoch(),
            max_value = MaxValue,
            ttl = TTL,
            check_interval = CheckInterval
            },
    {ok, set_check_timer(State)}.

handle_call({schedule, _Epoch, BadRPS, _CurrentTime, _MaxTime}, _From, #counter{rps = RPS} = State) when BadRPS /= RPS ->
    {reply, {error, {rps_mismatch, RPS}}, State};
handle_call({schedule, NewEpoch, _RPS, _CurrentTime, _MaxTime} = Call, From, #counter{epoch = Epoch, last_time = Time, rps = RPS} = State) when Epoch < NewEpoch ->
    % Update counter epoch
    NewTime = Time - (NewEpoch-Epoch) * epoch_size(RPS),
    handle_call(Call, From, State#counter{epoch = NewEpoch, last_time = NewTime});
handle_call({schedule, OldEpoch, _RPS, _CurrentTime, _MaxTime}, _From, #counter{epoch = Epoch} = State) when OldEpoch < Epoch ->
    {reply, {error, {epoch_mismatch, Epoch}}, State};
handle_call({schedule, _Epoch, _RPS, CurrentTime, MaxTime}, _From, #counter{last_time = Time, last_value = Value, max_value = MaxValue} = State) ->
    case handle_schedule(CurrentTime, MaxTime, Time, Value, MaxValue) of
        {ok, NewTime, NewValue} ->
            {reply, {ok, NewTime - CurrentTime}, State#counter{last_time = NewTime, last_value = NewValue}};
        {error, _} = Error ->
            {reply, Error, State}
    end.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({schedule, Epoch, RPS, Time, MaxTime, {Pid, Ref} = ReturnPath}, #counter{} = State) when is_pid(Pid) ->
    case handle_call({schedule, Epoch, RPS, Time, MaxTime}, ReturnPath, State) of
        {reply, Response, NextState} ->
            Pid ! {erater_response, Ref, Response},
            {noreply, NextState};
        {noreply, NextState} ->
            {noreply, NextState}
    end;
handle_info({schedule, Epoch, RPS, Time, MaxTime, {mfa, Mod, Fun, Args}}, #counter{} = State) ->
    case handle_call({schedule, Epoch, RPS, Time, MaxTime}, {undefined, undefined}, State) of
        {reply, Response, NextState} ->
            erlang:apply(Mod, Fun, [Response|Args]),
            {noreply, NextState};
        {noreply, NextState} ->
            {noreply, NextState}
    end;
handle_info({timeout, Check, _}, #counter{check_timer = Check} = State) ->
    {noreply, check_ttl(State)};
handle_info({timeout, _WrongTimer, _}, #counter{} = State) ->
    % Wrong timer event, ignore
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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


% Set check timer when possible. Remember active timer in state
set_check_timer(#counter{check_interval = CheckInterval} = State) when is_integer(CheckInterval) ->
    % To avoid regular load spikes randomize interval a bit
    MaxDevi = CheckInterval div 20,
    RandomInterval = CheckInterval + random:uniform(2*MaxDevi) - MaxDevi,
    Timer = erlang:start_timer(RandomInterval, self(), check),
    State#counter{check_timer = Timer};
set_check_timer(#counter{} = State) ->
    State#counter{check_timer = undefined}.

% Check if TTL exceeded
check_ttl(#counter{epoch = Epoch, last_time = LastTime, rps = RPS, ttl = TTL} = State) ->
    Timestamp = os:timestamp(),
    SameEpochTime = get_time(Timestamp, RPS) + (epoch(Timestamp) - Epoch) * epoch_size(RPS),
    case SameEpochTime > LastTime + TTL of
        true ->
            % We don't die immediately to serve requests possibly in their way
            % We deregister ourself and die in 1 second instead
            catch gproc:goodbye(),
            {ok, ExitTimer} = timer:exit_after(1000, shutdown),
            State#counter{check_timer = {shutdown, ExitTimer}};
        false ->
            set_check_timer(State)
    end.

