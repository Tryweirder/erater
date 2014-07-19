-module(erater_ets).
-export([start_link/2, init/2, master/2, time_server/2]).
-export([acquire/3]).

-export([global_time/1, global_time/2]).


start_link(Group, Config) ->
    {ok, proc_lib:spawn_link(?MODULE, master, [Group, Config])}.

start_link_time_server(Group, Config) ->
    proc_lib:spawn_link(?MODULE, time_server, [Group, Config]).

init(Group, Config) ->
    ets:new(Group, [named_table, public, set, {read_concurrency, true}, {write_concurrency, true}]),
    LockTable = list_to_atom(atom_to_list(Group) ++ "_locks"),
    ets:new(LockTable, [named_table, public, bag, {read_concurrency, true}, {write_concurrency, true}]),
    CleanConfig = erater_config:clean(Config),
    RPS = erater_config:rps(CleanConfig),
    SlotMillis = 1000 div RPS,
    ets:insert(Group, [{time, 1}, {lock_table, LockTable}, {slot_millis, SlotMillis}] ++ CleanConfig),
    ok.


master(Group, Config) ->
    register(Group, self()),
    init(Group, Config),
    start_link_time_server(Group, Config),
    loop(Group).

time_server(Group, Config) ->
    FinePeriod = slot_millis(Group) div 5,
    Period = max(5, FinePeriod),
    RPS = erater_config:rps(Config),
    ts_loop(Group, Period, RPS).

loop(Group) ->
    receive
        _ -> loop(Group)
    end.

ts_loop(Group, Period, RPS) ->
    % Update global time
    Time = get_time(os:timestamp(), RPS),
    global_time(Group, Time),
    % Wait before next update
    receive
        _ ->
            % TODO: log unexpected message
            ok
    after
        Period ->
            ok
    end,
    ts_loop(Group, Period, RPS).


global_time(Group) ->
    ets:lookup_element(Group, time, 2).
global_time(Group, NewValue) ->
    ets:insert(Group, {time, NewValue}).

get_time({MegaSeconds, Seconds, USeconds}, RPS) ->
    UStep = 1000000 div RPS,
    AllSeconds = 1000000 * MegaSeconds + Seconds,
    (AllSeconds * RPS) + (USeconds div UStep).

lock_table(Group) ->
    ets:lookup_element(Group, lock_table, 2).


max_value(Group) ->
    ets:lookup_element(Group, capacity, 2).

slot_millis(Group) ->
    ets:lookup_element(Group, slot_millis, 2).

make_counter(Name, Time, Value) ->
    {Name, Time, Value}.

acquire(Group, Name, MaxWait) ->
    LockTable = lock_table(Group),
    LockResult = gain_lock(LockTable, Name),
    Counter = case LockResult of
        ok -> get_counter(Group, Name);
        {ok, Inherited} -> Inherited
    end,
    Time = global_time(Group),
    SlotMillis = slot_millis(Group),
    MaxTime = Time + MaxWait div SlotMillis,
    MaxValue = max_value(Group),
    {Response, NewCounter} = update_counter(Group, Counter, Time, MaxTime, MaxValue),
    giveaway_lock(LockTable, Name, NewCounter),
    case Response of
        {ok, SlotsToWait} ->
            {ok, SlotsToWait * SlotMillis};
        {error, _} = Error ->
            Error
    end.

get_counter(Group, Name) ->
    case ets:lookup(Group, Name) of
        [] -> {new, Name};
        [Counter] -> Counter
    end.

% New counter: initialize at zero and re-run
update_counter(Group, {new, Name}, Time, MaxTime, MaxValue) ->
    Counter = make_counter(Name, Time, 0),
    update_counter(Group, Counter, Time, MaxTime, MaxValue);

% Old counter: extrapolate and re-run
update_counter(Group, {Name, PrevTime, PrevValue}, Time, MaxTime, MaxValue) when PrevTime < Time ->
    TimeSteps = Time - PrevTime,
    Value = max(0, PrevValue - TimeSteps),
    update_counter(Group, {Name, Time, Value}, Time, MaxTime, MaxValue);

% Timer is too far ahead
update_counter(_Group, {_Name, PrevTime, _} = Counter, _Time, MaxTime, _MaxValue) when PrevTime > MaxTime ->
    Response = {error, overflow},
    {Response, Counter};

% Cannot increment counter without going too far ahead (max value at max time)
update_counter(_Group, {_Name, MaxTime, PrevValue} = Counter, _Time, MaxTime, MaxValue) when PrevValue >= MaxValue ->
    Response = {error, overflow},
    {Response, Counter};

% Low enough value at reachable time
update_counter(Group, {Name, PrevTime, PrevValue}, Time, _MaxTime, MaxValue) when PrevValue < MaxValue ->
    Response = {ok, PrevTime - Time},
    NewCounter = {Name, PrevTime, PrevValue + 1},
    ets:insert(Group, NewCounter),
    {Response, NewCounter};

% Value is too high to increment but time is low, so increase time
update_counter(Group, {Name, PrevTime, PrevValue}, Time, _MaxTime, _MaxValue) ->
    NewTime = PrevTime + 1,
    Response = {ok, NewTime - Time},
    NewCounter = {Name, NewTime, PrevValue},
    ets:insert(Group, NewCounter),
    {Response, NewCounter}.



gain_lock(LockTable, Name) ->
    Self = self(),
    true = ets:insert(LockTable, {Name, Self}),
    Queue = ets:lookup(LockTable, Name),
    wait_lock(Self, undefined, Queue).

giveaway_lock(LockTable, Name, Info) ->
    Self = self(),
    Queue = ets:lookup(LockTable, Name),
    clean_until_inherit_next(LockTable, Queue, Self, Info).

wait_lock(Self, undefined, [{_, Self}|_]) ->
    ok;
wait_lock(Self, Previous, [{_, Self}|_]) ->
    inherit_lock(Previous);
wait_lock(Self, _OldPrev, [{_, Prev}|Tail]) when is_pid(Prev) ->
    wait_lock(Self, Prev, Tail);
wait_lock(Self, Previous, [{_, _NotPid}|Tail]) ->
    wait_lock(Self, Previous, Tail).

inherit_lock(Previous) ->
    Mon = erlang:monitor(process, Previous),
    receive
        {'DOWN', Mon, _, _, _} ->
            ok;
        {inherit_lock, Previous, Info} ->
            erlang:demonitor(Mon, [flush]),
            {ok, Info}
    after
        1000 ->
            erlang:exit(Previous, holding_lock_too_long),
            erlang:demonitor(Mon, [flush]),
            {forced, Previous}
    end.

clean_until_inherit_next(LockTable, [StaleObj|Tail], Self, Info) ->
    true = ets:delete_object(LockTable, StaleObj),
    case StaleObj of
        {_, Self} -> inherit_next(Tail, Self, Info);
        {_, _} -> clean_until_inherit_next(LockTable, Tail, Self, Info)
    end.

inherit_next([], _Self, _Info) ->
    ok;
inherit_next([{_, Next}|_], Self, Info) ->
    Next ! {inherit_lock, Self, Info}.






