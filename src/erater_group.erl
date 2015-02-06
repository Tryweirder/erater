-module(erater_group).
-behavior(gen_server).

-export([start_link/2]).
-export([configure/2, get_config/1, get_config/2, status/1]).
-export([set_clock/2, get_clock/1]).
-export([acquire/3, async_acquire/4]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

% Internal API
-export([run_spawned_counter/2, find_or_spawn/2]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Name, Config) when is_atom(Name), is_list(Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Config], []).

configure(Group, Config) ->
    gen_server:call(Group, {configure, Config}).

get_config(Group) ->
    ets:tab2list(Group).

get_config(Group, Key) ->
    ets:lookup_element(Group, Key, 2).

status(Group) when is_atom(Group) ->
    get_group_status(Group).


set_clock(Group, Clock) when is_tuple(Clock), element(1, Clock) == clock ->
    ets:insert(Group, Clock).

get_clock(Group) ->
    [Clock] = ets:lookup(Group, clock),
    Clock.


acquire(Group, CounterName, MaxWait) when is_atom(Group), is_integer(MaxWait) ->
    CounterPid = find_or_spawn(Group, CounterName),
    erater_counter:acquire(CounterPid, MaxWait).

async_acquire(Group, CounterName, MaxWait, ReturnPath) ->
    CounterPid = find_or_spawn(Group, CounterName),
    erater_counter:async_acquire(CounterPid, MaxWait, ReturnPath).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(group, {
        name,
        config
        }).

init([Name, Config]) ->
    Name = ets:new(Name, [set, public, named_table, {read_concurrency, true}]),
    save_config(Name, Config),
    State = #group{name = Name, config = Config},
    {ok, State}.

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
save_config(Name, Config) ->
    true = ets:insert(Name, erater_config:clean(Config)),
    ok.

get_group_status(Group) ->
    AllNodes = [node() | nodes()],
    ActiveNodes = [{Node, Shard} || {Shard, Node} <- erater_shard:map(Group)],
    NodesToExamine = [Node || Node <- AllNodes, not lists:keymember(Node, 1, ActiveNodes)],
    StandbyNodes = [{Node, standby} || Node <- NodesToExamine, node_has_group(Node, Group)],
    lists:keysort(1, ActiveNodes ++ StandbyNodes).

node_has_group(Node, Group) ->
    case rpc:call(Node, erater_group, get_config, [Group, shards]) of
        {badrpc, _} -> false;
        undefined -> false;
        Shards when is_integer(Shards) -> true
    end.



key(Group, CounterName) ->
    {n, l, {Group, CounterName}}.

find_or_spawn(Group, CounterName) ->
    Key = key(Group, CounterName),
    case gproc:where(Key) of
        CounterPid when is_pid(CounterPid) ->
            CounterPid;
        undefined ->
            {CounterPid, _} = gproc:reg_or_locate(Key, true, fun() -> ?MODULE:run_spawned_counter(Group, CounterName) end),
            CounterPid
    end.

% Hack: proc_lib internals are used here
run_spawned_counter(Group, CounterName) ->
    {dictionary, ParentDict} = process_info(whereis(Group), dictionary),
    Config = erater_group:get_config(Group),
    Parent = Group,
    Ancestors = proplists:get_value('$ancestors', ParentDict, []),

    proc_lib:init_p(Parent, Ancestors, erater_counter, run, [Group, CounterName, Config]).
