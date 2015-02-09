-module(erater).
-behavior(application).

% API
-export([start/0]).
-export([configure/2]).
-export([acquire/3, local_acquire/3, local_async_acquire/4]).
-export([groups/0, ngroups/0]).

% Application calbacks
-export([start/2, stop/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Application
start() ->
    {ok, _} = application:ensure_all_started(erater),
    ok.

start(_, _) ->
    StartResult = erater_sup:start_link(),
    case application:get_env(?MODULE, groups) of
        undefined ->
            ok;
        {ok, Groups} when is_list(Groups) ->
            [configure(Group, Config) || {Group, Config} <- Groups]
    end,
    StartResult.

stop(_) ->
    ok.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

groups() ->
    [Group || {Group, _, _ ,_} <- supervisor:which_children(erater_groups)].

groups(Node) ->
    case rpc:call(Node, ?MODULE, groups, []) of
        Groups when is_list(Groups) -> Groups;
        _ -> []
    end.

ngroups() ->
    AllNodes = [node()|nodes()],
    lists:usort(lists:flatmap(fun groups/1, AllNodes)).


configure(Group, Config) when is_atom(Group) ->
    ok = erater_config:validate(Config),
    case erater_sup:add_group(Group, Config) of
        {ok, _Pid} ->
            ok;
        {error,{already_started,_Pid}} ->
            erater_group:configure(Group, Config)
    end.

acquire(Group, CounterName, MaxWait) when is_atom(Group), is_integer(MaxWait) ->
    case erater_shard:whereis(Group, CounterName) of
        local ->
            local_acquire(Group, CounterName, MaxWait);
        undefined ->
            {error, unavailable};
        Node ->
            rpc:call(Node, ?MODULE, local_acquire, [Group, CounterName, MaxWait])
    end.

local_acquire(Group, CounterName, MaxWait) ->
    Driver = erater_group:get_config(Group, driver),
    Driver:acquire(Group, CounterName, MaxWait).

local_async_acquire(Group, CounterName, MaxWait, ReturnPath) ->
    Driver = erater_group:get_config(Group, driver),
    Driver:async_acquire(Group, CounterName, MaxWait, ReturnPath).

