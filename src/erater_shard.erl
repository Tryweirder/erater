-module(erater_shard).
-behavior(gen_server).

%% API
-export([start_link/2]).
-export([name/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

start_link(Group, Config) ->
    case proplists:get_value(shards, Config) of
        undefined ->
            ignore;
        Shards when is_integer(Shards) ->
            RegName = list_to_atom(atom_to_list(Group) ++ "_shard"),
            gen_server:start_link({local, RegName}, ?MODULE, [Group, Shards, Config], [])
    end.

% return global name for given group and shard
name(Group, Shard) ->
    {?MODULE, Group, Shard}.

-record(shard, {
        group, % erater group name
        config,
        total, % total number of shards
        current % current registered shard
        }).

init([Group, Shards, Config]) ->
    State = #shard{group = Group, config = Config, total = Shards, current = undefined},
    {ok, State, 0}.

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(timeout, #shard{current = undefined} = State) ->
    try_register(State);
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.




try_register(#shard{group = Group, total = Shards, current = undefined} = State) ->
    ManagerPid = whereis(Group),
    false = ManagerPid == undefined, % check if manager is running
    ok = global:sync(),
    case take_shard_name(Group, lists:seq(1, Shards), ManagerPid) of
        Registered when is_integer(Registered) ->
            {noreply, State#shard{current = Registered}};
        undefined ->
            % All shards are registered, re-check in 1 second
            % TODO: monitor all shards to be faster
            {noreply, State, 1000}
    end.

take_shard_name(_Group, [], _Manager) ->
    undefined;
take_shard_name(Group, [Shard|Shards], Manager) ->
    case global:register_name(name(Group, Shard), Manager, fun global:random_exit_name/3) of
        yes -> Shard;
        no -> take_shard_name(Group, Shards, Manager)
    end.
