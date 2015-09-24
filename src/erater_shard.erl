-module(erater_shard).
-behavior(minishard).

%% API
-export([shard/2, whereis/2, whereis_shard/2, map/1]).

%% minishard callbacks
-export([shard_count/1, cluster_nodes/1]).
-export([allocated/2, score/1, prolonged/2, deallocated/2]).

%% Configuration
shard_count(Group) ->
    erater_group:get_config(Group, shards).

cluster_nodes(Group) ->
    erater_group:get_config(Group, nodes).


% Return a node hosting shard for given counter name
whereis(Group, CounterName) when is_atom(Group), is_binary(CounterName) ->
    case shard(Group, CounterName) of
        undefined -> local;
        Shard -> minishard:get_node(Group, Shard)
    end.

whereis_shard(Group, Shard) when is_atom(Group), is_integer(Shard) ->
    minishard:get_manager(Group, Shard).

% Determine shard number for given counter
shard(Group, CounterName) ->
    case shard_count(Group) of
        undefined ->
            undefined;
        Shards ->
            (erlang:crc32(CounterName) rem Shards) + 1 % 1..Shards
    end.

% Returns map of [{Shard::integer(), Node::node()}]
map(Group) ->
    case erater_group:get_config(Group, shards) of
        undefined ->
            undefined;
        Shards ->
            [{Shard, shard_node(Group, Shard)} || Shard <- lists:seq(1, Shards)]
    end.

shard_node(Group, Shard) ->
    minishard:get_node(Group, Shard).

-record(er_shard, {
        group, % erater group name
        config,
        total, % total number of shards
        current, % current registered shard
        alloc_time
        }).


allocated(Group, Num) ->
    {ok, #er_shard{
            group = Group,
            current = Num,
            alloc_time = os:timestamp()
            }}.

score(#er_shard{alloc_time = AllocTime}) ->
    % Let the score be number of seconds we are active
    % TODO: maybe consider number of active counters
    timer:now_diff(os:timestamp(), AllocTime) / 10000000.

prolonged(_Loser, #er_shard{} = State) ->
    {ok, State}.

deallocated(_, #er_shard{}) ->
    ok.
