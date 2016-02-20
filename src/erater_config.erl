-module(erater_config).
-export([validate/1, clean/1]).
-export([nodes/1, driver/1, mode/1, rps/1, capacity/1, ttl/1, shards/1, default_wait/1]).

-compile({no_auto_import, [nodes/1]}).

validate(Config) when is_list(Config) ->
    true = is_number(rps(Config)),
    true = is_integer(capacity(Config)),
    DieAfter = ttl(Config),
    true = is_integer(DieAfter) orelse (DieAfter == infinity),
    Shards = shards(Config),
    true = is_integer(Shards) orelse (Shards == undefined),
    true = is_integer(default_wait(Config)),
    ok.

clean(Config) ->
    [
        {nodes, nodes(Config)},
        {driver, driver(Config)},
        {mode, mode(Config)},
        {rps, rps(Config)},
        {capacity, capacity(Config)},
        {ttl, ttl(Config)},
        {shards, shards(Config)},
        {default_wait, default_wait(Config)}
        ].

nodes(Config) ->
    NodesConf = case proplists:get_value(nodes, Config) of
        undefined ->
            application:get_env(erater, nodes, [node()]);
        Nodes ->
            Nodes
    end,
    expand_nodes(NodesConf).

expand_nodes(Nodes) when is_list(Nodes) ->
    Nodes;
expand_nodes({mfa, {M, F, A}}) ->
    apply(M, F, A).

driver(Config) ->
    proplists:get_value(driver, Config, erater_group).

mode(Config) ->
    case lists:keyfind(mode, 1, Config) of
        {mode, Mode} -> Mode;
        false -> default_mode(driver(Config))
    end.

default_mode(Driver) ->
    case erlang:function_exported(Driver, default_mode, 0) of
        true -> Driver:default_mode();
        false -> undefined
    end.

rps(Config) ->
    proplists:get_value(rps, Config, 1).

capacity(Config) ->
    proplists:get_value(capacity, Config, 1).

ttl(Config) ->
    case proplists:get_value(ttl, Config) of
        TTL when is_integer(TTL) ->
            TTL;
        _ ->
            ReplenishTime = round(1000 * capacity(Config) / rps(Config)),
            ReplenishTime + 2000
    end.

shards(Config) ->
    proplists:get_value(shards, Config).

default_wait(Config) ->
    proplists:get_value(default_wait, Config, 0).
