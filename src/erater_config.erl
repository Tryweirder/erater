-module(erater_config).
-export([validate/1, clean/1]).
-export([driver/1, rps/1, capacity/1, ttl/1, shards/1]).


validate(Config) when is_list(Config) ->
    true = is_integer(rps(Config)),
    true = is_integer(capacity(Config)),
    DieAfter = ttl(Config),
    true = is_integer(DieAfter) orelse (DieAfter == infinity),
    Shards = shards(Config),
    true = is_integer(Shards) orelse (Shards == undefined),
    ok.

clean(Config) ->
    [
        {driver, driver(Config)},
        {rps, rps(Config)},
        {capacity, capacity(Config)},
        {ttl, ttl(Config)},
        {shards, shards(Config)}
        ].


driver(Config) ->
    proplists:get_value(driver, Config, erater_group).

rps(Config) ->
    proplists:get_value(rps, Config, 1).

capacity(Config) ->
    proplists:get_value(capacity, Config, 1).

ttl(Config) ->
    case proplists:get_value(ttl, Config) of
        TTL when is_integer(TTL) ->
            TTL;
        _ ->
            ReplenishTime = (1000 * capacity(Config)) div rps(Config),
            ReplenishTime + 2000
    end.

shards(Config) ->
    proplists:get_value(shards, Config).
