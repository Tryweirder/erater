-module(erater_config).
-export([validate/1, clean/1]).
-export([driver/1, rps/1, capacity/1, die_after/1, shards/1]).


validate(Config) when is_list(Config) ->
    true = is_integer(rps(Config)),
    true = is_integer(capacity(Config)),
    DieAfter = die_after(Config),
    true = is_integer(DieAfter) orelse (DieAfter == infinity),
    Shards = shards(Config),
    true = is_integer(Shards) orelse (Shards == undefined),
    ok.

clean(Config) ->
    [
        {driver, driver(Config)},
        {rps, rps(Config)},
        {capacity, capacity(Config)},
        {die_after, die_after(Config)},
        {shards, shards(Config)}
        ].


driver(Config) ->
    proplists:get_value(driver, Config, erater_group).

rps(Config) ->
    proplists:get_value(rps, Config, 1).

capacity(Config) ->
    proplists:get_value(capacity, Config, 1).

die_after(Config) ->
    proplists:get_value(die_after, Config, 300000).

shards(Config) ->
    proplists:get_value(shards, Config).
