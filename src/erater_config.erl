-module(erater_config).
-export([validate/1, clean/1, rps/1, capacity/1, die_after/1]).


validate(Config) when is_list(Config) ->
    true = is_integer(rps(Config)),
    true = is_integer(capacity(Config)),
    DieAfter = die_after(Config),
    true = is_integer(DieAfter) orelse (DieAfter == infinity),
    ok.

clean(Config) ->
    [
        {rps, erater_config:rps(Config)},
        {capacity, erater_config:capacity(Config)},
        {die_after, erater_config:die_after(Config)}
        ].


rps(Config) ->
    proplists:get_value(rps, Config, 1).

capacity(Config) ->
    proplists:get_value(capacity, Config, 1).

die_after(Config) ->
    proplists:get_value(die_after, Config).
