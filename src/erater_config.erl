-module(erater_config).
-export([validate/1, rps/1, capacity/1]).


validate(Config) when is_list(Config) ->
    RPS = proplists:get_value(rps, Config, 1),
    true = is_integer(RPS),
    MaxValue = proplists:get_value(capacity, Config, 1),
    true = is_integer(MaxValue),
    ok.

rps(Config) ->
    proplists:get_value(rps, Config, 1).

capacity(Config) ->
    proplists:get_value(capacity, Config, 1).
