%%% Erater configurator: this process is spawned
%%% on group reconfiguration and iterates over all
%%% counters telling each of them to change its config
-module(erater_configurator).

-export([start_link/2]).
-export([init/2]).

start_link(Group, Config) when is_atom(Group), is_list(Config) ->
    proc_lib:start_link(?MODULE, init, [Group, Config], 60000).

%% Main function
init(Group, Config) ->
    Counters = erater_group:counters(Group),
    Length = length(Counters),
    put(counters_total, Length),
    proc_lib:init_ack({ok, self()}),
    reconfigure(Length, Counters, Config).

reconfigure(_Length, [], _Config) ->
    ok;
reconfigure(Length, [{_, Counter} | Counters], Config) ->
    put(counters_remaining, Length),
    configure_counter(Counter, Config),
    reconfigure(Length - 1, Counters, Config).

configure_counter(Counter, Config) ->
    try
        erater_counter:set_config(Counter, Config, 60000)
    catch
        exit:{noproc,_} ->
            ok
    end.
