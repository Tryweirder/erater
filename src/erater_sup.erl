-module(erater_sup).
-behavior(supervisor).

-export([start_link/0, add_group/2]).
-export([init/1]).

start_link() ->
    start_link(groups).

start_link(groups) ->
    supervisor:start_link({local, erater_groups}, ?MODULE, groups).

add_group(Name, Options) ->
    supervisor:start_child(erater_groups, [Name, Options]).

init(groups) ->
    GroupSpec = {undefined,
                 {erater_group, start_link, []},
                 transient, 1000, worker, [erater_group]},
    {ok, {{simple_one_for_one, 5, 10}, [GroupSpec]}}.
