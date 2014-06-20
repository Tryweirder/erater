-module(erater_sup).
-behavior(supervisor).

-export([start_link/0, add_group/2]).
-export([init/1]).

start_link() ->
    start_link(groups).

start_link(groups) ->
    supervisor:start_link({local, erater_groups}, ?MODULE, groups).

add_group(Name, Options) when is_atom(Name) ->
    GroupSupParam = {group, Name, Options},
    SupRegName = list_to_atom(atom_to_list(Name) ++ "_sup"),
    GroupSpec = {Name,
                 {supervisor, start_link, [{local, SupRegName}, ?MODULE, GroupSupParam]},
                 transient, 1000, supervisor, []},
    supervisor:start_child(erater_groups, GroupSpec).

init(groups) ->
    {ok, {{one_for_one, 5, 10}, []}};

init({group, Name, Options}) ->
    Manager = {manager,
                 {erater_group, start_link, [Name, Options]},
                 transient, 1000, worker, [erater_group]},
    Shard = {shard,
                 {erater_shard, start_link, [Name, Options]},
                 transient, 1000, worker, [erater_group]},
    {ok, {{one_for_all, 10, 1}, [Manager, Shard]}}.

