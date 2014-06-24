-module(erater_sup).
-behavior(supervisor).

-export([start_link/0, start_link/1, add_group/2]).
-export([init/1]).

%% Supervisors local registration names
regname(root) ->
    erater_sup;
regname(groups) ->
    erater_groups;
regname({group, Name, _}) ->
    list_to_atom(atom_to_list(Name) ++ "_sup").

%% By default start root supervisor
start_link() ->
    start_link(root).

%% Generic start function
start_link(Param) ->
    supervisor:start_link({local, regname(Param)}, ?MODULE, Param).


%% Start new group in groups supervisor
add_group(Name, Options) when is_atom(Name) ->
    GroupSpec = {Name,
                 {?MODULE, start_link, [{group, Name, Options}]},
                 transient, 1000, supervisor, []},
    supervisor:start_child(erater_groups, GroupSpec).


%% Root supervisor init
init(root) ->
    Groups = {groups,
              {?MODULE, start_link, [groups]},
              permanent, 1000, supervisor, []},
    Pinger = {pinger,
              {erater_pinger, start_link, []},
              permanent, 1000, worker, [erater_pinger]},
    {ok, {{one_for_one, 5, 10}, [Groups, Pinger]}};

%% Groups supervisor is empty on start
init(groups) ->
    {ok, {{one_for_one, 5, 10}, []}};

%% Each group is supervisor itself
init({group, Name, Options}) ->
    Manager = {manager,
                 {erater_group, start_link, [Name, Options]},
                 transient, 1000, worker, [erater_group]},
    Shard = {shard,
                 {erater_shard, start_link, [Name, Options]},
                 transient, 1000, worker, [erater_group]},
    {ok, {{one_for_all, 10, 1}, [Manager, Shard]}}.

