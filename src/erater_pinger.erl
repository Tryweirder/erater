-module(erater_pinger).

-behavior(gen_server).

-export([start_link/0]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-export([nodes_to_ping/0]).

start_link() ->
    case nodes_to_ping() of
        [] ->
            ignore;
        Nodes ->
            gen_server:start_link({local, erater_pinger}, ?MODULE, [{nodes, Nodes}], [])
    end.


% How often do we re-ping
-define(INTERVAL, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server api
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(pinger, {
        nodes,
        timer
        }).

init([{nodes, Nodes}]) ->
    {ok, set_reping_timer(#pinger{nodes = Nodes})}.

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({timeout, Timer, reping}, #pinger{timer = Timer, nodes = Nodes} = State) ->
    [net_adm:ping(Node) || Node <- Nodes],
    {noreply, set_reping_timer(State)};
handle_info(_, #pinger{} = State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
nodes_to_ping() ->
    lists:usort(lists:flatmap(fun get_nodes_from_param/1, [sync_nodes_mandatory, sync_nodes_optional])).

get_nodes_from_param(ParamName) ->
    case application:get_env(kernel, ParamName) of
        {ok, List} when is_list(List) ->
            List;
        undefined ->
            []
    end.

set_reping_timer(#pinger{} = State) ->
    Timer = erlang:start_timer(?INTERVAL, self(), reping),
    State#pinger{timer = Timer}.
