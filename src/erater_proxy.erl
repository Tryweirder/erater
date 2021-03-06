-module(erater_proxy).

-export([start_link/2, acquire/3, acquire/4, send_acquire/4]).

-behavior(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

start_link(Group, Peer) ->
    gen_server:start_link(?MODULE, [Group, Peer], []).

acquire(Proxy, CounterName, MaxWait) ->
    acquire(Proxy, CounterName, MaxWait, []).

acquire(Proxy, CounterName, MaxWait, Options) ->
    Ref = make_ref(),
    send_acquire(Proxy, CounterName, MaxWait, [{async, {self(), Ref}} | Options]),
    recv_acquire_response(Ref).

send_acquire(Proxy, CounterName, MaxWait, Options) ->
    Proxy ! {async_acquire, CounterName, MaxWait, Options}.

recv_acquire_response(Ref) ->
    receive
        {erater_response, Ref, Result} ->
            Result
    after 5000 ->
            error(acquire_timeout)
    end.

-record(proxy, {
        group,
        peer
        }).

init([Group, Peer]) ->
    State = #proxy{group = Group, peer = Peer},
    link(Peer),
    {ok, State}.


handle_info({async_acquire, CounterName, MaxWait, Options}, #proxy{group = Group} = State) ->
    erater:local_acquire(Group, CounterName, MaxWait, Options),
    {noreply, State}.


handle_cast(_, #proxy{} = State) ->
    {noreply, State}.

handle_call(_, _, #proxy{} = State) ->
    {reply, {error, not_implemented}, State}.

code_change(_, #proxy{} = State, _) ->
    {ok, State}.

terminate(_, _) ->
    ok.
