%%% Erater timeserver.
%%%
%%% This is a clock source for the group.
%%% Timeserver keeps an up-to-date tuple of
%%% current time, next tick timestamp, etc.
%%% in a ETS readable by anyone.
%%%
%%% Getting current time (by counter while handling request)
%%% is getting the entry from ETS, comparing current timestamp with
%%% next tick timestamp and choosing either
%%% (current time) or (current time) + 1
-module(erater_timeserver).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2, set_rate/2, get_time/1]).


%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


%%%
%%% API Implementation
%%%
start_link(Group, RPS) when is_atom(Group), is_number(RPS) ->
    RegName = list_to_atom(atom_to_list(Group) ++ "_timeserver"),
    gen_server:start_link({local, RegName}, ?MODULE, [Group, RPS], []);
start_link(Group, Config) when is_atom(Group) ->
    start_link(Group, erater_config:rps(Config)).

set_rate(Group, RPS) when is_atom(Group), is_number(RPS) ->
    {error, not_implemented}.

get_time(Group) when is_atom(Group) ->
    Clock = erater_group:get_clock(Group),
    clock_to_time(Clock).


%%%
%%% Internals
%%%
-record(clock, {
        time :: integer(),
        next_timestamp :: erlang:timestamp()
        }).

-record(timeserver, {
        group :: atom(),            % Group name
        rps :: number(),            % Configured RPS
        tick_us :: integer(),       % microseconds between ticks

        ref_timestamp :: erlang:timestamp(),     % reference timstamp for calculating absolute clock values
        ref_time :: integer(),      % time at reference point

        last_time :: integer(),     % Last public time
        next_timestamp :: erlang:timestamp()     % when time should be incremented
        }).


init([Group, RPS]) ->
    Tick_us = round(1000000/RPS),
    Timestamp = os:timestamp(),
    State0 = #timeserver{
            group = Group,
            rps = RPS,
            tick_us = Tick_us,

            ref_timestamp = Timestamp,
            ref_time = 1,

            last_time = undefined,
            next_timestamp = undefined
            },
    {NextTick, State} = sync_clock(State0),
    {ok, State, NextTick}.

handle_call(_, _, State) ->
    update_and_reply({error, not_implemented}, State).

handle_cast(_, State) ->
    update_noreply(State).

handle_info(_, State) ->
    update_noreply(State).



terminate(_, _) ->
    ok.

code_change(_, #timeserver{} = State, _) ->
    {ok, State}.


update_and_reply(Reply, State) ->
    {NextTick, State1} = sync_clock(State),
    {reply, Reply, State1, NextTick}.

update_noreply(State) ->
    {NextTick, State1} = sync_clock(State),
    {noreply, State1, NextTick}.



sync_clock(#timeserver{} = State) ->
    maybe_update_clock(os:timestamp(), State).

maybe_update_clock(Timestamp, #timeserver{next_timestamp = {_, _, _} = NextTS} = State)
        when Timestamp < NextTS ->
    % next_timestamp is valid and fresh enough -- do not update
    NextTick = timer:now_diff(Timestamp, NextTS) div 1000,
    {NextTick, State};
maybe_update_clock(Timestamp, #timeserver{ref_timestamp = RefTS, ref_time = RefTime, tick_us = Tick_us} = State) ->
    Elapsed_us = timer:now_diff(Timestamp, RefTS),
    CurrentTime = RefTime + (Elapsed_us div Tick_us),
    NextTSRel = Tick_us - (Elapsed_us rem Tick_us),
    NextTS = now_add(Timestamp, NextTSRel),
    NewState = State#timeserver{last_time = CurrentTime, next_timestamp = NextTS},
    {NextTSRel div 1000, store_clock(NewState)}.


store_clock(#timeserver{group = Group, last_time = CurrentTime, next_timestamp = NextTS} = State) ->
    Clock = #clock{time = CurrentTime, next_timestamp = NextTS},
    erater_group:set_clock(Group, Clock),
    State.

clock_to_time(#clock{time = Time, next_timestamp = NextTS}) ->
    case (os:timestamp() > NextTS) of
        true -> Time + 1;
        false -> Time
    end.


now_add({Mega, Sec, Micro}, AddMicro) ->
    Micro0 = Micro + AddMicro,
    Sec0 = Sec + Micro0 div 1000000,
    Mega0 = Mega + Sec0 div 1000000,
    {Mega0, Sec0 rem 1000000, Micro0 rem 1000000}.




%%% Tests
now_add_test() ->
    ?assertEqual({200,300,700}, now_add({200,300,400}, 300)),
    ?assertEqual({200,301,7}, now_add({200,300,400003}, 600004)),
    ?assertEqual({201,1,7}, now_add({200,999999,400003}, 1600004)),
    ok.
