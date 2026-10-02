%%%-------------------------------------------------------------------
%%% @doc Failure models: permanent loss, crashes, and dropped sends.
%%%
%%%   none          — no failures
%%%   {node, P}     — each actor is removed permanently with probability P
%%%                   before the algorithm starts
%%%   {kill, P}     — on each transmission, the sender crashes with
%%%                   probability P and its current state is lost
%%%   {drop, P}     — a transmission attempt fails with probability P;
%%%                   gossip rumors are lost, push-sum keeps its mass and
%%%                   tries again later (the link is down, not corrupted)
%%%   {link, P}     — each existing edge is deleted permanently with
%%%                   probability P when the topology is built
%%%-------------------------------------------------------------------
-module(failure).

-export([
    parse/1,
    none/0,
    enabled/1,
    dynamic/1,
    static_death/1,
    keep_edge/3,
    transmit/2,
    crash_on_send/1
]).

none() ->
    none.

enabled(none) -> false;
enabled(_) -> true.

%% Failures that can disconnect the network after the run has started.
dynamic({kill, _}) -> true;
dynamic({drop, _}) -> true;
dynamic(_) -> false.

%% Extra CLI args after: numNodes topology algorithm
parse([]) ->
    none;
parse(["node", Raw]) ->
    {node, probability(Raw)};
parse(["kill", Raw]) ->
    {kill, probability(Raw)};
parse(["drop", Raw]) ->
    {drop, probability(Raw)};
parse(["link", Raw]) ->
    {link, probability(Raw)};
parse(Other) ->
    error({unknown_failure, Other}).

probability(Raw) ->
    P = to_float(Raw),
    true = P >= 0.0 andalso P =< 1.0,
    P.

to_float(Raw) ->
    case string:to_float(Raw) of
        {F, []} ->
            F;
        {error, no_float} ->
            case string:to_integer(Raw) of
                {I, []} -> float(I);
                _ -> error({bad_probability, Raw})
            end
    end.

%% Permanent crash decided once, before startup.
static_death({node, P}) ->
    rand:uniform() < P;
static_death(_) ->
    false.

%% Undirected: both endpoints share one coin flip, stored for this run.
keep_edge(I, J, {link, P}) ->
    Key = {min(I, J), max(I, J)},
    case get({gossip_edge, Key}) of
        undefined ->
            Alive = rand:uniform() >= P,
            put({gossip_edge, Key}, Alive),
            Alive;
        Alive ->
            Alive
    end;
keep_edge(_I, _J, _) ->
    true.

%% Temporary link failure. `true` means the send may proceed.
transmit({drop, P}, _MsgKind) ->
    rand:uniform() >= P;
transmit(_, _) ->
    true.

%% Dynamic node crash at the moment of sending.
crash_on_send({kill, P}) ->
    rand:uniform() < P;
crash_on_send(_) ->
    false.
