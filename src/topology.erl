%%%-------------------------------------------------------------------
%%% @doc Neighbor relations for the gossip / push-sum simulator.
%%%
%%% Topologies:
%%%   full  — every actor is a neighbor of every other actor
%%%   2D    — 4-connected grid; numNodes is rounded up to a square
%%%   line  — a path graph
%%%   imp2D — 2D grid plus one extra random neighbor
%%%-------------------------------------------------------------------
-module(topology).

-export([
    normalize/1,
    actual_n/2,
    neighbor_indexes/2,
    self_check/0
]).

-spec normalize(string()) -> full | grid2d | line | imp2d.
normalize("full") -> full;
normalize("2D") -> grid2d;
normalize("line") -> line;
normalize("imp2D") -> imp2d;
normalize(Other) -> error({unknown_topology, Other}).

%% 2D topologies use the smallest square that can hold the request.
-spec actual_n(full | grid2d | line | imp2d, pos_integer()) -> pos_integer().
actual_n(grid2d, N) ->
    side(N) * side(N);
actual_n(imp2d, N) ->
    side(N) * side(N);
actual_n(_, N) ->
    N.

-spec side(pos_integer()) -> pos_integer().
side(N) when N < 1 ->
    1;
side(N) ->
    trunc(math:ceil(math:sqrt(N))).

%% Neighbor lists are 1-based actor indexes.
%% `full` is returned as an atom so callers do not materialize an N^2 list.
-spec neighbor_indexes(full | grid2d | line | imp2d, pos_integer()) ->
    full | #{pos_integer() => [pos_integer()]}.
neighbor_indexes(full, _N) ->
    full;
neighbor_indexes(line, N) ->
    maps:from_list([{I, line_neighbors(I, N)} || I <- lists:seq(1, N)]);
neighbor_indexes(grid2d, N) ->
    Side = side(N),
    Actual = Side * Side,
    maps:from_list([{I, grid_neighbors(I, Side)} || I <- lists:seq(1, Actual)]);
neighbor_indexes(imp2d, N) ->
    Side = side(N),
    Actual = Side * Side,
    maps:from_list([
        {I, imperfect_neighbors(I, Side, Actual)} || I <- lists:seq(1, Actual)
    ]).

line_neighbors(1, 1) ->
    [];
line_neighbors(1, _N) ->
    [2];
line_neighbors(N, N) ->
    [N - 1];
line_neighbors(I, _N) ->
    [I - 1, I + 1].

grid_neighbors(Index, Side) ->
    Row = (Index - 1) div Side,
    Col = (Index - 1) rem Side,
    Candidates = [
        {Row - 1, Col},
        {Row + 1, Col},
        {Row, Col - 1},
        {Row, Col + 1}
    ],
    [
        to_index(R, C, Side)
     || {R, C} <- Candidates,
        R >= 0,
        R < Side,
        C >= 0,
        C < Side
    ].

imperfect_neighbors(Index, Side, Actual) ->
    Grid = grid_neighbors(Index, Side),
    case extra_candidates(Index, Actual, Grid) of
        [] ->
            Grid;
        Candidates ->
            Extra = lists:nth(rand:uniform(length(Candidates)), Candidates),
            [Extra | Grid]
    end.

extra_candidates(Index, Actual, Grid) ->
    [
        J
     || J <- lists:seq(1, Actual),
        J =/= Index,
        not lists:member(J, Grid)
    ].

to_index(Row, Col, Side) ->
    Row * Side + Col + 1.

self_check() ->
    1 = actual_n(line, 1),
    16 = actual_n(grid2d, 10),
    16 = actual_n(imp2d, 13),
    10 = actual_n(full, 10),
    Line = neighbor_indexes(line, 4),
    [2] = maps:get(1, Line),
    [1, 3] = lists:sort(maps:get(2, Line)),
    [3] = maps:get(4, Line),
    Grid = neighbor_indexes(grid2d, 9),
    [2, 4] = lists:sort(maps:get(1, Grid)),
    [2, 4, 6, 8] = lists:sort(maps:get(5, Grid)),
    Imp = neighbor_indexes(imp2d, 9),
    true = length(maps:get(5, Imp)) =:= 5,
    true = lists:all(
        fun(I) ->
            Ns = maps:get(I, Imp),
            length(Ns) =:= length(lists:usort(Ns)) andalso not lists:member(I, Ns)
        end,
        lists:seq(1, 9)
    ),
    full = neighbor_indexes(full, 5),
    ok.
