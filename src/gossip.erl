%%%-------------------------------------------------------------------
%%% @doc Gossip / push-sum simulator.
%%%
%%%   gossip numNodes topology algorithm [failureModel probability]
%%%
%%% topology:  full | 2D | line | imp2D
%%% algorithm: gossip | push-sum
%%% failure:   node | kill | drop | link   (optional; omit for a clean run)
%%%-------------------------------------------------------------------
-module(gossip).

-export([main/1]).

main(Args) ->
    try
        run(Args)
    catch
        error:{unknown_topology, Name} ->
            io:format("Unknown topology: ~s~n", [Name]),
            usage(),
            halt(1);
        error:{unknown_algorithm, Name} ->
            io:format("Unknown algorithm: ~s~n", [Name]),
            usage(),
            halt(1);
        error:{unknown_failure, Rest} ->
            io:format("Unknown failure arguments: ~p~n", [Rest]),
            usage(),
            halt(1);
        error:{bad_probability, Raw} ->
            io:format("Probability must be between 0 and 1: ~s~n", [Raw]),
            usage(),
            halt(1);
        error:badarg ->
            usage(),
            halt(1);
        Class:Reason:Stack ->
            io:format("Error: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(1)
    end.

usage() ->
    io:format(
        "Usage: gossip numNodes topology algorithm [node|kill|drop|link probability]~n"
        "  topology:  full | 2D | line | imp2D~n"
        "  algorithm: gossip | push-sum~n"
    ).

run([RawN, RawTopo, RawAlgo | Rest]) ->
    N = list_to_integer(RawN),
    true = N >= 1,
    Topo = topology:normalize(RawTopo),
    Algo = normalize_algo(RawAlgo),
    Fail = failure:parse(Rest),
    Actual = topology:actual_n(Topo, N),
    {Micros, Stats} = simulate(Actual, Topo, Algo, Fail),
    print_report(RawTopo, RawAlgo, N, Actual, Micros, Stats),
    halt(0);
run(_) ->
    usage(),
    halt(1).

normalize_algo("gossip") -> gossip;
normalize_algo("push-sum") -> pushsum;
normalize_algo("pushsum") -> pushsum;
normalize_algo(Other) -> error({unknown_algorithm, Other}).

measure_running_time(Fun) ->
    Start = erlang:monotonic_time(microsecond),
    Res = Fun(),
    End = erlang:monotonic_time(microsecond),
    {End - Start, Res}.

simulate(N, Topo, Algo, Fail) ->
    process_flag(trap_exit, true),
    Indexes = lists:seq(1, N),
    DeadIds = [I || I <- Indexes, failure:static_death(Fail)],
    DeadIdSet = sets:from_list(DeadIds),
    Pairs = [{I, spawn_link(fun actor:start/0)} || I <- Indexes],
    PidMap = maps:from_list(Pairs),
    PidTuple = list_to_tuple([maps:get(I, PidMap) || I <- Indexes]),
    persistent_term:put({gossip, actors}, PidTuple),
    try
        Adj = build_adjacency(Topo, N, DeadIdSet, Fail),
        Main = self(),
        lists:foreach(
            fun(I) ->
                Alive = not sets:is_element(I, DeadIdSet),
                Mode = make_mode(I, Adj, PidMap, Alive),
                maps:get(I, PidMap) ! {init, Mode, I, Fail, Alive, Algo, Main}
            end,
            Indexes
        ),
        {_Ready, Dead} = collect_startup(N, sets:new(), sets:new()),
        LiveIds = [I || {I, Pid} <- Pairs, not sets:is_element(Pid, Dead)],
        ExpectedAll = N * (N + 1) / 2,
        ExpectedLive = float(lists:sum(LiveIds)),
        Participants = length(LiveIds),
        Timed = measure_running_time(fun() ->
            TRef = erlang:start_timer(timeout_ms(Fail), self(), stop),
            Outcome =
                case LiveIds of
                    [] when Algo =:= gossip ->
                        {converged, sets:new(), Dead, 0};
                    [] ->
                        {converged, #{}, Dead, 0};
                    _ ->
                        StarterId = pick_one(LiveIds),
                        GoalIds = reachable_ids(StarterId, Adj, LiveIds),
                        Goal = sets:from_list([maps:get(I, PidMap) || I <- GoalIds]),
                        Starter = maps:get(StarterId, PidMap),
                        case Algo of
                            gossip ->
                                Starter ! rumor,
                                wait_gossip(
                                    sets:new(),
                                    sets:new(),
                                    Dead,
                                    Goal,
                                    Participants,
                                    failure:enabled(Fail),
                                    TRef
                                );
                            pushsum ->
                                Starter ! start,
                                wait_push(
                                    #{},
                                    Dead,
                                    Goal,
                                    Participants,
                                    TRef
                                )
                        end
                end,
            _ = erlang:cancel_timer(TRef),
            finalize(Algo, N, Outcome, ExpectedAll, ExpectedLive, Participants)
        end),
        %% Actors on a full network are still forwarding when the timer
        %% stops. Shut them down before the pid table disappears.
        stop_actors([Pid || {_I, Pid} <- Pairs]),
        Timed
    after
        persistent_term:erase({gossip, actors})
    end.

stop_actors(Pids) ->
    Alive = [Pid || Pid <- Pids, is_process_alive(Pid)],
    lists:foreach(fun(Pid) -> Pid ! shutdown end, Alive),
    Deadline = erlang:monotonic_time(millisecond) + 2000,
    wait_stopped(Alive, Deadline).

wait_stopped([], _Deadline) ->
    ok;
wait_stopped(Pids, Deadline) ->
    Wait = Deadline - erlang:monotonic_time(millisecond),
    case Wait =< 0 of
        true ->
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, Pids),
            ok;
        false ->
            receive
                {'EXIT', Pid, _Reason} ->
                    wait_stopped(lists:delete(Pid, Pids), Deadline);
                _Other ->
                    wait_stopped(Pids, Deadline)
            after Wait ->
                lists:foreach(fun(Pid) -> exit(Pid, kill) end, Pids),
                ok
            end
    end.

timeout_ms(Fail) ->
    case failure:enabled(Fail) of
        true -> 3000;
        false -> 600000
    end.

%% `full` means every surviving actor can reach every other survivor.
%% Otherwise the value is a map of living index -> living neighbor indexes.
build_adjacency(full, _N, DeadIdSet, Fail) ->
    case static_full_graph(Fail, DeadIdSet) of
        true ->
            full;
        false ->
            Indexes = lists:seq(1, _N),
            Living = [I || I <- Indexes, not sets:is_element(I, DeadIdSet)],
            maps:from_list([
                {I, [J || J <- Living, J =/= I, failure:keep_edge(I, J, Fail)]}
             || I <- Living
            ])
    end;
build_adjacency(Topo, N, DeadIdSet, Fail) ->
    Base = topology:neighbor_indexes(Topo, N),
    Indexes = lists:seq(1, N),
    maps:from_list([
        {
            I,
            [
                J
             || J <- maps:get(I, Base),
                not sets:is_element(J, DeadIdSet),
                failure:keep_edge(I, J, Fail)
            ]
        }
     || I <- Indexes,
        not sets:is_element(I, DeadIdSet)
    ]).

static_full_graph(none, DeadIdSet) ->
    sets:size(DeadIdSet) =:= 0;
static_full_graph({drop, _}, DeadIdSet) ->
    sets:size(DeadIdSet) =:= 0;
static_full_graph({kill, _}, DeadIdSet) ->
    sets:size(DeadIdSet) =:= 0;
static_full_graph(_, _) ->
    false.

make_mode(_I, _Adj, _PidMap, false) ->
    {list, []};
make_mode(I, full, _PidMap, true) ->
    {full, I};
make_mode(I, Adj, PidMap, true) ->
    {list, [maps:get(J, PidMap) || J <- maps:get(I, Adj, [])]}.

reachable_ids(StarterId, full, LiveIds) ->
    case lists:member(StarterId, LiveIds) of
        true -> LiveIds;
        false -> [StarterId]
    end;
reachable_ids(StarterId, Adj, _LiveIds) ->
    sets:to_list(bfs([StarterId], sets:from_list([StarterId]), Adj)).

bfs([], Seen, _Adj) ->
    Seen;
bfs([I | Rest], Seen, Adj) ->
    New = [J || J <- maps:get(I, Adj, []), not sets:is_element(J, Seen)],
    bfs(Rest ++ New, sets:union(Seen, sets:from_list(New)), Adj).

collect_startup(0, Ready, Dead) ->
    {Ready, Dead};
collect_startup(Remaining, Ready, Dead) ->
    receive
        {ready, Pid} ->
            collect_startup(Remaining - 1, sets:add_element(Pid, Ready), Dead);
        {dead, Pid} ->
            collect_startup(Remaining - 1, Ready, sets:add_element(Pid, Dead));
        {'EXIT', _Pid, normal} ->
            collect_startup(Remaining, Ready, Dead);
        {'EXIT', Pid, Reason} ->
            error({actor_crashed, Pid, Reason})
    end.

pick_one(List) ->
    lists:nth(rand:uniform(length(List)), List).

wait_gossip(Heard, Silent, Dead, Goal, Participants, FailOn, TRef) ->
    case goal_met(Goal, Heard, Dead) of
        true ->
            {finish_status(Goal, Participants), Heard, Dead, sets:size(Goal)};
        false ->
            Carriers = sets:subtract(sets:subtract(Heard, Silent), Dead),
            Stalled =
                FailOn andalso sets:size(Carriers) =:= 0 andalso sets:size(Heard) > 0,
            case Stalled of
                true ->
                    {partial, Heard, Dead, sets:size(Goal)};
                false ->
                    receive
                        {heard, Pid} ->
                            wait_gossip(
                                sets:add_element(Pid, Heard),
                                Silent,
                                Dead,
                                Goal,
                                Participants,
                                FailOn,
                                TRef
                            );
                        {silent, Pid} ->
                            wait_gossip(
                                Heard,
                                sets:add_element(Pid, Silent),
                                Dead,
                                Goal,
                                Participants,
                                FailOn,
                                TRef
                            );
                        {dead, Pid} ->
                            wait_gossip(
                                Heard,
                                Silent,
                                sets:add_element(Pid, Dead),
                                Goal,
                                Participants,
                                FailOn,
                                TRef
                            );
                        {timeout, TRef, stop} ->
                            {timeout, Heard, Dead, sets:size(Goal)};
                        {'EXIT', _Pid, normal} ->
                            wait_gossip(Heard, Silent, Dead, Goal, Participants, FailOn, TRef);
                        {'EXIT', Pid, Reason} ->
                            error({actor_crashed, Pid, Reason})
                    end
            end
    end.

%% Dead actors are removed from the goal. Survivors keep exchanging until
%% their ratios settle, including after a mid-run crash.
wait_push(Converged, Dead, Goal, Participants, TRef) ->
    case goal_met(Goal, maps:keys(Converged), Dead) of
        true ->
            {finish_status(Goal, Participants), Converged, Dead, sets:size(Goal)};
        false ->
            receive
                {converged, Pid, Ratio} ->
                    case sets:is_element(Pid, Dead) of
                        true ->
                            wait_push(Converged, Dead, Goal, Participants, TRef);
                        false ->
                            wait_push(
                                maps:put(Pid, Ratio, Converged),
                                Dead,
                                Goal,
                                Participants,
                                TRef
                            )
                    end;
                {revoked, Pid} ->
                    wait_push(
                        maps:remove(Pid, Converged),
                        Dead,
                        Goal,
                        Participants,
                        TRef
                    );
                {dead, Pid} ->
                    wait_push(
                        maps:remove(Pid, Converged),
                        sets:add_element(Pid, Dead),
                        Goal,
                        Participants,
                        TRef
                    );
                {timeout, TRef, stop} ->
                    {timeout, Converged, Dead, sets:size(Goal)};
                {'EXIT', _Pid, normal} ->
                    wait_push(Converged, Dead, Goal, Participants, TRef);
                {'EXIT', Pid, Reason} ->
                    error({actor_crashed, Pid, Reason})
            end
    end.

%% Heard is a set. Push-sum passes the converged keys, which is a list.
goal_met(Goal, Heard, Dead) when is_list(Heard) ->
    goal_met(Goal, sets:from_list(Heard), Dead);
goal_met(Goal, Heard, Dead) ->
    sets:size(sets:subtract(sets:subtract(Goal, Heard), Dead)) =:= 0.

finish_status(Goal, Participants) ->
    case sets:size(Goal) >= Participants of
        true -> converged;
        false -> partial
    end.

finalize(gossip, N, {Status, Heard, Dead, Reachable}, ExpectedAll, ExpectedLive, Participants) ->
    LivingHeard = sets:size(sets:subtract(Heard, Dead)),
    DeadCount = sets:size(Dead),
    #{
        algorithm => gossip,
        status => Status,
        heard => LivingHeard,
        dead => DeadCount,
        living => N - DeadCount,
        participants => Participants,
        reachable => Reachable,
        expected_all => ExpectedAll,
        expected_live => ExpectedLive
    };
finalize(pushsum, N, {Status, Converged, Dead, Reachable}, ExpectedAll, ExpectedLive, Participants) ->
    Ratios = maps:values(Converged),
    Ratio =
        case Ratios of
            [] -> 0.0;
            _ -> lists:sum(Ratios) / length(Ratios)
        end,
    %% Each participant starts with w = 1, so s/w converges to the average
    %% and (s/w) * participants recovers the sum when no weight is lost.
    SumEstimate = Ratio * Participants,
    DeadCount = sets:size(Dead),
    #{
        algorithm => pushsum,
        status => Status,
        estimate => Ratio,
        sum_estimate => SumEstimate,
        converged => length(Ratios),
        dead => DeadCount,
        living => N - DeadCount,
        participants => Participants,
        reachable => Reachable,
        expected_all => ExpectedAll,
        expected_live => ExpectedLive
    }.

print_report(Topo, Algo, Requested, Actual, Micros, Stats) ->
    io:format("Time: ~p~n", [Micros]),
    io:format("Requested: ~p~n", [Requested]),
    io:format("Nodes: ~p~n", [Actual]),
    io:format("Topology: ~s~n", [Topo]),
    io:format("Algorithm: ~s~n", [Algo]),
    io:format("Status: ~s~n", [atom_to_list(maps:get(status, Stats))]),
    io:format("Living: ~p~n", [maps:get(living, Stats)]),
    io:format("Dead: ~p~n", [maps:get(dead, Stats)]),
    io:format("Reachable: ~p~n", [maps:get(reachable, Stats)]),
    case maps:get(algorithm, Stats) of
        gossip ->
            Heard = maps:get(heard, Stats),
            io:format("Heard: ~p~n", [Heard]),
            io:format("Coverage: ~.6f~n", [Heard / Actual]),
            Living = max(maps:get(living, Stats), 1),
            io:format("SurvivorCoverage: ~.6f~n", [Heard / Living]);
        pushsum ->
            Ratio = maps:get(estimate, Stats),
            SumEstimate = maps:get(sum_estimate, Stats),
            Expected = maps:get(expected_all, Stats),
            LiveExpected = maps:get(expected_live, Stats),
            io:format("Converged: ~p~n", [maps:get(converged, Stats)]),
            io:format("Estimate: ~.10f~n", [Ratio]),
            io:format("SumEstimate: ~.10f~n", [SumEstimate]),
            io:format("Expected: ~.10f~n", [Expected]),
            io:format("SurvivorExpected: ~.10f~n", [LiveExpected]),
            io:format("RelativeError: ~.6e~n", [rel_error(SumEstimate, Expected)]),
            io:format("SurvivorError: ~.6e~n", [rel_error(SumEstimate, LiveExpected)])
    end.

rel_error(_Estimate, Expected) when abs(Expected) < 1.0e-12 ->
    0.0;
rel_error(Estimate, Expected) ->
    abs(Estimate - Expected) / abs(Expected).
