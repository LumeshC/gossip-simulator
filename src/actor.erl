%%%-------------------------------------------------------------------
%%% @doc One network participant.
%%%
%%% Gossip: keep forwarding the rumor to a random neighbor until this
%%% actor has heard it 10 times.
%%%
%%% Push-sum: s starts at the actor index, w starts at 1. Every send
%%% keeps half of (s, w) and transmits the other half. A receive adds
%%% the incoming pair. The actor converges when s/w stays within 1.0e-10
%%% for 3 consecutive receives. s and w themselves are not expected to
%%% settle — only their ratio.
%%%-------------------------------------------------------------------
-module(actor).

-export([start/0]).

-define(RATIO_EPS, 1.0e-10).
%% Sends performed for each rumor still owed. Keeps a line/grid wave
%% alive: a node must forward many times before 10 receipts silence it.
-define(GOSSIP_BURST, 10).

start() ->
    receive
        {init, Mode, S0, Fail, Alive, Algo, Main} ->
            case Alive of
                false ->
                    Main ! {dead, self()};
                true ->
                    Main ! {ready, self()},
                    case Algo of
                        gossip ->
                            gossip_idle(Mode, Fail, Main);
                        pushsum ->
                            push_idle(Mode, Fail, float(S0), 1.0, Main)
                    end
            end
    end.

%% ------------------------------------------------------------------
%% Gossip
%% ------------------------------------------------------------------

gossip_idle(Mode, Fail, Main) ->
    receive
        shutdown ->
            ok;
        rumor ->
            gossip_count(Mode, Fail, Main, 0)
    end.

%% Count is how many times the rumor was heard before this message.
gossip_count(Mode, Fail, Main, Count) ->
    NewCount = Count + 1,
    case Count of
        0 -> Main ! {heard, self()};
        _ -> ok
    end,
    case NewCount >= 10 orelse not has_neighbor(Mode) of
        true ->
            Main ! {silent, self()},
            gossip_quiet(Main);
        false ->
            gossip_active(Mode, Fail, Main, NewCount)
    end.

%% Finish a burst before reading another rumor, so a mailbox full of
%% duplicates cannot trip the "heard it 10 times" rule early.
gossip_active(Mode, Fail, Main, Count) ->
    gossip_burst(Mode, Fail, Main, Count, 0).

gossip_burst(Mode, Fail, Main, Count, Sent) when Sent >= ?GOSSIP_BURST ->
    receive
        shutdown ->
            ok;
        rumor ->
            gossip_count(Mode, Fail, Main, Count)
    end;
gossip_burst(Mode, Fail, Main, Count, Sent) ->
    case send_rumor(Mode, Fail) of
        dead ->
            Main ! {dead, self()};
        nosend ->
            Main ! {silent, self()},
            gossip_quiet(Main);
        _ ->
            gossip_burst(Mode, Fail, Main, Count, Sent + 1)
    end.

gossip_quiet(Main) ->
    receive
        shutdown -> ok;
        rumor -> gossip_quiet(Main);
        _Other -> gossip_quiet(Main)
    end.

send_rumor(Mode, Fail) ->
    case pick_neighbor(Mode) of
        none ->
            nosend;
        {ok, Pid} ->
            Result =
                case failure:transmit(Fail, rumor) of
                    true ->
                        Pid ! rumor,
                        sent;
                    false ->
                        dropped
                end,
            %% Die after the attempt so a crash removes this actor but does
            %% not erase a rumor that was already handed to a neighbor.
            case failure:crash_on_send(Fail) of
                true -> dead;
                false -> Result
            end
    end.

%% ------------------------------------------------------------------
%% Push-sum
%% ------------------------------------------------------------------

push_idle(Mode, Fail, S, W, Main) ->
    receive
        shutdown ->
            ok;
        {push, Rs, Rw} ->
            push_on_msg(Mode, Fail, S, W, ratio(S, W), 0, Main, false, Rs, Rw);
        start ->
            push_begin(Mode, Fail, S, W, Main)
    after idle_timeout(Fail) ->
        %% A crash can kill every actor that would have contacted this one.
        %% Join the exchange, or stop if no living neighbor remains.
        push_begin(Mode, Fail, S, W, Main)
    end.

push_begin(Mode, Fail, S, W, Main) ->
    case has_neighbor(Mode) of
        false ->
            Main ! {converged, self(), ratio(S, W)},
            push_quiet(Main);
        true ->
            push_forward(Mode, Fail, S, W, ratio(S, W), 0, Main, false)
    end.

%% One exchange per received pair. Forwarding continues after the local
%% ratio has settled so the in-flight half can still reach actors that
%% have not yet seen three stable rounds.
push_on_msg(Mode, Fail, S, W, Prev, Streak, Main, Announced, Rs, Rw) ->
    S1 = S + Rs,
    W1 = W + Rw,
    Ratio = ratio(S1, W1),
    case abs(Ratio - Prev) =< ?RATIO_EPS of
        true ->
            Streak1 = Streak + 1,
            Announced1 = maybe_converge(Announced, Streak1, Main, Ratio),
            push_forward(Mode, Fail, S1, W1, Ratio, Streak1, Main, Announced1);
        false ->
            case Announced of
                true -> Main ! {revoked, self()};
                false -> ok
            end,
            push_forward(Mode, Fail, S1, W1, Ratio, 0, Main, false)
    end.

push_forward(Mode, Fail, S, W, Prev, Streak, Main, Announced) ->
    case send_half(Mode, Fail, S, W) of
        dead ->
            Main ! {dead, self()};
        nosend ->
            maybe_converge(Announced, 3, Main, ratio(S, W)),
            push_quiet(Main);
        %% Link is down this instant. Keep s and w and try another neighbor.
        %% after 0 lets a shutdown message in, so a run of failed sends
        %% cannot ignore the main process forever.
        retry ->
            receive
                shutdown ->
                    ok
            after 0 ->
                push_forward(Mode, Fail, S, W, Prev, Streak, Main, Announced)
            end;
        {ok, S2, W2} ->
            push_wait(Mode, Fail, S2, W2, Prev, Streak, Main, Announced)
    end.

push_wait(Mode, Fail, S, W, Prev, Streak, Main, Announced) ->
    receive
        shutdown ->
            ok;
        {push, Rs, Rw} ->
            push_on_msg(Mode, Fail, S, W, Prev, Streak, Main, Announced, Rs, Rw)
    after idle_timeout(Fail) ->
        %% Kill removes the partner that would have answered. Try another
        %% send so a survivor either keeps mixing or sees that nobody is left.
        push_forward(Mode, Fail, S, W, Prev, Streak, Main, Announced)
    end.

%% No-failure runs wait for a real message. A dynamic failure must not.
idle_timeout(Fail) ->
    case failure:dynamic(Fail) of
        true -> 50;
        false -> infinity
    end.

push_quiet(Main) ->
    receive
        shutdown -> ok;
        {push, _, _} -> push_quiet(Main);
        _Other -> push_quiet(Main)
    end.

maybe_converge(true, _Streak, _Main, _Ratio) ->
    true;
maybe_converge(false, Streak, Main, Ratio) when Streak >= 3 ->
    Main ! {converged, self(), Ratio},
    true;
maybe_converge(false, _Streak, _Main, _Ratio) ->
    false.

send_half(Mode, Fail, S, W) ->
    case pick_neighbor(Mode) of
        none ->
            nosend;
        {ok, Pid} ->
            Result =
                case failure:transmit(Fail, push) of
                    false ->
                        retry;
                    true ->
                        Pid ! {push, S / 2, W / 2},
                        {ok, S / 2, W / 2}
                end,
            %% The kept half disappears with the actor. The half already
            %% sent stays in flight, so one crash does not freeze the run.
            case failure:crash_on_send(Fail) of
                true -> dead;
                false -> Result
            end
    end.

ratio(_S, W) when W == 0 ->
    0.0;
ratio(S, W) ->
    S / W.

%% ------------------------------------------------------------------
%% Neighbors
%% ------------------------------------------------------------------

has_neighbor({list, []}) ->
    false;
has_neighbor({list, _}) ->
    true;
has_neighbor({full, _SelfIndex}) ->
    case actor_table() of
        undefined -> false;
        Pids -> tuple_size(Pids) >= 2
    end.

pick_neighbor({list, Neighbors}) ->
    pick_live(Neighbors);
pick_neighbor({full, SelfIndex}) ->
    pick_full(SelfIndex).

pick_live([]) ->
    none;
pick_live(Neighbors) ->
    Alive = [Pid || Pid <- Neighbors, is_process_alive(Pid)],
    case Alive of
        [] ->
            none;
        _ ->
            {ok, lists:nth(rand:uniform(length(Alive)), Alive)}
    end.

%% Random probes first. If those miss every living actor, scan once so a
%% kill run does not treat a live neighbor as gone.
pick_full(SelfIndex) ->
    case actor_table() of
        undefined ->
            none;
        Pids ->
            case pick_full_random(Pids, SelfIndex, 32) of
                none -> pick_full_scan(Pids, SelfIndex, 1);
                Found -> Found
            end
    end.

pick_full_random(_Pids, _SelfIndex, Tries) when Tries =< 0 ->
    none;
pick_full_random(Pids, SelfIndex, Tries) ->
    N = tuple_size(Pids),
    case N < 2 of
        true ->
            none;
        false ->
            J = rand:uniform(N - 1),
            Index =
                case J >= SelfIndex of
                    true -> J + 1;
                    false -> J
                end,
            Pid = element(Index, Pids),
            case is_process_alive(Pid) of
                true -> {ok, Pid};
                false -> pick_full_random(Pids, SelfIndex, Tries - 1)
            end
    end.

pick_full_scan(Pids, _SelfIndex, Index) when Index > tuple_size(Pids) ->
    none;
pick_full_scan(Pids, SelfIndex, SelfIndex) ->
    pick_full_scan(Pids, SelfIndex, SelfIndex + 1);
pick_full_scan(Pids, SelfIndex, Index) ->
    Pid = element(Index, Pids),
    case is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> pick_full_scan(Pids, SelfIndex, Index + 1)
    end.

actor_table() ->
    persistent_term:get({gossip, actors}, undefined).
