# Gossip Simulator

A small Erlang simulator for the Gossip and Push-Sum protocols. Every node is its own process. You pick a topology and an algorithm, and the program reports how long the network takes to converge.

## Requirements

[Erlang/OTP](https://www.erlang.org/downloads) 24 or newer. `erl` and `erlc` need to be on `PATH`.

## Run

```sh
./gossip numNodes topology algorithm
```

| Argument | Values |
| --- | --- |
| `topology` | `full`, `2D`, `line`, `imp2D` |
| `algorithm` | `gossip`, `push-sum` |

`2D` and `imp2D` round `numNodes` up to the next perfect square and print the size they actually used.

```sh
./gossip 100 full gossip
./gossip 100 2D push-sum
./gossip 50 line gossip
./gossip 50 imp2D push-sum
```

`Time:` is the convergence time in microseconds, measured with `erlang:monotonic_time` from the moment the starter actor is signaled until the algorithm finishes. Startup is not included.

### Failure models

Add a model name and a probability between 0 and 1. Leave them off for a clean run.

```sh
./gossip 100 line gossip node 0.05
./gossip 100 full push-sum link 0.1
./gossip 100 full push-sum kill 0.01
./gossip 100 2D gossip drop 0.2
```

| Model | Effect |
| --- | --- |
| `node` | Each actor is removed before startup with the given probability. |
| `link` | Each undirected edge is deleted with the given probability. |
| `kill` | After a transmission, the sender crashes with the given probability and its remaining state is lost. |
| `drop` | A transmission attempt fails with the given probability. Gossip loses that copy of the rumor. Push-Sum keeps its mass and tries again. |

A line falls apart under a small chance of node or link failure. A full network still spreads the rumor, and Push-Sum still sums the actors that remain. Mid-run crashes (`kill`) bias the Push-Sum total even on a full network, because the crashed actor takes the half of `(s, w)` it was holding.

## Algorithms

**Gossip.** One actor starts with the rumor. Until it has heard the rumor 10 times, each receipt is followed by ten forwards to a random neighbor. The extra forwards keep the rumor moving on a line and a grid. The run ends when every reachable actor has heard it.

**Push-Sum.** Actor `i` starts with `s = i` and `w = 1`. A send keeps half of `s` and `w` and transmits the other half. The ratio `s/w` settles on the average of the actor values, so the sum is that ratio times the number of actors. An actor is done when `s/w` stays within `1e-10` for three receives in a row. A finished run recovers `n(n+1)/2`.

## Topologies

| Name | Neighbors |
| --- | --- |
| `full` | Every other actor. |
| `line` | The actors on either side, in a path. |
| `2D` | The four orthogonal neighbors on a square grid. |
| `imp2D` | The 2D grid plus one extra random neighbor. |

## Build

```sh
make          # compile into ebin/
make test     # check the line, grid, and imperfect-grid neighbor rules
make smoke    # four short end-to-end runs
make clean
```

## Layout

| Path | Role |
| --- | --- |
| `gossip` | Command-line entry point |
| `src/gossip.erl` | Startup, timing, and convergence |
| `src/actor.erl` | Gossip and Push-Sum actors |
| `src/topology.erl` | Full, 2D, line, and imperfect 2D graphs |
| `src/failure.erl` | Failure models |

## Notes from running it

Times below are a single run on my machine, algorithm time only.

| Topology | Gossip | Push-Sum |
| --- | --- | --- |
| full | 8000 actors, 4.55 s | 2000 actors, 0.61 s |
| 2D | 6400 actors (80×80), 2.49 s | 784 actors (28×28), 4.82 s |
| line | 8000 actors, 6.12 s | 200 actors, 15.8 s |
| imp2D | 6400 actors, 2.40 s | 784 actors, 0.53 s |

Push-Sum on a line is the slow case. Two hundred actors already take about 16 seconds, and the time grows roughly with the cube of the length.

## License

[MIT](LICENSE)
