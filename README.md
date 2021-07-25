# RaftFleet

Elixir library to run multiple [Raft](https://raft.github.io/) consensus groups in a cluster of ErlangVMs

- [API Documentation](http://hexdocs.pm/raft_fleet/)
- [Hex package information](https://hex.pm/packages/raft_fleet)

[![Hex.pm](http://img.shields.io/hexpm/v/raft_fleet.svg)](https://hex.pm/packages/raft_fleet)
[![Build Status](https://travis-ci.com/skirino/raft_fleet.svg)](https://travis-ci.com/github/skirino/raft_fleet)
[![Coverage Status](https://coveralls.io/repos/github/skirino/raft_fleet/badge.svg?branch=master)](https://coveralls.io/github/skirino/raft_fleet?branch=master)

## Feature & Design

- Easy hosting of multiple "cluster-wide state"s
    - Flexible data model (defined by [rafted_value](https://github.com/skirino/rafted_value))
- Decentralized architecture and fault tolerance
- Reasonably scalable placement of processes for multiple Raft consensus groups
    - Consensus member processes are distributed to ErlangVMs in a data center-aware manner using [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
    - Automatic rebalancing on adding/removing nodes
- Location transparency
    - Each consensus group leader is accessible using name (an atom) of the consensus group
    - Actual pids of consensus leader processes are cached in a local ETS table for fast access

## Notes on backward compatibility

- Users of `<= 0.6.0` should upgrade to `0.6.1` before upgrading to `0.7.x` due to a change in internal data structure.
  While `<= 0.6.0` and `0.7.x` are not compatible, `0.6.1` should be able to interact with both `<= 0.6.0` and `0.7.x`.

## Usage example

Suppose we have a cluster of 4 erlang nodes:

```ex
$ iex --sname 1 -S mix
iex(1@skirino-Manjaro)>

$ iex --sname 2 -S mix
iex(2@skirino-Manjaro)> Node.connect(:"1@skirino-Manjaro")

$ iex --sname 3 -S mix
iex(3@skirino-Manjaro)> Node.connect(:"1@skirino-Manjaro")

$ iex --sname 4 -S mix
iex(4@skirino-Manjaro)> Node.connect(:"1@skirino-Manjaro")
```

Load the following module that implements `RaftedValue.Data` behaviour on all nodes in the cluster.

```ex
defmodule JustAnInt do
  @behaviour RaftedValue.Data
  def new(), do: 0
  def command(i, {:set, j}), do: {i, j    }
  def command(i, :inc     ), do: {i, i + 1}
  def query(i, :get), do: i
end
```

Call `RaftFleet.activate/1` on all nodes.

```ex
iex(1@skirino-Manjaro)> RaftFleet.activate("zone1")

iex(2@skirino-Manjaro)> RaftFleet.activate("zone2")

iex(3@skirino-Manjaro)> RaftFleet.activate("zone1")

iex(4@skirino-Manjaro)> RaftFleet.activate("zone2")
```

Create 5 consensus groups each of which replicates an integer and has 3 consensus members.

```ex
iex(1@skirino-Manjaro)> rv_config = RaftedValue.make_config(JustAnInt)
iex(1@skirino-Manjaro)> RaftFleet.add_consensus_group(:consensus1, 3, rv_config)
iex(1@skirino-Manjaro)> RaftFleet.add_consensus_group(:consensus2, 3, rv_config)
iex(1@skirino-Manjaro)> RaftFleet.add_consensus_group(:consensus3, 3, rv_config)
iex(1@skirino-Manjaro)> RaftFleet.add_consensus_group(:consensus4, 3, rv_config)
iex(1@skirino-Manjaro)> RaftFleet.add_consensus_group(:consensus5, 3, rv_config)
```

Now we can run query/command from any node in the cluster:

```ex
iex(1@skirino-Manjaro)> RaftFleet.query(:consensus1, :get)
{:ok, 0}

iex(2@skirino-Manjaro)> RaftFleet.command(:consensus1, :inc)
{:ok, 0}

iex(3@skirino-Manjaro)> RaftFleet.query(:consensus1, :get)
{:ok, 1}
```

Activating/deactivating a node in the cluster triggers rebalancing of consensus member processes.

## Deployment notes

To run `raft_fleet` within an ErlangVM cluster, the followings are our general recommendations.

- Cluster should consist of at least 3 nodes to tolerate 1 node failure.
  Similarly cluster nodes should span 3 (or more) data centers, so that the system keeps on functioning in the face of 1 data center failure.
- When you add new ErlangVM nodes, each node should run the following initialization steps:
    1. establish connections to other running nodes,
    2. call `RaftFleet.activate/1`.

  These steps are typically done within `start/2` of the main OTP application.
  Information of other running nodes should be available from e.g. IaaS API.
- When terminating a node you should proceed as follows
  (although `raft_fleet` tolerates failures as long as quorums are maintained,
  it's much better to tell `raft_fleet` to make preparations beforehand):
    1. call `RaftFleet.deactivate/0` within the node-to-be-terminated,
    2. wait for a while (say, 10 min) so that existing consensus group members are migrated to the other nodes, then
    3. finally shutdown the node.

## Links

- [Raft official website](https://raft.github.io/)
- [The original paper](https://raft.github.io/raft.pdf) and
  [the thesis](https://github.com/ongardie/dissertation#readme) about the Raft protocol
- [`rafted_value`](https://github.com/skirino/rafted_value) : Elixir implementation of the Raft consensus protocol
- [skirino's slides to introduce rafted_value and raft_fleet](https://skirino.github.io/slides/raft_fleet.html#/)
