# RaftFleet

Running multiple [Raft](https://raft.github.io/) consensus groups in a cluster of Erlang VMs

- [API Documentation](http://hexdocs.pm/raft_fleet/)
- [Hex package information](https://hex.pm/packages/raft_fleet)

[![Hex.pm](http://img.shields.io/hexpm/v/raft_fleet.svg)](https://hex.pm/packages/raft_fleet)
[![Build Status](https://travis-ci.org/skirino/raft_fleet.svg)](https://travis-ci.org/skirino/raft_fleet)
[![Coverage Status](https://coveralls.io/repos/github/skirino/raft_fleet/badge.svg?branch=master)](https://coveralls.io/github/skirino/raft_fleet?branch=master)

## Design

- Scalable placement of processes for multiple Raft consensus groups
    - consensus member processes are distributed to ErlangVMs in a data center-aware way using [randezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
    - automatic rebalancing on adding/removing nodes
- Each consensus group leader is accessible using its registered name (which must be an atom)
    - Process IDs of consensus leaders are cached in a local ETS table
- [rafted_value](https://github.com/skirino/rafted_value) as a building block

## Example

Suppose we have 4 erlang nodes:

```ex
$ iex --sname 1 -S mix
iex(1@skirino-Manjaro)>

$ iex --sname 2 -S mix
iex(2@skirino-Manjaro)> Node.connect :"1@skirino-Manjaro"

$ iex --sname 3 -S mix
iex(3@skirino-Manjaro)> Node.connect :"1@skirino-Manjaro"

$ iex --sname 4 -S mix
iex(4@skirino-Manjaro)> Node.connect :"1@skirino-Manjaro"
```

Load the following module in all nodes.

```ex
defmodule JustAnInt do
  @behaviour RaftedValue.Data
  def new, do: 0
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

Adding/removing nodes trigger rebalancing of consensus member processes.
