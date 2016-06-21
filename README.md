# RaftFleet

Running multiple [Raft](https://raft.github.io/) consensus groups in a cluster of Erlang VMs

- [API Documentation](http://hexdocs.pm/raft_fleet/)
- [Hex package information](https://hex.pm/packages/raft_fleet)

[![Hex.pm](http://img.shields.io/hexpm/v/raft_fleet.svg)](https://hex.pm/packages/raft_fleet)
[![Build Status](https://travis-ci.org/skirino/raft_fleet.svg)](https://travis-ci.org/skirino/raft_fleet)
[![Coverage Status](https://coveralls.io/repos/github/skirino/raft_fleet/badge.svg?branch=master)](https://coveralls.io/github/skirino/raft_fleet?branch=master)


## Design

- Scalable placement of multiple Raft consensus groups
    - data center-aware placement of processes using [randezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
    - automatic re-balancing on adding/removing nodes
- Each consensus group is accessible using a logical name (which must be an atom)
    - Members in the same consensus group are registered with the same local name
    - Process IDs of consensus leaders are cached in ETS
- Uses [rafted_value](https://github.com/skirino/rafted_value)

## TODO

- leader hook
  - Notify manager process of unhealthy raft members; manager takes an action (purge node if something is really wrong with the node)
- test
  - add/remove node during continuous client accesses
  - accidentally killed raft member is recovered (old member is removed from consensus group, new member is added to consensus group)
- doc
