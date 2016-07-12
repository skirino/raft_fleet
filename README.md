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
