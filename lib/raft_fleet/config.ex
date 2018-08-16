use Croma

defmodule RaftFleet.Config do
  @default_balancing_interval                (if Mix.env() == :test, do:  1_000, else:  60_000)
  @default_node_purge_failure_time_window    (if Mix.env() == :test, do: 30_000, else: 600_000)
  @default_node_purge_reconnect_interval     (if Mix.env() == :test, do:  5_000, else:  60_000)
  @default_leader_pid_cache_refresh_interval 300_000
  @default_follower_addition_delay           200

  @moduledoc """
  RaftFleet defines the following application configs:

  - `:balancing_interval`
      - Time interval between periodic triggers of workers whose job is to re-balance Raft member processes across the cluster.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :balancing_interval, #{@default_balancing_interval})`,
        i.e. it defaults to #{div(@default_balancing_interval, 60_000)} minute.
  - `:leader_pid_cache_refresh_interval`
      - Interval time in milliseconds of leader pids cached in each nodes' local ETS tables.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :leader_pid_cache_refresh_interval, #{@default_leader_pid_cache_refresh_interval})`,
        i.e. it defaults to #{div(@default_leader_pid_cache_refresh_interval, 60_000)} minutes.
  - `:follower_addition_delay`
      - Time duration in milliseconds to wait for before spawning a new follower for a consensus group.
        Concurrently spawning multiple followers may lead to race conditions (adding a member can only be done one-by-one).
        Although this race condition can be automatically resolved by retries and thus is basically harmless,
        this configuration item may be useful to reduce useless error logs.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :follower_addition_delay, #{@default_follower_addition_delay})`.
  - `:node_purge_failure_time_window`
      - A node is considered "unhealthy" if it has been disconnected from the other nodes
        without declaring itself as `inactive` (by calling `RaftFleet.deactivate/0`).
        RaftFleet tries to reconnect to unhealthy nodes in order to recover from short-term issues
        such as temporary network failures (see also `:node_purge_reconnect_interval` below).
        To handle longer-term issues, RaftFleet automatically removes nodes that remain "unhealthy"
        for this time window (in milliseconds) from the list of participating active nodes.
        After removal, consensus member processes are automatically re-balanced within remaining active nodes.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :node_purge_failure_time_window, #{@default_node_purge_failure_time_window})`,
        i.e. it defaults to #{div(@default_node_purge_failure_time_window, 60_000)} minutes.
  - `:node_purge_reconnect_interval`
      - Time interval (in milliseconds) of periodic reconnect attempts to disconnected nodes.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :node_purge_reconnect_interval, #{@default_node_purge_reconnect_interval})`,
        i.e. it defaults to #{div(@default_node_purge_reconnect_interval, 60_000)} minute.
  - `:rafted_value_config_maker`
      - A module that implements `RaftFleet.RaftedValueConfigMaker` behaviour.
        The module is used when `:raft_fleet` needs to construct a `t:RaftedValue.Config.t/0`.
        To be more precise, it's used
          1. in `RaftFleet.add_consensus_group/1`, or
          2. when restoring `RaftFleet.Cluster` and the other consensus groups from log & snapshot files.
      - We recommend that you specify your own callback module as `:rafted_value_config_maker`, since, if omitted,
        you have the following limitations:
          1. `RaftFleet.add_consensus_group/1` cannot be used (you must use `RaftFleet.add_consensus_group/3` instead), and
          2. when restoring from log & snapshot files, some consensus groups may not be restored
            (this limitation comes from the current implementation, but we don't have plan to fix this issue).
      - Note that you can customize the `t:RaftedValue.Config.t/0` used by the `RaftFleet.Cluster` consensus group.
        If you are not interested in customizing that value, you can use `RaftFleet.Cluster.default_rv_config/0`
        in your callback module.
      - Note also that libraries using `:raft_fleet` may add their own consensus groups
        (just as in the same way as `RaftFleet.Cluster` expained above).
        Implementations of `RaftFleet.RaftedValueConfigMaker` behaviour must be aware of such consensus groups
        and return appropriate configuration when `c:RaftFleet.RaftedValueConfigMaker.make/1` is called.
  - `:per_member_options_maker`
      - A module that implements `RaftFleet.PerMemberOptionsMaker` behaviour.
        The module is used when constructing a 2nd argument of `RaftedValue.start_link/2`,
        i.e., when starting a new consensus member process.
        This configuration provides a way to customize options (such as whether to persist Raft logs & snapshots)
        for each consensus group member.
      - Defaults to `nil`, which means that the default options of `RaftedValue.start_link/2` are used for all consensus groups.
      - Note that `RaftFleet.Cluster` (a special consensus group to manage metadata for `:raft_fleet`)
        also uses `:per_member_options_maker` module (if set).
        Callback implementation must handle `RaftFleet.Cluster` appropriately, in addition to consensus group names
        that are explicitly added by `RaftFleet.add_consensus_group/3`.
      - Note also that you cannot specify `:name` option by your callback implementation as it's fixed by `:raft_fleet`.

  Note that each raft_fleet process uses application configs stored in the local node.
  If you want to configure the options above you must set them on all nodes in your cluster.
  """

  defun balancing_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :balancing_interval, @default_balancing_interval)
  end

  defun leader_pid_cache_refresh_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :leader_pid_cache_refresh_interval, @default_leader_pid_cache_refresh_interval)
  end

  defun follower_addition_delay() :: pos_integer do
    Application.get_env(:raft_fleet, :follower_addition_delay, @default_follower_addition_delay)
  end

  defun node_purge_failure_time_window() :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_failure_time_window, @default_node_purge_failure_time_window)
  end

  defun node_purge_reconnect_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_reconnect_interval, @default_node_purge_reconnect_interval)
  end

  defun per_member_options_maker() :: nil | module do
    Application.get_env(:raft_fleet, :per_member_options_maker)
  end

  defun rafted_value_config_maker() :: nil | module do
    Application.get_env(:raft_fleet, :rafted_value_config_maker)
  end
end

if Mix.env() == :test do
  defmodule RaftFleet.PerMemberOptionsMaker.Persist do
    @behaviour RaftFleet.PerMemberOptionsMaker
    defun make(name :: atom) :: [RaftedValue.option] do
      [persistence_dir: Path.join(["tmp", Atom.to_string(Node.self()), Atom.to_string(name)])]
    end
  end

  defmodule RaftFleet.PerMemberOptionsMaker.Raise do
    @behaviour RaftFleet.PerMemberOptionsMaker
    defun make(name :: atom) :: [RaftedValue.option] do
      if name == RaftFleet.Cluster do
        []
      else
        [test_inject_fault_after_add_follower: :raise]
      end
    end
  end

  defmodule RaftFleet.PerMemberOptionsMaker.Timeout do
    @behaviour RaftFleet.PerMemberOptionsMaker
    defun make(name :: atom) :: [RaftedValue.option] do
      if name == RaftFleet.Cluster do
        []
      else
        [test_inject_fault_after_add_follower: :timeout]
      end
    end
  end
end
