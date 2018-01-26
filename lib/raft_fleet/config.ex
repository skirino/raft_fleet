use Croma

defmodule RaftFleet.Config do
  @default_balancing_interval                (if Mix.env() == :test, do:  1_000, else:  60_000)
  @default_node_purge_failure_time_window    (if Mix.env() == :test, do: 30_000, else: 600_000)
  @default_node_purge_reconnect_interval     (if Mix.env() == :test, do:  5_000, else:  60_000)
  @default_leader_pid_cache_refresh_interval 300_000

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
  - `:node_purge_failure_time_window`
      - A node is considered "unhealthy" if it has been disconnected from the other nodes
        without declaring itself as `inactive` (by calling `RaftFleet.deactivate/0`).
        RaftFleet tries to reconnect to unhealthy node in order to recover from short-term issues
        such as temporary network failures (see also `:node_purge_reconnect_interval` below).
        To handle longer-term issues, RaftFleet automatically removes nodes that remain "unhealthy"
        for this time window (in milliseconds) from the participating active nodes.
        After removal, consensus member processes are automatically re-balanced within remaining active nodes.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :node_purge_failure_time_window, #{@default_node_purge_failure_time_window})`,
        i.e. it defaults to #{div(@default_node_purge_failure_time_window, 60_000)} minutes.
  - `:node_purge_reconnect_interval`
      - Time interval (in milliseconds) of periodic reconnect attempts to disconnected nodes.
      - The actual value used is obtained by
        `Application.get_env(:raft_fleet, :node_purge_reconnect_interval, #{@default_node_purge_reconnect_interval})`,
        i.e. it defaults to #{div(@default_node_purge_reconnect_interval, 60_000)} minute.
  - `:persistence_dir_parent`
      - Parent directory of directories to store Raft logs & snapshots.
        If given, each consensus member process persists its logs and periodic snapshots in
        `Path.join(Application.get_env(:raft_fleet, :persistence_dir_parent), Atom.to_string(consensus_group_name))`.
        See also options for `RaftedValue.start_link/2`.
        If not given all processes will run in in-memory mode.

  Note that each raft_fleet process uses application configs stored in the local node.
  If you want to configure the options above you must set them on all nodes in your cluster.
  """

  defun balancing_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :balancing_interval, @default_balancing_interval)
  end

  defun leader_pid_cache_refresh_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :leader_pid_cache_refresh_interval, @default_leader_pid_cache_refresh_interval)
  end

  defun node_purge_failure_time_window() :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_failure_time_window, @default_node_purge_failure_time_window)
  end

  defun node_purge_reconnect_interval() :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_reconnect_interval, @default_node_purge_reconnect_interval)
  end

  defun persistence_dir_parent() :: nil | Path.t do
    Application.get_env(:raft_fleet, :persistence_dir_parent)
  end

  @doc false
  defun rafted_value_test_inject_fault_after_add_follower() :: nil | :raise | :timeout do
    Application.get_env(:raft_fleet, :rafted_value_test_inject_fault_after_add_follower)
  end
end
