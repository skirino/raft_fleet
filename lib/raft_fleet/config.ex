use Croma

defmodule RaftFleet.Config do
  @moduledoc """
  RaftFleet defines the following application configs:

  - `:balancing_interval`
      - Time interval between periodic triggers of workers whose job is to re-balance Raft member processes across the cluster.
      - The actual value used is obtained by `Application.get_env(:raft_fleet, :balancing_interval, 60_000)`,
        i.e. it defaults to 1 minutes.
  - `:node_purge_threshold_failing_members`
      - RaftFleet automatically purges unhealthy nodes.
        To judge whether a node is healthy or not, it uses number of unresponsive Raft members.
      - The actual value used is obtained by `Application.get_env(:raft_fleet, :node_purge_threshold_failing_members, 2)`,
        i.e. nodes that have more than or equal to 3 failing Raft members are regarded as "unhealthy".
  - `:node_purge_failure_time_window`
      - When a node remains "unhealthy" for longer than this time window, it is removed from the participating nodes
        (that is, all activated nodes that host consensus members).
      - The actual value used is obtained by `Application.get_env(:raft_fleet, :node_purge_failure_time_window, 600_000)`,
        i.e. it defaults to 10 minutes.
  """

  @default_balancing_interval (if Mix.env == :test, do: 1_000, else: 60_000)
  defun balancing_interval :: pos_integer do
    Application.get_env(:raft_fleet, :balancing_interval, @default_balancing_interval)
  end

  defun node_purge_threshold_failing_members :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_threshold_failing_members, 2)
  end

  @default_node_purge_failure_time_window (if Mix.env == :test, do: 5_000, else: 600_000)
  defun node_purge_failure_time_window :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_failure_time_window, @default_node_purge_failure_time_window)
  end
end
