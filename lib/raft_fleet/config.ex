use Croma

defmodule RaftFleet.Config do
  @default_balancing_interval (if Mix.env == :test, do: 1_000, else: 60_000)
  defun balancing_interval :: pos_integer do
    Application.get_env(:raft_fleet, :balancing_interval, @default_balancing_interval)
  end

  defun node_purge_threshold_failing_members :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_threshold_failing_members, 2)
  end

  @default_node_purge_failure_time_window (if Mix.env == :test, do: 5_000, else: 300_000)
  defun node_purge_failure_time_window :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_failure_time_window, @default_node_purge_failure_time_window)
  end
end
