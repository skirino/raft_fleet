use Croma

defmodule RaftFleet.LeaderPidCache do
  @table_name :raft_fleet_leader_pid_cache

  defun init :: :ok do
    _table_id = :ets.new(@table_name, [:public, :named_table, {:read_concurrency, true}])
    :ok
  end

  defun get(name :: atom) :: nil | pid do
    case :ets.lookup(@table_name, name) do
      []         -> nil
      [{_, pid}] -> pid
    end
  end

  defun set(name :: atom, leader :: nil | pid) :: :ok do
    :ets.insert(@table_name, {name, leader})
    :ok
  end
end
