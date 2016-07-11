use Croma

defmodule RaftFleet.Deactivator do
  alias RaftFleet.{Cluster, LeaderPidCache}

  @tries            5
  @sleep            1_000
  @deactivate_steps [
    :remove_node_command,
    :remove_follower_from_cluster_consensus,
    :delete_child_from_supervisor,
  ]

  def deactivate do
    run_steps(@deactivate_steps, 10)
  end

  defp run_steps(_, 0), do: raise "Failed to complete all steps of node deactivation!"
  defp run_steps([], _), do: :ok
  defp run_steps([s | ss], tries_remaining) do
    case step(s) do
      :ok    -> run_steps(ss, tries_remaining)
      :error ->
        :timer.sleep(@sleep)
        run_steps([s | ss], tries_remaining - 1)
    end
  end

  defp step(:remove_node_command) do
    case RaftFleet.command(Cluster, {:remove_node, Node.self}) do
      {:ok, _} -> :ok
      _        -> :error
    end
  end
  defp step(:remove_follower_from_cluster_consensus) do
    leader = LeaderPidCache.get(Cluster)
    case RaftedValue.remove_follower(leader, Process.whereis(Cluster)) do
      :ok -> :ok
      {:error, :cannot_remove_leader} ->
        # member in this node is the current leader => should replace it with other member (if any)
        status = RaftedValue.status(Cluster)
        case List.delete(status[:members], leader) do
          []      -> :ok
          members ->
            next_leader = Enum.random(members)
            RaftedValue.replace_leader(leader, next_leader)
            LeaderPidCache.set(Cluster, next_leader)
            :error
        end
      {:error, {:not_leader, nil}} ->
        _current_leader = RaftFleet.find_leader(Cluster) # this line also call LeaderPidCache.set/2 on success
        :error
      {:error, {:not_leader, leader_hint}} ->
        LeaderPidCache.set(Cluster, leader_hint)
        :error
      {:error, _} -> :error
    end
  end
  defp step(:delete_child_from_supervisor) do
    :ok = Supervisor.terminate_child(RaftFleet.Supervisor, Cluster.Server)
    :ok = Supervisor.delete_child(RaftFleet.Supervisor, Cluster.Server)
  end
end
