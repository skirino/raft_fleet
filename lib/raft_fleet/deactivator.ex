use Croma

defmodule RaftFleet.Deactivator do
  require Logger
  alias RaftFleet.{Cluster, LeaderPidCache, Util}

  @tries            10
  @sleep            1_000
  @deactivate_steps [
    :remove_node_command,
    :remove_follower_from_cluster_consensus,
    :delete_child_from_supervisor,
  ]

  def deactivate do
    run_steps(@deactivate_steps, @tries)
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
          []            -> :ok
          other_members ->
            case pick_next_leader(leader, other_members) do
              nil         -> nil # there are other members but they are in inactive nodes; nothing we can do except for waiting and retrying
              next_leader -> replace_leader(leader, next_leader)
            end
            :error
        end
      {:error, {:not_leader, nil}} ->
        _current_leader = Util.find_leader_and_cache(Cluster)
        :error
      {:error, {:not_leader, leader_hint}} ->
        LeaderPidCache.set(Cluster, leader_hint)
        :error
      {:error, reason} ->
        Logger.error("remove follower failed: #{inspect(reason)}")
        :error
    end
  end
  defp step(:delete_child_from_supervisor) do
    :ok = Supervisor.terminate_child(RaftFleet.Supervisor, Cluster.Server)
    :ok = Supervisor.delete_child(RaftFleet.Supervisor, Cluster.Server)
  end

  defp pick_next_leader(current_leader, other_members) do
    # We don't want to migrate the current leader to an inactive node; check current active nodes before choosing a member.
    {:ok, nodes_per_zone} = RaftedValue.query(current_leader, :active_nodes)
    nodes = Map.values(nodes_per_zone) |> List.flatten |> MapSet.new
    case Enum.filter(other_members, &(node(&1) in nodes)) do
      []                      -> nil
      members_in_active_nodes -> Enum.random(members_in_active_nodes)
    end
  end

  defp replace_leader(leader, next_leader) do
    case RaftedValue.replace_leader(leader, next_leader) do
      :ok ->
        Logger.info("replaced current leader (#{inspect(leader)}) in this node with #{inspect(next_leader)} in #{node(next_leader)} to deactivate this node")
        LeaderPidCache.set(Cluster, next_leader)
      {:error, reason} ->
        Logger.error("tried to replace current leader in this node but failed: #{inspect(reason)}")
    end
  end
end
