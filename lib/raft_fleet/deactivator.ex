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
    :notify_node_reconnector_in_this_node,
  ]

  def deactivate() do
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
    case RaftFleet.command(Cluster, {:remove_node, Node.self()}) do
      {:ok, _} -> :ok
      _        -> :error
    end
  end
  defp step(:remove_follower_from_cluster_consensus) do
    local_member = Process.whereis(Cluster)
    case Util.find_leader_and_cache(Cluster) do
      nil           -> :error
      ^local_member ->
        # member in this node is the current leader => should replace it with other member (if any)
        status = RaftedValue.status(local_member)
        case List.delete(status[:members], local_member) do
          []            -> :ok
          other_members ->
            case pick_next_leader(local_member, other_members) do
              nil         -> nil # there are other members but no suitable member found; nothing we can do except for waiting and retrying
              next_leader -> replace_leader(local_member, next_leader)
            end
            :error
        end
      current_leader ->
        catch_exit(fn -> RaftedValue.remove_follower(current_leader, local_member) end)
        |> case do
          :ok              -> :ok
          {:error, reason} ->
            Logger.error("remove follower failed: #{inspect(reason)}")
            :error
        end
    end
  end
  defp step(:delete_child_from_supervisor) do
    :ok = Supervisor.terminate_child(RaftFleet.Supervisor, Cluster.Server)
    :ok = Supervisor.delete_child(RaftFleet.Supervisor, Cluster.Server)
  end
  defp step(:notify_node_reconnector_in_this_node) do
    GenServer.cast(RaftFleet.NodeReconnector, :this_node_deactivated)
  end

  defp pick_next_leader(current_leader, other_members) do
    # We don't want to migrate the current leader to an inactive node; check currently active nodes before choosing a member.
    case RaftedValue.query(current_leader, :active_nodes) do
      {:ok, nodes_per_zone} ->
        nodes = Map.values(nodes_per_zone) |> List.flatten() |> MapSet.new()
        case Enum.filter(other_members, &(node(&1) in nodes)) do
          []                      -> nil
          members_in_active_nodes -> Enum.random(members_in_active_nodes)
        end
      {:error, _} ->
        # Although local member has been the leader until very recently, it turns out that it's not leader now.
        # Let's retry from the beginning of the step.
        nil
    end
  end

  defp replace_leader(leader, next_leader) do
    catch_exit(fn -> RaftedValue.replace_leader(leader, next_leader) end)
    |> case do
      :ok ->
        Logger.info("replaced current leader (#{inspect(leader)}) in this node with #{inspect(next_leader)} in #{node(next_leader)} to deactivate this node")
        LeaderPidCache.set(Cluster, next_leader)
      {:error, reason} ->
        Logger.error("tried to replace current leader in this node but failed: #{inspect(reason)}")
    end
  end

  defp catch_exit(f) do
    try do
      f.()
    catch
      :exit, reason -> {:error, reason}
    end
  end
end
