use Croma

defmodule RaftFleet.ConsensusMemberAdjuster do
  alias RaftFleet.{Cluster, Manager, LeaderPidCache, Config}

  def adjust do
    case RaftFleet.query(Cluster, {:consensus_groups, Node.self}) do
      {:error, _} ->
        :ok # ignore error, just retry at the next time
      {:ok, {participating_nodes, groups, removed_groups_queue}} ->
        leader_pid = LeaderPidCache.get(Cluster)
        kill_members_of_removed_groups(removed_groups_queue)
        adjust_consensus_member_sets(participating_nodes, groups)
        if node(leader_pid) == Node.self do
          adjust_cluster_consensus_members(leader_pid)
        end
    end
  end

  defp kill_members_of_removed_groups(removed_groups_queue) do
    {removed1, removed2} = removed_groups_queue
    Enum.each(removed1, &brutally_kill/1)
    Enum.each(removed2, &brutally_kill/1)
  end

  defp brutally_kill(group_name) do
    case Process.whereis(group_name) do
      nil -> :ok
      pid -> :gen_fsm.stop(pid)
    end
  end

  defp adjust_consensus_member_sets(participating_nodes, groups) do
    unhealthy_members_counts =
      Enum.flat_map(groups, fn group -> do_adjust(participating_nodes, group) end)
      |> Enum.reduce(%{}, fn(node, map) -> Map.update(map, node, 1, &(&1 + 1)) end)
    threshold = Config.node_purge_threshold_failing_members
    RaftFleet.command(Cluster, {:report_unhealthy_members, Node.self, unhealthy_members_counts, threshold})
  end

  defp do_adjust(_, {_, []}), do: []
  defp do_adjust(participating_nodes, {group_name, desired_member_nodes}) do
    adjust_one_step(participating_nodes, group_name, desired_member_nodes)
  end

  defpt adjust_one_step(participating_nodes, group_name, [leader_node | follower_nodes] = desired_member_nodes) do
    case try_status({group_name, leader_node}) do
      %{state_name: :leader, from: pid, members: members, unresponsive_followers: unresponsive_pids} ->
        follower_nodes_from_leader = Enum.map(members, &node/1) |> List.delete(leader_node) |> Enum.sort
        cond do
          (nodes_to_be_added = follower_nodes -- follower_nodes_from_leader) != [] ->
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added))
          (nodes_to_be_removed = follower_nodes_from_leader -- follower_nodes) != [] ->
            target_node = Enum.random(nodes_to_be_removed)
            target_pid  = Enum.find(members, fn m -> node(m) == target_node end)
            RaftedValue.remove_follower(pid, target_pid)
          unresponsive_pids != [] ->
            target_pid = Enum.random(unresponsive_pids)
            case try_status(target_pid) do
              :noproc ->
                # `target_pid` is confirmed to be dead; remove it from the consensus group
                RaftedValue.remove_follower(pid, target_pid)
              _ -> :ok
            end
          true -> :ok
        end
        Enum.map(unresponsive_pids, &node/1)
      status_or_reason ->
        pairs0 =
          List.delete(participating_nodes, leader_node)
          |> Enum.map(fn n -> {n, try_status({group_name, n})} end)
          |> Enum.filter(&match?({_, %{}}, &1))
        pairs = if is_map(status_or_reason), do: [{leader_node, status_or_reason} | pairs0], else: pairs0
        nodes_with_living_members = Enum.reject(pairs, &match?({_, nil}, &1)) |> Enum.map(fn {n, _} -> n end)
        cond do
          (nodes_to_be_added = desired_member_nodes -- nodes_with_living_members) != [] ->
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added))
          undesired_leader = find_undesired_leader(pairs0, group_name) ->
            RaftedValue.replace_leader(undesired_leader, Process.whereis(group_name))
          true -> :ok
        end
        []
    end
  end

  defp try_status(dest) do
    try do
      # RaftedValue.status(dest)
      :gen_fsm.sync_send_all_state_event(dest, :status, 500)
    catch
      :exit, {reason, _} -> reason # :noproc, {:nodedown, node}, :timeout
    end
  end

  defp find_undesired_leader(pairs, group_name) do
    statuses = Enum.map(pairs, fn {_, s} -> s end)
    case find_leader_from_statuses(statuses) do
      nil ->
        # No leader found in participating nodes.
        # However there may be a leader in already-deactivated nodes; try them
        statuses_not_participating =
          (Node.list -- Enum.map(pairs, fn {n, _} -> n end))
          |> Enum.map(fn n -> try_status({group_name, n}) end)
          |> Enum.filter(&is_map/1)
        if Enum.empty?(statuses_not_participating) do
          nil
        else
          find_leader_from_statuses(statuses_not_participating ++ statuses) # use all statuses in order to exclude stale leader
        end
      pid -> pid
    end
  end

  defp find_leader_from_statuses(statuses) do
    statuses_by_term = Enum.group_by(statuses, fn %{current_term: t} -> t end)
    latest_term = Map.keys(statuses_by_term) |> Enum.max
    statuses_by_term[latest_term]
    |> Enum.find_value(fn
      %{state_name: :leader, from: from} -> from
      _                                  -> nil
    end)
  end

  defp adjust_cluster_consensus_members(leader_pid) do
    # when cluster consensus member process dies and is restarted by its supervisor, pid of the dead process should be removed from consensus
    case RaftedValue.status(leader_pid) do
      %{unresponsive_followers: []} -> :ok
      %{members: member_pids, unresponsive_followers: unresponsive_pids} ->
        healthy_member_nodes = Enum.map(member_pids -- unresponsive_pids, &node/1)
        targets = Enum.filter(unresponsive_pids, fn pid -> node(pid) in healthy_member_nodes end)
        if targets != [] do
          target_pid = Enum.random(targets)
          RaftedValue.remove_follower(leader_pid, target_pid)
        end
      _ -> :ok
    end
  end
end