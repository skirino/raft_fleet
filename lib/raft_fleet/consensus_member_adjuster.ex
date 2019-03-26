use Croma

defmodule RaftFleet.ConsensusMemberAdjuster do
  require Logger
  alias RaftFleet.{Cluster, Manager, LeaderPidCache}

  # Currently this is a constant; I don't know whether this should be customizable or not.
  @wait_time_before_forgetting_deactivated_node 30 * 60_000

  def adjust() do
    case RaftFleet.query(Cluster, {:consensus_groups, Node.self()}) do
      {:error, reason} ->
        Logger.warn("querying all consensus groups failed: #{inspect(reason)}")
      {:ok, {participating_nodes, groups, removed_groups}} ->
        kill_members_of_removed_groups(removed_groups)
        adjust_consensus_member_sets(participating_nodes, groups)
        remove_extra_members_in_cluster_consensus_if_leader_resides_in_this_node(participating_nodes)
    end
  end

  #
  # Common utilities
  #
  defp try_status(dest) do
    try do
      RaftedValue.status(dest)
    catch
      :exit, {reason, _} -> reason # :noproc | {:nodedown, node} | :timeout
    end
  end

  defp relevant_node_set(participating_nodes) do
    # We need to take both of the following types of nodes into account to correctly find all member processes:
    # - participating (active) nodes, which may not be connected due to temporary netsplit
    # - currently connected nodes, which may already be deactivated but may still have member processes
    Enum.into(participating_nodes, MapSet.new(Node.list()))
  end

  #
  # Kill processes of already-removed consensus groups
  #
  defp kill_members_of_removed_groups({removed_groups, index}) do
    Enum.each(removed_groups, &stop_members_of_removed_group/1)
    notify_completion_of_cleanup(index)
  end

  defp stop_members_of_removed_group(group_name) do
    case try_status(group_name) do
      %{from: from, members: members} ->
        stop_member(from)
        case List.delete(members, from) do
          []            -> :ok
          other_members -> spawn(fn -> Enum.each(other_members, &stop_member/1) end)
        end
      _failed ->
        # Even if `status/1` times out, we have to remove at least locally running member
        case Process.whereis(group_name) do
          nil -> :ok
          pid -> stop_member(pid)
        end
    end
  end

  defp stop_member(pid) do
    try do
      :gen_statem.stop(pid)
    catch
      :exit, _ -> :ok # Any other concurrent activity has just killed the pid; neglect it.
    end
  end

  defp notify_completion_of_cleanup(index_or_group_name_or_nil) do
    spawn(fn ->
      millis = System.system_time(:millisecond)
      RaftFleet.command(Cluster, {:stopped_extra_members, Node.self(), index_or_group_name_or_nil, millis, @wait_time_before_forgetting_deactivated_node})
    end)
  end

  #
  # Adjust (add, remove, rebalance) member processes of existing consensus groups
  #
  defp adjust_consensus_member_sets(participating_nodes, groups) do
    Enum.each(groups, fn pair -> do_adjust(participating_nodes, pair) end)
  end

  defp do_adjust(_, {_, []}), do: :ok
  defp do_adjust(participating_nodes, {group_name, desired_member_nodes}) do
    # delegate to a defpt function for testing
    adjust_one_step(participating_nodes, group_name, desired_member_nodes)
  end

  defpt adjust_one_step(participating_nodes, group_name, [leader_node | _] = desired_member_nodes) do
    debug_assert(leader_node == Node.self(), "this node is supposed to host leader process of this group")
    case try_status(group_name) do
      %{state_name: :leader, from: leader, members: members, unresponsive_followers: unresponsive_followers} ->
        adjust_with_desired_leader(group_name, desired_member_nodes, leader, members, unresponsive_followers)
      status_or_reason ->
        # No leader in this node; now we have to collect statuses from all "relevant" nodes to judge what to do.
        relevant_nodes = relevant_node_set(participating_nodes)
        {node_with_status_pairs, node_with_error_reason_pairs} =
          try_fetch_all_node_statuses_or_reasons(group_name, relevant_nodes, leader_node, status_or_reason)
        noproc_nodes = for {n, :noproc} <- node_with_error_reason_pairs, into: MapSet.new(), do: n
        case find_leader_from_statuses(node_with_status_pairs) do
          {undesired_leader, leader_status} ->
            adjust_with_undesired_leader(group_name, desired_member_nodes, undesired_leader, leader_status, node_with_status_pairs, noproc_nodes)
          nil ->
            adjust_with_no_leader(group_name, relevant_nodes, node_with_status_pairs, noproc_nodes)
        end
    end
  end

  defp adjust_with_desired_leader(group_name,
                                  [leader_node | follower_nodes],
                                  leader,
                                  members,
                                  unresponsive_followers) do
    follower_nodes_from_leader = List.delete(members, leader) |> Enum.map(&node/1) |> Enum.sort()
    cond do
      (nodes_to_be_added = follower_nodes -- follower_nodes_from_leader) != [] ->
        Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), leader_node)
      (nodes_to_be_removed = follower_nodes_from_leader -- follower_nodes) != [] ->
        target_node = Enum.random(nodes_to_be_removed)
        target_pid  = Enum.find(members, fn m -> node(m) == target_node end)
        RaftedValue.remove_follower(leader, target_pid)
      unresponsive_followers != [] ->
        remove_follower_if_definitely_dead(group_name, leader, Enum.random(unresponsive_followers))
      true -> :ok
    end
  end

  defp adjust_with_undesired_leader(group_name,
                                    desired_member_nodes,
                                    undesired_leader,
                                    undesired_leader_status,
                                    node_with_status_pairs,
                                    noproc_nodes) do
    nodes_missing = desired_member_nodes -- Enum.map(node_with_status_pairs, &elem(&1, 0))
    dead_follower_pids =
      Map.get(undesired_leader_status || %{}, :unresponsive_followers, [])
      |> Enum.filter(&(node(&1) in noproc_nodes))
    cond do
      (nodes_to_be_added = nodes_missing -- Enum.map(dead_follower_pids, &node/1)) != [] ->
        Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), node(undesired_leader))
      nodes_missing != [] and dead_follower_pids != [] ->
        remove_definitely_dead_follower(group_name, undesired_leader, Enum.random(dead_follower_pids))
      true ->
        # As the previous cond branches don't match, there must be a member process in this node
        replace_leader_with_member_in_this_node_and_log(group_name, undesired_leader)
    end
  end

  defp adjust_with_no_leader(group_name, relevant_nodes, node_with_status_pairs, noproc_nodes) do
    if MapSet.equal?(relevant_nodes, noproc_nodes) do
      # Something really bad happened to this consensus group and now we are sure that there's no surviving member in `relevant_nodes`.
      recheck_that_no_survivor_exists_then_remove_consensus_group(group_name, relevant_nodes)
    else
      # Try to restore the consensus group by removing unhealthy and/or unnecessary member(s).
      {members_in_relevant_nodes, members_in_irrelevant_nodes} =
        Enum.flat_map(node_with_status_pairs, fn {_n, %{members: ms}} -> ms end)
        |> Enum.uniq()
        |> Enum.split_with(&(node(&1) in relevant_nodes))
      if members_in_irrelevant_nodes == [] do
        case Enum.split_with(members_in_relevant_nodes, fn m -> node(m) in noproc_nodes end) do
          {[], _} ->
            :ok # Nothing we can do, just wait and retry...
          {definitely_dead_members, probably_living_members} ->
            force_remove_definitely_dead_member(group_name, probably_living_members, Enum.random(definitely_dead_members))
        end
      else
        force_remove_member_in_irrelevant_node(group_name, members_in_relevant_nodes, Enum.random(members_in_irrelevant_nodes))
      end
    end
  end

  defp try_fetch_all_node_statuses_or_reasons(group_name, relevant_nodes, node_self, status_or_reason_self) do
    pairs_without_node_self =
      MapSet.delete(relevant_nodes, node_self)
      |> Enum.map(fn n -> {n, try_status({group_name, n})} end)
    pairs = [{node_self, status_or_reason_self} | pairs_without_node_self]
    Enum.split_with(pairs, &match?({_, %{}}, &1))
  end

  defp find_leader_from_statuses([]), do: nil
  defp find_leader_from_statuses(pairs) do
    (for {_node, %{state_name: :leader} = s} <- pairs, do: s)
    |> case do
      [] -> nil
      ss ->
        s = Enum.max_by(ss, &(&1.current_term))
        {s.leader, s}
    end
  end

  defp remove_follower_if_definitely_dead(group_name, leader, target_follower) do
    case try_status(target_follower) do
      :noproc -> remove_definitely_dead_follower(group_name, leader, target_follower)
      _       -> :ok
    end
  end

  defp remove_definitely_dead_follower(group_name, leader, target_follower) do
    remove_follower_and_log(leader, target_follower, "a member (#{inspect(target_follower)}) of #{group_name} is definitely dead; remove it from the group")
  end

  defp remove_follower_and_log(leader, target_follower, log_prefix) do
    ret = RaftedValue.remove_follower(leader, target_follower)
    Logger.info("#{log_prefix}: #{inspect(ret)}")
  end

  defp force_remove_definitely_dead_member(group_name, members, target_member) do
    log_message = "trying to force-remove a definitely dead member (#{inspect(target_member)}) of #{group_name}"
    force_remove_a_member_from_existing_members_and_log(group_name, members, target_member, log_message)
  end

  defp force_remove_member_in_irrelevant_node(group_name, members, target_member) do
    log_message = "trying to force-remove a member of #{group_name} in node #{node(target_member)} which is neither active nor connected"
    force_remove_a_member_from_existing_members_and_log(group_name, members, target_member, log_message)
  end

  defp force_remove_a_member_from_existing_members_and_log(group_name, members, target_member, log_message) do
    Logger.warn(log_message)
    Enum.each(members, fn m ->
      try do
        RaftedValue.force_remove_member(m, target_member)
      catch
        :exit, {reason, _} ->
          Logger.error("failed to force-remove a member #{inspect(m)} of #{group_name} from #{inspect(m)}: #{inspect(reason)}")
      end
    end)
  end

  defp replace_leader_with_member_in_this_node_and_log(group_name, current_leader) do
    ret = RaftedValue.replace_leader(current_leader, Process.whereis(group_name))
    Logger.info("migrating leader of #{group_name} in #{node(current_leader)} to the member in this node: #{inspect(ret)}")
  end

  defp recheck_that_no_survivor_exists_then_remove_consensus_group(group_name, relevant_nodes) do
    # Confirm that it's actually the case after sleep, in order to exclude the situation where the consensus group is just being added.
    :timer.sleep(5_000)
    if Enum.all?(relevant_nodes, fn n -> try_status({group_name, n}) == :noproc end) do
      ret = RaftFleet.remove_consensus_group(group_name)
      Logger.error("all members of #{group_name} have failed; removing the consensus group as a last resort: #{inspect(ret)}")
    end
  end

  #
  # Adjust member processes in `RaftFleet.Cluster` consensus group
  #
  defp remove_extra_members_in_cluster_consensus_if_leader_resides_in_this_node(participating_nodes) do
    leader_pid = LeaderPidCache.get(Cluster)
    if is_pid(leader_pid) and node(leader_pid) == Node.self() do
      remove_extra_members_in_cluster_consensus(leader_pid, participating_nodes)
    end
  end

  defp remove_extra_members_in_cluster_consensus(leader_pid, participating_nodes) do
    case RaftedValue.status(leader_pid) do
      %{unresponsive_followers: []} -> :ok
      %{members: member_pids, unresponsive_followers: unresponsive_pids} ->
        # Try to remove the following 2 types of pids:
        # - pids in irrelevant (not connected and not active) nodes should be cleaned up
        # - after supervisor restart of `RaftFleet.Cluster` process, pid of the dead process should be removed from consensus
        relevant_nodes = relevant_node_set(participating_nodes)
        healthy_member_nodes = Enum.map(member_pids -- unresponsive_pids, &node/1)
        pids_to_be_removed =
          Enum.filter(unresponsive_pids, fn pid ->
            n = node(pid)
            (n not in relevant_nodes) or (n in healthy_member_nodes)
          end)
        if pids_to_be_removed != [] do
          target_pid = Enum.random(pids_to_be_removed)
          RaftedValue.remove_follower(leader_pid, target_pid)
        end
    end
  end
end
