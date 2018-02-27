use Croma

defmodule RaftFleet.ConsensusMemberAdjuster do
  require Logger
  alias RaftFleet.{Cluster, Manager, LeaderPidCache}

  # Currently this is fixed; simply I don't know whether this should be customizable or not.
  @wait_time_before_forgetting_deactivated_node 30 * 60_000

  def adjust() do
    case RaftFleet.query(Cluster, {:consensus_groups, Node.self()}) do
      {:error, reason} ->
        Logger.info("querying all consensus groups failed: #{inspect(reason)}")
      {:ok, {participating_nodes, groups, removed_groups}} ->
        kill_members_of_removed_groups(removed_groups)
        adjust_consensus_member_sets(participating_nodes, groups)
        leader_pid = LeaderPidCache.get(Cluster)
        if is_pid(leader_pid) and node(leader_pid) == Node.self() do
          adjust_cluster_consensus_members(leader_pid)
        end
    end
  end

  defp kill_members_of_removed_groups({removed1, removed2}) when is_list(removed2) do
    # interacting with `RaftFleet.Cluster` of version `< 0.7.0`; this clause will be removed in a future release.
    Enum.each(removed1, &stop_members_of_removed_group/1)
    Enum.each(removed2, &stop_members_of_removed_group/1)
    notify_completion_of_cleanup(List.first(removed1)) # as no `index` is available here, we use the latest removed group name
  end
  defp kill_members_of_removed_groups({removed_groups, index}) do
    # interacting with `RaftFleet.Cluster` of version `>= 0.7.0`
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
      millis = System.system_time(:milliseconds)
      RaftFleet.command(Cluster, {:stopped_extra_members, Node.self(), index_or_group_name_or_nil, millis, @wait_time_before_forgetting_deactivated_node})
    end)
  end

  defp adjust_consensus_member_sets(participating_nodes, groups) do
    Enum.each(groups, fn group -> do_adjust(participating_nodes, group) end)
  end

  defp do_adjust(_, {_, []}), do: []
  defp do_adjust(participating_nodes, {group_name, desired_member_nodes}) do
    adjust_one_step(participating_nodes, group_name, desired_member_nodes)
  end

  defpt adjust_one_step(participating_nodes, group_name, [leader_node | follower_nodes] = desired_member_nodes) do
    # Note: `leader_node == Node.self()` always holds, as this node is supposed to host leader process of this `group`
    case try_status(group_name) do
      %{state_name: :leader, from: pid, members: members, unresponsive_followers: unresponsive_pids} ->
        follower_nodes_from_leader = List.delete(members, pid) |> Enum.map(&node/1) |> Enum.sort()
        cond do
          (nodes_to_be_added = follower_nodes -- follower_nodes_from_leader) != [] ->
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), leader_node)
          (nodes_to_be_removed = follower_nodes_from_leader -- follower_nodes) != [] ->
            target_node = Enum.random(nodes_to_be_removed)
            target_pid  = Enum.find(members, fn m -> node(m) == target_node end)
            RaftedValue.remove_follower(pid, target_pid)
          unresponsive_pids != [] ->
            remove_dead_follower(group_name, pid, unresponsive_pids)
          true -> :ok
        end
      status_or_reason ->
        # We need to take both of the followings into account to correctly find member processes:
        # - currently connected nodes, which may already be deactivated but may still have member processes
        # - participating (active) nodes, which may not be connected due to temporary netsplit
        connected_nodes        = MapSet.new(Node.list())
        all_nodes              = Enum.into(participating_nodes, connected_nodes)
        all_nodes_without_self = MapSet.delete(all_nodes, leader_node)
        node_with_status_or_reason_pairs0 = Enum.map(all_nodes_without_self, fn n -> {n, try_status({group_name, n})} end)
        node_with_status_or_reason_pairs  = [{leader_node, status_or_reason} | node_with_status_or_reason_pairs0]
        {node_with_status_pairs, node_with_error_reason_pairs} = Enum.split_with(node_with_status_or_reason_pairs, &match?({_, %{}}, &1))
        nodes_with_living_members = Enum.map(node_with_status_pairs, &elem(&1, 0))
        {undesired_leader, undesired_leader_status} = Enum.map(node_with_status_pairs, &elem(&1, 1)) |> find_leader_from_statuses()
        nodes_missing               = desired_member_nodes -- nodes_with_living_members
        nodes_without_living_member = for {n, reason} <- node_with_error_reason_pairs, reason == :noproc, into: MapSet.new(), do: n
        dead_follower_pids          = Map.get(undesired_leader_status || %{}, :unresponsive_followers, []) |> Enum.filter(&(node(&1) in nodes_without_living_member))
        cond do
          undesired_leader == nil and majority_of_members_definitely_died?(group_name, node_with_status_pairs, node_with_error_reason_pairs) ->
            # Something really bad happened to this consensus group and it's impossible to rescue the group to healthy state;
            # remove the group as a last resort (to prevent from repeatedly failing to add followers).
            ret = RaftFleet.remove_consensus_group(group_name)
            Logger.error("majority of members in #{group_name} have failed; remove the group as a last resort: #{inspect(ret)}")
          (nodes_to_be_added = nodes_missing -- Enum.map(dead_follower_pids, &node/1)) != [] ->
            leader_node_hint = if is_nil(undesired_leader), do: nil, else: node(undesired_leader)
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), leader_node_hint)
          undesired_leader != nil and nodes_missing != [] and dead_follower_pids != [] ->
            remove_dead_follower(group_name, undesired_leader, dead_follower_pids)
          undesired_leader != nil ->
            # As the previous cond branch doesn't match, there must be a member process in this node; replace the leader
            ret = RaftedValue.replace_leader(undesired_leader, Process.whereis(group_name))
            Logger.info("migrating leader of #{group_name} in #{node(undesired_leader)} to the member in this node: #{inspect(ret)}")
          true -> :ok
        end
    end
  end

  defp try_status(dest) do
    try do
      RaftedValue.status(dest)
    catch
      :exit, {reason, _} -> reason # :noproc | {:nodedown, node} | :timeout
    end
  end

  defp remove_dead_follower(group_name, leader, unresponsive_followers) do
    target_pid = Enum.random(unresponsive_followers)
    case try_status(target_pid) do
      :noproc ->
        ret = RaftedValue.remove_follower(leader, target_pid)
        Logger.info("A member (#{inspect(target_pid)}) in #{group_name} is definitely dead; remove it from the group: #{inspect(ret)}")
      _ -> :ok
    end
  end

  defp majority_of_members_definitely_died?(group_name, node_with_status_pairs, node_with_error_reason_pairs) do
    if majority_of_members_absent?(node_with_status_pairs, node_with_error_reason_pairs) do
      # Confirm that it's actually the case after sleep, in order to exclude the situation where the consensus group is just being added.
      :timer.sleep(5_000)
      all_nodes = (node_with_status_pairs ++ node_with_error_reason_pairs) |> Enum.map(&elem(&1, 0))
      {node_with_status_pairs_after_sleep, node_with_error_reason_pairs_after_sleep} =
        Enum.map(all_nodes, fn n -> {n, try_status({group_name, n})} end)
        |> Enum.split_with(&match?({_, %{}}, &1))
      majority_of_members_absent?(node_with_status_pairs_after_sleep, node_with_error_reason_pairs_after_sleep)
    else
      false
    end
  end

  defp majority_of_members_absent?(node_with_status_pairs, node_with_error_reason_pairs) do
    if Enum.all?(node_with_error_reason_pairs, &match?({_, :noproc}, &1)) do
      noproc_nodes = Enum.map(node_with_error_reason_pairs, &elem(&1, 0))
      Enum.all?(node_with_status_pairs, fn {_, %{members: members}} ->
        member_nodes = Enum.map(members, &node/1) |> Enum.uniq()
        n_living_members = length(member_nodes -- noproc_nodes)
        2 * n_living_members <= length(members)
      end)
    else
      # There's at least one node whose member status is unclear; the consensus members may reside in the other side of netsplit.
      # Be conservative and don't try to remove consensus group
      false
    end
  end

  defp find_leader_from_statuses([]), do: {nil, nil}
  defp find_leader_from_statuses(statuses) do
    Enum.filter(statuses, &match?(%{state_name: :leader}, &1))
    |> case do
      [] -> {nil, nil}
      ss ->
        s = Enum.max_by(ss, &(&1.current_term))
        {s.leader, s}
    end
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
    end
  end
end
