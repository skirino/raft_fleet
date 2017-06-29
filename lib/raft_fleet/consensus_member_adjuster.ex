use Croma

defmodule RaftFleet.ConsensusMemberAdjuster do
  require Logger
  alias RaftFleet.{Cluster, Manager, LeaderPidCache, Config}

  def adjust() do
    case RaftFleet.query(Cluster, {:consensus_groups, Node.self()}) do
      {:error, reason} ->
        Logger.info("querying all consensus groups failed: #{inspect(reason)}")
      {:ok, {participating_nodes, groups, removed_groups_queue}} ->
        leader_pid = LeaderPidCache.get(Cluster)
        kill_members_of_removed_groups(removed_groups_queue)
        adjust_consensus_member_sets(participating_nodes, groups)
        if is_pid(leader_pid) and node(leader_pid) == Node.self() do
          adjust_cluster_consensus_members(leader_pid)
        end
    end
  end

  defp kill_members_of_removed_groups(removed_groups_queue) do
    {removed1, removed2} = removed_groups_queue
    Enum.each(removed1, &stop_local_member/1)
    Enum.each(removed2, &stop_local_member/1)
  end

  defp stop_local_member(group_name) do
    case Process.whereis(group_name) do
      nil -> :ok
      pid -> :gen_fsm.stop(pid)
    end
  end

  defp adjust_consensus_member_sets(participating_nodes, groups) do
    unhealthy_members_counts =
      Enum.flat_map(groups, fn group -> do_adjust(participating_nodes, group) end)
      |> Enum.reduce(%{}, fn(node, map) -> Map.update(map, node, 1, &(&1 + 1)) end)
    threshold = Config.node_purge_threshold_failing_members()
    RaftFleet.command(Cluster, {:report_unhealthy_members, Node.self(), unhealthy_members_counts, threshold})
  end

  defp do_adjust(_, {_, []}), do: []
  defp do_adjust(participating_nodes, {group_name, desired_member_nodes}) do
    adjust_one_step(participating_nodes, group_name, desired_member_nodes)
  end

  defpt adjust_one_step(participating_nodes, group_name, [leader_node | follower_nodes] = desired_member_nodes) do
    # Note: `leader_node == Node.self()` always holds, as this node is supposed to host leader process of this `group`
    case try_status(group_name) do
      %{state_name: :leader, from: pid, members: members, unresponsive_followers: unresponsive_pids} ->
        follower_nodes_from_leader = Enum.map(members, &node/1) |> List.delete(leader_node) |> Enum.sort
        cond do
          (nodes_to_be_added = follower_nodes -- follower_nodes_from_leader) != [] ->
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), leader_node)
          (nodes_to_be_removed = follower_nodes_from_leader -- follower_nodes) != [] ->
            target_node = Enum.random(nodes_to_be_removed)
            target_pid  = Enum.find(members, fn m -> node(m) == target_node end)
            RaftedValue.remove_follower(pid, target_pid)
          unresponsive_pids != [] ->
            target_pid = Enum.random(unresponsive_pids)
            case try_status(target_pid) do
              :noproc ->
                Logger.info("A member (#{inspect(target_pid)}) in #{group_name} is definitely dead; remove it from the group")
                RaftedValue.remove_follower(pid, target_pid)
              _ -> :ok
            end
          true -> :ok
        end
        Enum.map(unresponsive_pids, &node/1)
      status_or_reason ->
        # We need to take both of the followings into account to correctly find member processes:
        # - currently connected nodes, which may already be deactivated but may still have member processes
        # - participating (active) nodes, which may not be connected due to temporary netsplit
        connected_nodes        = MapSet.new(Node.list())
        all_nodes              = Enum.into(participating_nodes, connected_nodes)
        all_nodes_without_self = MapSet.delete(all_nodes, leader_node)
        node_with_status_or_reason_pairs0 = Enum.map(all_nodes_without_self, fn n -> {n, try_status({group_name, n})} end)
        node_with_status_or_reason_pairs  = [{leader_node, status_or_reason} | node_with_status_or_reason_pairs0]
        {node_with_status_pairs, node_with_error_reason_pairs} = Enum.partition(node_with_status_or_reason_pairs, &match?({_, %{}}, &1))
        nodes_with_living_members = Enum.map(node_with_status_pairs, &elem(&1, 0))
        undesired_leader          = Enum.map(node_with_status_pairs, &elem(&1, 1)) |> find_leader_from_statuses()
        cond do
          undesired_leader == nil and majority_of_members_definitely_died?(group_name, node_with_status_pairs, node_with_error_reason_pairs) ->
            # Something really bad happened to this consensus group and it's impossible to rescue the group to healthy state;
            # remove the group as a last resort (to prevent from repeatedly failing to add followers).
            Logger.error("majority of members in #{group_name} have failed; remove the group as a last resort")
            RaftFleet.remove_consensus_group(group_name)
          (nodes_to_be_added = desired_member_nodes -- nodes_with_living_members) != [] ->
            Manager.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added), nil)
          undesired_leader != nil ->
            # As the previous cond branch doesn't match, there must be a member process in this node; replace the leader
            Logger.info("migrating leader of #{group_name} in #{node(undesired_leader)} to the member in this node")
            RaftedValue.replace_leader(undesired_leader, Process.whereis(group_name))
          true -> :ok
        end
        []
    end
  end

  defp try_status(dest) do
    try do
      RaftedValue.status(dest)
    catch
      :exit, {reason, _} -> reason # :noproc, {:nodedown, node}, :timeout
    end
  end

  defp majority_of_members_definitely_died?(group_name, node_with_status_pairs, node_with_error_reason_pairs) do
    if majority_of_members_absent?(node_with_status_pairs, node_with_error_reason_pairs) do
      # Confirm that it's actually the case after sleep, in order to exclude the situation where the consensus group is just being added.
      :timer.sleep(5_000)
      all_nodes = (node_with_status_pairs ++ node_with_error_reason_pairs) |> Enum.map(&elem(&1, 0))
      {node_with_status_pairs_after_sleep, node_with_error_reason_pairs_after_sleep} =
        Enum.map(all_nodes, fn n -> {n, try_status({group_name, n})} end)
        |> Enum.partition(&match?({_, %{}}, &1))
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

  defp find_leader_from_statuses([]), do: nil
  defp find_leader_from_statuses(statuses) do
    statuses_by_term = Enum.group_by(statuses, fn %{current_term: t} -> t end)
    latest_term = Map.keys(statuses_by_term) |> Enum.max()
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
    end
  end
end
