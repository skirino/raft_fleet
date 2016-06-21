use Croma

defmodule RaftFleet.MemberAdjuster do
  alias RaftFleet.{Cluster, MemberSup}

  def adjust do
    case RaftFleet.query(Cluster, {:consensus_groups, Node.self}) do
      {:error, _}                           -> :ok
      {:ok, {groups, removed_groups_queue}} ->
        {removed1, removed2} = removed_groups_queue
        Enum.each(removed1, &brutally_kill/1)
        Enum.each(removed2, &brutally_kill/1)
        Enum.each(groups, &do_adjust/1)
    end
  end

  defp brutally_kill(group_name) do
    case Process.whereis(group_name) do
      nil -> :ok
      pid -> :gen_fsm.stop(pid)
    end
  end

  defp do_adjust({_, []}), do: :ok
  defp do_adjust({group_name, nodes}), do: adjust_one_step(group_name, nodes)

  defpt adjust_one_step(group_name, [leader_node | follower_nodes] = all_nodes) do
    case try_status({group_name, leader_node}) do
      %{state_name: :leader, members: members} ->
        follower_nodes_from_leader = Enum.map(members, &node/1) |> List.delete(leader_node) |> Enum.sort
        cond do
          (nodes_to_be_added = follower_nodes -- follower_nodes_from_leader) != [] ->
            MemberSup.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added))
          (nodes_to_be_removed = follower_nodes_from_leader -- follower_nodes) != [] ->
            MemberSup.remove(group_name, Enum.random(nodes_to_be_removed))
          true -> :ok
        end
      status_or_nil ->
        pairs0 =
          [Node.self | Node.list]
          |> List.delete(leader_node)
          |> Enum.map(fn n -> {n, try_status({group_name, n})} end)
          |> Enum.reject(&match?({_, nil}, &1))
        pairs = if status_or_nil, do: [{leader_node, status_or_nil} | pairs0], else: pairs0
        nodes_with_living_members = Enum.reject(pairs, &match?({_, nil}, &1)) |> Enum.map(fn {n, _} -> n end)
        cond do
          (nodes_to_be_added = all_nodes -- nodes_with_living_members) != [] ->
            MemberSup.start_consensus_group_follower(group_name, Enum.random(nodes_to_be_added))
          undesired_leader = find_undesired_leader(pairs0) ->
            RaftedValue.replace_leader(undesired_leader, Process.whereis(group_name))
          true -> :ok
        end
    end
  end

  defp try_status(dest) do
    try do
      RaftedValue.status(dest)
    catch
      :exit, _ -> nil
    end
  end

  defp find_undesired_leader(pairs) do
    statuses = Enum.map(pairs, fn {_, s} -> s end)
    statuses_by_term = Enum.group_by(statuses, fn %{current_term: t} -> t end)
    latest_term = Map.keys(statuses_by_term) |> Enum.max
    statuses_by_term[latest_term]
    |> Enum.find_value(fn
      %{state_name: :leader, from: from} -> from
      _                                  -> nil
    end)
  end
end
