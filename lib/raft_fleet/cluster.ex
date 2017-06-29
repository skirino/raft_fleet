use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Cluster do
  alias RaftFleet.{NodesPerZone, ConsensusGroups, CappedQueue, MembersPerLeaderNode, UnhealthyMembersCountsMap, Config}

  defmodule Server do
    defun start_link(rv_config :: RaftedValue.Config.t, name :: g[atom]) :: GenServer.on_start do
      # Use lock facility provided by :global module to avoid race conditions
      result =
        :global.trans({:raft_fleet_cluster_state_initialization, self()}, fn ->
          if !Enum.any?(Node.list(), fn n -> rafted_value_server_alive?({name, n}) end) do
            options =
              case Config.persistence_dir_parent() do
                nil -> [name: name]
                dir -> [name: name, persistence_dir: Path.join(dir, Atom.to_string(RaftFleet.Cluster))]
              end
            RaftedValue.start_link({:create_new_consensus_group, rv_config}, options)
          end
        end, [Node.self() | Node.list()], 0)
      case result do
        {:ok, pid} -> {:ok, pid}
        _          ->
          # Other server exists or cannot acquire lock; we need retry as there may be no leader
          start_follower_with_retry(name, 3)
      end
    end

    defunp rafted_value_server_alive?(server :: {atom, node}) :: boolean do
      try do
        _ = RaftedValue.status(server)
        true
      catch
        :exit, {:noproc, _} -> false
      end
    end

    defunp start_follower_with_retry(name :: atom, tries_remaining :: non_neg_integer) :: GenServer.on_start do
      if tries_remaining == 0 do
        {:error, :no_leader}
      else
        servers = Node.list() |> Enum.map(fn n -> {name, n} end)
        case RaftedValue.start_link({:join_existing_consensus_group, servers}, [name: name]) do
          {:ok, pid}  -> {:ok, pid}
          {:error, _} ->
            :timer.sleep(1_000)
            start_follower_with_retry(name, tries_remaining - 1)
        end
      end
    end

    defun child_spec() :: Supervisor.Spec.spec do
      alias RaftFleet.Cluster, as: C
      rv_config = RaftedValue.make_config(C, [leader_hook_module: C.Hook, election_timeout_clock_drift_margin: 500])
      Supervisor.Spec.worker(__MODULE__, [rv_config, C], [restart: :transient])
    end
  end

  defmodule State do
    use Croma.Struct, fields: [
      nodes_per_zone:                   NodesPerZone,
      consensus_groups:                 ConsensusGroups,
      recently_removed_consensus_names: CappedQueue,
      members_per_leader_node:          MembersPerLeaderNode,      # this is cache; reproducible from `nodes` and `consensus_groups`
      unhealthy_members_map:            UnhealthyMembersCountsMap, # reported from all active nodes; used to calculate `node_to_purge`
      node_to_purge:                    TG.nilable(Croma.Atom),    # node that has too many unhealthy raft members
    ]

    def add_group(%__MODULE__{nodes_per_zone:                   nodes,
                              consensus_groups:                 groups,
                              recently_removed_consensus_names: removed,
                              members_per_leader_node:          members} = state,
                  group,
                  n_replica) do
      if Map.has_key?(groups, group) do
        {{:error, :already_added}, state}
      else
        if Enum.empty?(nodes) do
          {{:error, :no_active_node}, state}
        else
          new_groups   = Map.put(groups, group, n_replica)
          new_removed  = CappedQueue.filter(removed, &(&1 != group))
          [leader | _] = member_nodes = NodesPerZone.lrw_members(nodes, group, n_replica)
          pair         = {group, member_nodes}
          new_members  = Map.update(members, leader, [pair], &[pair | &1])
          new_state    = %__MODULE__{state | consensus_groups: new_groups, recently_removed_consensus_names: new_removed, members_per_leader_node: new_members}
          {{:ok, member_nodes}, new_state}
        end
      end
    end

    def remove_group(%__MODULE__{nodes_per_zone:                   nodes,
                                 consensus_groups:                 groups,
                                 recently_removed_consensus_names: removed,
                                 members_per_leader_node:          members} = state,
                     group) do
      if Map.has_key?(groups, group) do
        new_groups  = Map.delete(groups, group)
        new_removed = CappedQueue.enqueue(removed, group)
        new_state   = %__MODULE__{state | consensus_groups: new_groups, recently_removed_consensus_names: new_removed}
        if map_size(nodes) == 0 do
          {:ok, new_state}
        else
          [leader] = NodesPerZone.lrw_members(nodes, group, 1)
          new_group_members_pairs = members[leader] |> Enum.reject(&match?({^group, _}, &1))
          new_members =
            case new_group_members_pairs do
              []         -> Map.delete(members, leader)
              _not_empty -> Map.put(members, leader, new_group_members_pairs)
            end
          {:ok, %__MODULE__{new_state | members_per_leader_node: new_members}}
        end
      else
        {{:error, :not_found}, state}
      end
    end

    def add_node(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state, n, z) do
      new_nodes  = Map.update(nodes, z, [n], fn ns -> Enum.uniq([n | ns]) end)
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: compute_members(new_nodes, groups)}
    end

    def remove_node(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups, unhealthy_members_map: umm, node_to_purge: node_to_purge} = state, n) do
      new_nodes =
        Enum.reduce(nodes, %{}, fn({z, ns}, m) ->
          case ns do
            [^n] -> m
            _    -> Map.put(m, z, List.delete(ns, n))
          end
        end)
      new_members       = compute_members(new_nodes, groups)
      new_umm           = UnhealthyMembersCountsMap.remove_node(umm, n)
      new_node_to_purge = if node_to_purge == n, do: nil, else: node_to_purge
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: new_members, unhealthy_members_map: new_umm, node_to_purge: new_node_to_purge}
    end

    defp compute_members(nodes, groups) do
      if map_size(nodes) == 0 do
        %{}
      else
        Enum.map(groups, fn {group, n_replica} -> {group, NodesPerZone.lrw_members(nodes, group, n_replica)} end)
        |> Enum.group_by(fn {_, members} -> hd(members) end)
      end
    end

    def update_unhealthy_members(%__MODULE__{nodes_per_zone: nodes, unhealthy_members_map: umm} = state, from_node, counts, threshold) do
      participating_nodes = Enum.flat_map(nodes, fn {_z, ns} -> ns end)
      new_umm       = Map.put(umm, from_node, Map.take(counts, participating_nodes))
      node_to_purge = UnhealthyMembersCountsMap.most_unhealthy_node(new_umm, threshold)
      %__MODULE__{state | unhealthy_members_map: new_umm, node_to_purge: node_to_purge}
    end
  end

  defmodule Hook do
    alias RaftFleet.Manager

    @behaviour RaftedValue.LeaderHook

    def on_command_committed(state_before, entry, ret, state_after) do
      case entry do
        {:add_group, group_name, _, rv_config, leader_node} ->
          # Start leader only when this hook is run on the same node to which :add_group command was submitted
          # in order not to spawn multiple leaders even if this log entry is committed multiple times
          if Node.self() == leader_node do
            case ret do
              {:error, _}  -> nil
              {:ok, nodes} -> Manager.start_consensus_group_members(group_name, rv_config, nodes)
            end
          end
        {:remove_node, _}                    -> notify_if_node_to_purge_changed(state_before, state_after)
        {:report_unhealthy_members, _, _, _} -> notify_if_node_to_purge_changed(state_before, state_after)
        _ -> nil
      end
    end
    def on_query_answered(_, _, _), do: nil
    def on_follower_added(_, _), do: nil
    def on_follower_removed(_, _), do: nil
    def on_elected(state) do
      Manager.node_purge_candidate_changed(state.node_to_purge)
    end

    defp notify_if_node_to_purge_changed(state_before, state_after) do
      node_to_purge = state_after.node_to_purge
      if node_to_purge != state_before.node_to_purge do
        Manager.node_purge_candidate_changed(node_to_purge)
      end
    end
  end

  @behaviour RaftedValue.Data
  @typep t :: State.t

  defun new() :: t do
    q = CappedQueue.new(100)
    %State{nodes_per_zone: %{}, consensus_groups: %{}, recently_removed_consensus_names: q, members_per_leader_node: %{}, unhealthy_members_map: %{}}
  end

  defun command(data :: t, arg :: Data.command_arg) :: {Data.command_ret, t} do
    (data, {:add_group, group, n, _rv_config, _node}    ) -> State.add_group(data, group, n)
    (data, {:remove_group, group}                       ) -> State.remove_group(data, group)
    (data, {:add_node, node, zone}                      ) -> {:ok, State.add_node(data, node, zone)}
    (data, {:remove_node, node}                         ) -> {:ok, State.remove_node(data, node)}
    (data, {:report_unhealthy_members, from, counts, th}) -> {:ok, State.update_unhealthy_members(data, from, counts, th)}
    (data, _                                            ) -> {{:error, :invalid_command}, data} # For safety
  end

  defun query(data :: t, arg :: Data.query_arg) :: Data.query_ret do
    (%State{nodes_per_zone: nodes, members_per_leader_node: members, recently_removed_consensus_names: removed}, {:consensus_groups, node}) ->
      participating_nodes = Enum.flat_map(nodes, fn {_z, ns} -> ns end)
      groups_led_by_the_node = Map.get(members, node, [])
      {participating_nodes, groups_led_by_the_node, CappedQueue.underlying_queue(removed)}
    (%State{nodes_per_zone: nodes}, :active_nodes)        -> nodes
    (%State{consensus_groups: groups}, :consensus_groups) -> groups
    (_, _)                                                -> {:error, :invalid_query} # For safety
  end
end
