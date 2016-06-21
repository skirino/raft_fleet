use Croma

defmodule RaftFleet.Cluster do
  alias RaftFleet.{NodesPerZone, ConsensusGroups, CappedQueue, MembersPerLeaderNode, UnhealthyRaftMembers}

  defmodule Server do
    defun start_link(config :: RaftedValue.Config.t, name :: g[atom]) :: GenServer.on_start do
      servers = Node.list |> Enum.map(fn n -> {name, n} end)
      if Enum.any?(servers, &rafted_value_server_alive?/1) do
        RaftedValue.start_link({:join_existing_consensus_group, servers}, name)
      else
        # FIXME: race condition to spawn multiple leaders each of which leads its own consensus group
        RaftedValue.start_link({:create_new_consensus_group, config}, name)
      end
    end

    defun rafted_value_server_alive?(server :: {atom, node}) :: boolean do
      try do
        _ = RaftedValue.status(server)
        true
      catch
        :exit, {:noproc, _} -> false
      end
    end
  end

  defmodule State do
    use Croma.Struct, fields: [
      nodes_per_zone:                   NodesPerZone,
      consensus_groups:                 ConsensusGroups,
      recently_removed_consensus_names: CappedQueue,
      members_per_leader_node:          MembersPerLeaderNode, # this is cache; reproducible from `nodes` and `consensus_groups`
      unhealthy_members:                UnhealthyRaftMembers, # will be used to identify unhealthy nodes
    ]

    def add_node(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state, n, z) do
      new_nodes  = Map.update(nodes, z, [n], &[n | &1])
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: compute_members(new_nodes, groups)}
      |> __MODULE__.validate!
    end

    def remove_node(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state, n) do
      new_nodes =
        Enum.reduce(nodes, %{}, fn({z, ns}, m) ->
          case ns do
            [^n] -> m
            _    -> Map.put(m, z, List.delete(ns, n))
          end
        end)
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: compute_members(new_nodes, groups)}
      |> __MODULE__.validate!
    end

    def add_group(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups, members_per_leader_node: members} = state, group, n_replica) do
      if Map.has_key?(groups, group) do
        {{:error, :already_added}, state}
      else
        new_groups = Map.put(groups, group, n_replica)
        if map_size(nodes) == 0 do
          {{:ok, []}, %__MODULE__{state | consensus_groups: new_groups}}
        else
          [leader | _] = member_nodes = NodesPerZone.lrw_members(nodes, group, n_replica)
          pair = {group, member_nodes}
          new_members = Map.update(members, leader, [pair], &[pair | &1])
          new_state = %__MODULE__{state | consensus_groups: new_groups, members_per_leader_node: new_members}
          {{:ok, member_nodes}, new_state}
        end
      end
    end

    def remove_group(%__MODULE__{nodes_per_zone: nodes, consensus_groups: groups, recently_removed_consensus_names: removed, members_per_leader_node: members} = state, group) do
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

    defp compute_members(nodes, groups) do
      if map_size(nodes) == 0 do
        %{}
      else
        Enum.map(groups, fn {group, n_replica} -> {group, NodesPerZone.lrw_members(nodes, group, n_replica)} end)
        |> Enum.group_by(fn {_, members} -> hd(members) end)
      end
    end
  end

  @type t :: State.t

  defmodule Hook do
    @behaviour RaftedValue.LeaderHook

    def on_command_committed(_, entry, ret, _state_after) do
      case entry do
        {:add_group, group_name, _, rv_config} ->
          case ret do
            {:ok, []}         -> nil
            {:ok, [node | _]} -> RaftFleet.MemberSup.start_consensus_group_leader(group_name, node, rv_config)
            {:error, _}       -> nil
          end
        _ -> nil
      end
    end
    def on_query_answered(_, _, _), do: nil
    def on_follower_added(_), do: nil
    def on_follower_removed(_), do: nil
    def on_elected, do: nil
  end

  @behaviour RaftedValue.Data

  defun new :: t do
    q = CappedQueue.new(100)
    %State{nodes_per_zone: %{}, consensus_groups: %{}, recently_removed_consensus_names: q, members_per_leader_node: %{}, unhealthy_members: %{}}
  end

  defun command(data :: t, arg :: Data.command_arg) :: {Data.command_ret, t} do
    (data, {:add_node, node, zone}           ) -> {:ok, State.add_node(data, node, zone)}
    (data, {:remove_node, node}              ) -> {:ok, State.remove_node(data, node)}
    (data, {:add_group, group, n, _rv_config}) -> State.add_group(data, group, n)
    (data, {:remove_group, group}            ) -> State.remove_group(data, group)
  end

  defun query(data :: t, arg :: Data.query_arg) :: Data.query_ret do
    (%State{members_per_leader_node: members, recently_removed_consensus_names: removed}, {:consensus_groups, node}) ->
      groups_led_by_the_node = Map.get(members,node, [])
      {groups_led_by_the_node, CappedQueue.underlying_queue(removed)}
  end

  defun rv_config :: RaftedValue.Config.t do
    RaftedValue.make_config(__MODULE__, [leader_hook_module: Hook, election_timeout_clock_drift_margin: 500])
  end
end
