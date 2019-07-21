use Croma

defmodule RaftFleet.Cluster do
  alias RaftedValue.Data, as: RVData
  alias RaftFleet.{NodesPerZone, ConsensusGroups, RecentlyRemovedGroups, MembersPerLeaderNode, PerMemberOptions}

  defmodule Server do
    alias RaftFleet.Cluster

    defun start_link(rv_config :: RaftedValue.Config.t, name :: g[atom]) :: GenServer.on_start do
      # Use lock facility provided by :global module to avoid race conditions
      :global.trans({:raft_fleet_cluster_state_initialization, self()}, fn ->
        if not Enum.any?(Node.list(), fn n -> rafted_value_server_alive?({name, n}) end) do
          RaftedValue.start_link({:create_new_consensus_group, rv_config}, PerMemberOptions.build(name))
        end
      end, [Node.self() | Node.list()], 0)
      |> case do
        {:ok, pid} -> {:ok, pid}
        _          ->
          # Other server exists or cannot acquire lock; we need to retry since there may be no leader
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
        case RaftedValue.start_link({:join_existing_consensus_group, servers}, PerMemberOptions.build(name)) do
          {:ok, pid}  -> {:ok, pid}
          {:error, _} ->
            :timer.sleep(1_000)
            start_follower_with_retry(name, tries_remaining - 1)
        end
      end
    end

    defun child_spec() :: Supervisor.child_spec do
      rv_config =
        case RaftFleet.Config.rafted_value_config_maker() do
          nil -> Cluster.make_rv_config()
          mod -> mod.make(Cluster)
        end
      %{
        id:       __MODULE__,
        start:    {__MODULE__, :start_link, [rv_config, Cluster]},
        type:     :worker,
        restart:  :transient,
        shutdown: 5000,
      }
    end
  end

  defmodule State do
    use Croma.Struct, fields: [
      nodes_per_zone:          NodesPerZone,
      consensus_groups:        ConsensusGroups,
      recently_removed_groups: RecentlyRemovedGroups,
      members_per_leader_node: MembersPerLeaderNode, # this is cache; reproducible from `nodes` and `consensus_groups`
    ]

    defp migrate_from_older_version(state) do
      if Map.has_key?(state, :recently_removed_consensus_names) do
        state
        |> Map.delete(:recently_removed_consensus_names)
      else
        state
      end
    end

    def add_group(state0, group, n_replica) do
      %__MODULE__{nodes_per_zone:          nodes,
                  consensus_groups:        groups,
                  recently_removed_groups: rrgs,
                  members_per_leader_node: members} = state = migrate_from_older_version(state0)
      if Map.has_key?(groups, group) do
        {{:error, :already_added}, state}
      else
        if Enum.empty?(nodes) do
          {{:error, :no_active_node}, state}
        else
          if RecentlyRemovedGroups.cleanup_ongoing?(rrgs, group) do
            {{:error, :cleanup_ongoing}, state}
          else
            new_groups   = Map.put(groups, group, n_replica)
            [leader | _] = member_nodes = NodesPerZone.lrw_members(nodes, group, n_replica)
            pair         = {group, member_nodes}
            new_members  = Map.update(members, leader, [pair], &[pair | &1])
            new_state    = %__MODULE__{state | consensus_groups: new_groups, members_per_leader_node: new_members}
            {{:ok, member_nodes}, new_state}
          end
        end
      end
    end

    def remove_group(state0, group) do
      %__MODULE__{nodes_per_zone:          nodes,
                  consensus_groups:        groups,
                  recently_removed_groups: rrgs,
                  members_per_leader_node: members} = state = migrate_from_older_version(state0)
      if Map.has_key?(groups, group) do
        new_groups = Map.delete(groups, group)
        new_rrgs   = RecentlyRemovedGroups.add(rrgs, group)
        new_state  = %__MODULE__{state | consensus_groups: new_groups, recently_removed_groups: new_rrgs}
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

    def add_node(state0, n, z) do
      %__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state = migrate_from_older_version(state0)
      new_nodes = Map.update(nodes, z, [n], fn ns -> Enum.uniq([n | ns]) end)
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: compute_members(new_nodes, groups)}
    end

    def remove_node(state0, n) do
      %__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state = migrate_from_older_version(state0)
      new_nodes =
        Enum.reduce(nodes, %{}, fn({z, ns}, m) ->
          case ns do
            [^n] -> m
            _    -> Map.put(m, z, List.delete(ns, n))
          end
        end)
      new_members = compute_members(new_nodes, groups)
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: new_members}
    end

    defp compute_members(nodes, groups) do
      if map_size(nodes) == 0 do
        %{}
      else
        Enum.map(groups, fn {group, n_replica} -> {group, NodesPerZone.lrw_members(nodes, group, n_replica)} end)
        |> Enum.group_by(fn {_, members} -> hd(members) end)
      end
    end

    def update_removed_groups(state0, node, index_or_nil, now, wait_time) do
      %__MODULE__{nodes_per_zone: npz,
                  recently_removed_groups: rrgs} = state = migrate_from_older_version(state0)
      new_rrgs = RecentlyRemovedGroups.update(rrgs, npz, node, index_or_nil, now, wait_time)
      %__MODULE__{state | recently_removed_groups: new_rrgs}
    end
  end

  defmodule Hook do
    alias RaftFleet.Manager

    @behaviour RaftedValue.LeaderHook

    @impl true
    def on_command_committed(_state_before, entry, ret, _state_after) do
      case entry do
        {:add_group, group_name, _, rv_config, leader_node} ->
          # Start leader only when this hook is run on the same node to which :add_group command was submitted
          # in order not to spawn multiple leaders even if this log entry is committed multiple times
          if Node.self() == leader_node do
            case ret do
              {:error, _}  -> nil
              {:ok, nodes} ->
                # If it's currently restoring from logs & snapshot files, consensus groups are started
                # at the last by `on_restored_from_files/1` (to avoid confusion due to already removed groups).
                restoring? = Process.get(:rafted_value_restoring, false)
                if not restoring? do
                  Manager.start_consensus_group_members(group_name, rv_config, nodes)
                end
            end
          end
        _ -> nil
      end
    end

    @impl true
    def on_query_answered(_, _, _), do: nil

    @impl true
    def on_follower_added(_, _), do: nil

    @impl true
    def on_follower_removed(_, _), do: nil

    @impl true
    def on_elected(_), do: nil

    @impl true
    def on_restored_from_files(%State{consensus_groups: gs}) do
      case RaftFleet.Config.rafted_value_config_maker() do
        nil -> :ok
        mod ->
          # As it's right after 1st activation, it's highly likely that the cluster consists of only 1 node;
          # let's start consensus groups with 1 member.
          target_nodes = [Node.self()]
          Enum.each(gs, fn {g, _} ->
            Manager.start_consensus_group_members(g, mod.make(g), target_nodes)
          end)
      end
    end
  end

  @behaviour RVData
  @typep t :: State.t

  @impl true
  defun new() :: t do
    %State{nodes_per_zone: %{}, consensus_groups: %{}, recently_removed_groups: RecentlyRemovedGroups.empty(), members_per_leader_node: %{}}
  end

  @impl true
  defun command(data :: t, arg :: RVData.command_arg) :: {RVData.command_ret, t} do
    (data, {:add_group, group, n, _rv_config, _node}   ) -> State.add_group(data, group, n) # `rv_config` and `node` will be used in `Hook`
    (data, {:remove_group, group}                          ) -> State.remove_group(data, group)
    (data, {:add_node, node, zone}                         ) -> {:ok, State.add_node(data, node, zone)}
    (data, {:remove_node, node}                            ) -> {:ok, State.remove_node(data, node)}
    (data, {:stopped_extra_members, node, index, now, wait}) -> {:ok, State.update_removed_groups(data, node, index, now, wait)}
    (data, _                                               ) -> {{:error, :invalid_command}, data} # For safety
  end

  @impl true
  defun query(data :: t, arg :: RVData.query_arg) :: RVData.query_ret do
    (%State{nodes_per_zone: nodes, members_per_leader_node: members, recently_removed_groups: removed}, {:consensus_groups, node}) ->
      participating_nodes = Enum.flat_map(nodes, fn {_z, ns} -> ns end)
      groups_led_by_the_node = Map.get(members, node, [])
      removed_groups_with_index = RecentlyRemovedGroups.names_for_node(removed, node)
      {participating_nodes, groups_led_by_the_node, removed_groups_with_index}
    (%State{nodes_per_zone: nodes}, :active_nodes)        -> nodes
    (%State{consensus_groups: groups}, :consensus_groups) -> groups
    (_, _)                                                -> {:error, :invalid_query} # For safety
  end

  @default_rv_config_options [
    election_timeout_clock_drift_margin: 500,
    leader_hook_module:                  Hook,
  ]

  defun make_rv_config(rv_config_options :: Keyword.t \\ @default_rv_config_options) :: RaftedValue.Config.t do
    opts = Keyword.put(rv_config_options, :leader_hook_module, Hook) # :leader_hook_module must not be changed
    RaftedValue.make_config(__MODULE__, opts)
  end

  @deprecated "Use `make_rv_config/1` instead"
  defun default_rv_config() :: RaftedValue.Config.t do
    RaftedValue.make_config(__MODULE__, @default_rv_config_options)
  end
end
