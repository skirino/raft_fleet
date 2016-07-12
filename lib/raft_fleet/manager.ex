use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Manager do
  use GenServer
  alias RaftFleet.{Cluster, ConsensusMemberSup, ConsensusMemberAdjuster, Activator, Deactivator, Config}

  defmodule State do
    use Croma.Struct, fields: [
      adjust_timer:                 TG.nilable(Croma.Reference),
      adjust_worker:                TG.nilable(Croma.Pid),
      activate_worker:              TG.nilable(Croma.Pid),
      deactivate_worker:            TG.nilable(Croma.Pid),
      purge_wait_timer:             TG.nilable(Croma.Reference),
      being_added_consensus_groups: Croma.Map,
    ]

    def phase(%__MODULE__{adjust_timer: t, activate_worker: a, deactivate_worker: d}) do
      cond do
        t    -> :active
        a    -> :activating
        d    -> :deactivating
        true -> :inactive
      end
    end
  end

  defun start_link :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    Process.flag(:trap_exit, true)
    {:ok, %State{being_added_consensus_groups: %{}}}
  end

  def handle_call({:activate, zone}, _from, state) do
    if State.phase(state) == :inactive do
      new_state = %State{state | activate_worker: spawn_link_monitor(Activator, :activate, [zone])}
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :not_inactive}, state}
    end
  end
  def handle_call(:deactivate, _from, state) do
    if State.phase(state) == :active do
      new_state = %State{state | deactivate_worker: spawn_link_monitor(Deactivator, :deactivate, [])} |> stop_timer
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :inactive}, state}
    end
  end
  def handle_call({:await_completion_of_adding_consensus_group, name}, from, %State{being_added_consensus_groups: gs} = state) do
    if State.phase(state) in [:active, :activating] do
      case gs[name] do
        :leader_started -> {:reply, :ok, %State{state | being_added_consensus_groups: Map.delete(gs, name)}}
        _               -> {:noreply, %State{state | being_added_consensus_groups: Map.put(gs, name, from)}}
      end
    else
      {:reply, {:error, :inactive}, state}
    end
  end

  def handle_cast({:node_purge_candidate_changed, node_to_purge}, %State{purge_wait_timer: ref1} = state) do
    if State.phase(state) == :active do
      if ref1, do: Process.cancel_timer(ref1)
      ref2 =
        if node_to_purge do
          Process.send_after(self, {:purge_node, node_to_purge}, Config.node_purge_failure_time_window)
        else
          nil
        end
      {:noreply, %State{state | purge_wait_timer: ref2}}
    else
      {:noreply, state}
    end
  end
  def handle_cast({:start_consensus_group_members, name, rv_config, member_nodes}, %State{being_added_consensus_groups: gs} = state) do
    if State.phase(state) in [:active, :activating] do
      # Spawn leader in this node (neglecting desired leader node defined by randezvous hashing) to avoid potential failures
      {:ok, _} = Supervisor.start_child(ConsensusMemberSup, [{:create_new_consensus_group, rv_config}, name])
      List.delete(member_nodes, Node.self)
      |> Enum.each(fn n ->
        start_consensus_group_follower(name, n)
      end)
      new_gs =
        case gs[name] do
          {_, _} = from ->
            GenServer.reply(from, :ok)
            Map.delete(gs, name)
          _ -> Map.put(gs, name, :leader_started)
        end
      {:noreply, %State{state | being_added_consensus_groups: new_gs}}
    else
      {:noreply, state}
    end
  end
  def handle_cast({:start_consensus_group_follower, name}, state) do
    if State.phase(state) == :active do
      other_node_members = Enum.map(Node.list, fn n -> {name, n} end)
      # `start_child/2` may fail due to `:uncommitted_membership_change`;
      # just neglect the error here and let `ConsensusMemberAdjuster` retry this operation.
      # In addition, to avoid blocking the Manager process indefinitely, we spawn a temporary process solely for `start_child/2`.
      spawn_link(Supervisor, :start_child, [ConsensusMemberSup, [{:join_existing_consensus_group, other_node_members}, name]])
    end
    {:noreply, state}
  end

  def handle_info(:adjust_members, %State{adjust_worker: worker} = state) do
    new_state =
      if worker do
        state # don't invoke multiple workers
      else
        %State{state | adjust_worker: spawn_link_monitor(ConsensusMemberAdjuster, :adjust, [])}
      end
    if State.phase(state) == :active do
      {:noreply, start_timer(new_state)}
    else
      {:noreply, new_state}
    end
  end
  def handle_info({:DOWN, _ref, :process, pid, _info}, %State{activate_worker: pid} = state) do
    {:noreply, start_timer(%State{state | activate_worker: nil})}
  end
  def handle_info({:DOWN, _ref, :process, pid, _info}, %State{deactivate_worker: pid} = state) do
    {:noreply, %State{state | deactivate_worker: nil}}
  end
  def handle_info({:DOWN, _ref, :process, pid, _info}, %State{adjust_worker: pid} = state) do
    {:noreply, %State{state | adjust_worker: nil}}
  end
  def handle_info({:purge_node, node}, state) do
    if State.phase(state) in [:active, :activating] do
      %{state_name: state_name, members: members} = RaftedValue.status(Cluster)
      if state_name == :leader do
        RaftedValue.command(Cluster, {:remove_node, node})
        target_pid = Enum.find(members, fn pid -> node(pid) == node end)
        RaftedValue.remove_follower(Cluster, target_pid)
      end
      {:noreply, %State{state | purge_wait_timer: nil}}
    else
      {:noreply, state}
    end
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | adjust_timer: Process.send_after(self, :adjust_members, Config.balancing_interval)}
  end
  defp stop_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | adjust_timer: nil}
  end

  defp spawn_link_monitor(mod, fun, args) do
    {pid, _} = Process.spawn(mod, fun, args, [:link, :monitor])
    pid
  end

  defun node_purge_candidate_changed(node_to_purge :: node) :: :ok do
    GenServer.cast(__MODULE__, {:node_purge_candidate_changed, node_to_purge})
  end

  defun start_consensus_group_members(name :: atom, rv_config :: RaftedValue.Config.t, member_nodes :: [node]) :: :ok do
    GenServer.cast(__MODULE__, {:start_consensus_group_members, name, rv_config, member_nodes})
  end

  defun start_consensus_group_follower(name :: atom, node :: node) :: :ok do
    GenServer.cast({__MODULE__, node}, {:start_consensus_group_follower, name})
  end
end
