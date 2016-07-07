use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Manager do
  use GenServer
  alias RaftFleet.{Cluster, ConsensusMemberSup, ConsensusMemberAdjuster, LeaderPidCache, Config}

  defmodule State do
    use Croma.Struct, fields: [
      timer:            TG.nilable(Croma.Reference),
      worker:           TG.nilable(Croma.Pid),
      purge_wait_timer: TG.nilable(Croma.Reference),
    ]
  end

  defun start_link :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    {:ok, %State{}}
  end

  def handle_call({:activate, zone}, _from, %State{timer: timer} = state) do
    if timer do
      {:reply, {:error, :activated}, state}
    else
      {:ok, pid} = Supervisor.start_child(RaftFleet.Supervisor, Cluster.Server.child_spec)
      {:ok, _} = RaftFleet.command(Cluster, {:add_node, Node.self, zone})
      {:reply, :ok, start_timer(state)}
    end
  end
  def handle_call(:deactivate, _from, %State{timer: timer} = state) do
    if timer do
      {:ok, _} = RaftFleet.command(Cluster, {:remove_node, Node.self})
      terminate_cluster_consensus_member
      {:reply, :ok, stop_timer(state)}
    else
      {:reply, {:error, :inactive}, state}
    end
  end
  def handle_call(msg, _from, state) do
    {:reply, msg, state}
  end

  def handle_cast({:node_purge_candidate_changed, node_to_purge}, %State{purge_wait_timer: ref1} = state) do
    if ref1, do: Process.cancel_timer(ref1)
    ref2 =
      if node_to_purge do
        Process.send_after(self, {:purge_node, node_to_purge}, Config.node_purge_failure_time_window)
      else
        nil
      end
    new_state = %State{state | purge_wait_timer: ref2}
    {:noreply, new_state}
  end
  def handle_cast({:start_consensus_group_members, name, rv_config, member_nodes}, state) do
    # Spawn leader in this node (neglecting desired leader node defined by randezvous hashing) to avoid potential failures
    {:ok, _} = Supervisor.start_child(ConsensusMemberSup, [{:create_new_consensus_group, rv_config}, name])
    List.delete(member_nodes, Node.self)
    |> Enum.each(fn n ->
      start_consensus_group_follower(name, n)
    end)
    {:noreply, state}
  end
  def handle_cast({:start_consensus_group_follower, name}, state) do
    other_node_members = Enum.map(Node.list, fn n -> {name, n} end)
    Supervisor.start_child(ConsensusMemberSup, [{:join_existing_consensus_group, other_node_members}, name])
    {:noreply, state}
  end
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  def handle_info(:adjust_members, %State{timer: timer, worker: worker} = state) do
    new_state =
      if worker do
        state # don't invoke multiple workers
      else
        {pid, _} = spawn_monitor(ConsensusMemberAdjuster, :adjust, [])
        %State{state | worker: pid}
      end
    if timer do
      {:noreply, start_timer(new_state)}
    else
      {:noreply, new_state}
    end
  end
  def handle_info({:DOWN, _ref, :process, _pid, _info}, state) do
    {:noreply, %State{state | worker: nil}}
  end
  def handle_info({:purge_node, node}, state) do
    %{state_name: state_name, members: members} = RaftedValue.status(Cluster)
    if state_name == :leader do
      RaftedValue.command(Cluster, {:remove_node, node})
      target_pid = Enum.find(members, fn pid -> node(pid) == node end)
      RaftedValue.remove_follower(Cluster, target_pid)
    end
    {:noreply, %State{state | purge_wait_timer: nil}}
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer(%State{timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | timer: Process.send_after(self, :adjust_members, Config.balancing_interval)}
  end
  defp stop_timer(%State{timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | timer: nil}
  end

  defp terminate_cluster_consensus_member do
    leader = LeaderPidCache.get(Cluster)
    if node(leader) == Node.self do
      status = RaftedValue.status(Cluster)
      case List.delete(status[:members], leader) do
        []      -> :ok
        members ->
          next_leader = Enum.random(members)
          :ok = RaftedValue.replace_leader(leader, next_leader)
          :timer.sleep(1_000)
          :ok = RaftedValue.remove_follower(next_leader, Process.whereis(Cluster))
      end
    else
      RaftedValue.remove_follower(leader, Process.whereis(Cluster))
    end
    :ok = Supervisor.terminate_child(RaftFleet.Supervisor, Cluster.Server)
    :ok = Supervisor.delete_child(RaftFleet.Supervisor, Cluster.Server)
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
