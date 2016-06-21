use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Manager do
  use GenServer
  alias RaftFleet.{Cluster, MemberAdjuster, LeaderPidCache, Config}

  defmodule State do
    use Croma.Struct, fields: [
      timer:  TG.nilable(Croma.Reference),
      worker: TG.nilable(Croma.Pid),
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
      rv_config = Cluster.rv_config
      spec = Supervisor.Spec.worker(Cluster.Server, [rv_config, Cluster], [restart: :transient])
      {:ok, pid} = Supervisor.start_child(RaftFleet.Supervisor, spec)
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

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  def handle_info(:adjust_members, %State{timer: timer, worker: worker} = state) do
    new_state =
      if worker do
        state # don't invoke multiple workers
      else
        {pid, _} = spawn_monitor(MemberAdjuster, :adjust, [])
        %State{worker: pid}
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
          :timer.sleep(3000)
          :ok = RaftedValue.remove_follower(next_leader, Process.whereis(Cluster))
      end
    else
      RaftedValue.remove_follower(leader, Process.whereis(Cluster))
    end
    :ok = Supervisor.terminate_child(RaftFleet.Supervisor, Cluster.Server)
    :ok = Supervisor.delete_child(RaftFleet.Supervisor, Cluster.Server)
  end
end
