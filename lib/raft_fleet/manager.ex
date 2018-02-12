use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Manager do
  use GenServer
  require Logger
  alias RaftFleet.{ConsensusMemberSup, ConsensusMemberAdjuster, Activator, Deactivator, Config, ProcessAndDiskLogIndexInspector}

  defmodule State do
    @type consensus_group_progress :: :leader_started | {:leader_delegated_to, node} | :process_exists | GenServer.from

    use Croma.Struct, fields: [
      adjust_timer:                 TG.nilable(Croma.Reference),
      adjust_worker:                TG.nilable(Croma.Pid),
      activate_worker:              TG.nilable(Croma.Pid),
      deactivate_worker:            TG.nilable(Croma.Pid),
      being_added_consensus_groups: Croma.Map, # %{atom => consensus_group_progress}
    ]

    def phase(%__MODULE__{adjust_timer: t, activate_worker: a, deactivate_worker: d}) do
      cond do
        t    -> :active
        a    -> :activating
        d    -> :deactivating
        true -> :inactive
      end
    end

    defun update_being_added_consensus_groups(%__MODULE__{being_added_consensus_groups: gs} = s,
                                              name   :: atom,
                                              value2 :: consensus_group_progress) :: t do
      case gs[name] do
        nil    -> %__MODULE__{s | being_added_consensus_groups: Map.put(gs, name, value2)}
        value1 ->
          case {gen_server_from?(value1), gen_server_from?(value2)} do
            {true , false} -> GenServer.reply(value1, convert_to_reply(value2)); %__MODULE__{s | being_added_consensus_groups: Map.delete(gs, name)}
            {false, true } -> GenServer.reply(value2, convert_to_reply(value1)); %__MODULE__{s | being_added_consensus_groups: Map.delete(gs, name)}
            _              -> %__MODULE__{s | being_added_consensus_groups: Map.put(gs, name, value2)}
          end
      end
    end

    defp gen_server_from?({p, r}) when is_pid(p) and is_reference(r), do: true
    defp gen_server_from?(_), do: false

    defp convert_to_reply(:leader_started             ), do: {:ok, :leader_started}
    defp convert_to_reply({:leader_delegated_to, node}), do: {:ok, {:leader_delegated_to, node}}
    defp convert_to_reply(:process_exists             ), do: {:error, :process_exists}
  end

  defun start_link() :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    {:ok, %State{being_added_consensus_groups: %{}}}
  end

  def handle_call({:activate, zone}, _from, state) do
    if State.phase(state) == :inactive do
      new_state = %State{state | activate_worker: start_worker(Activator, :activate, [zone])}
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :not_inactive}, state}
    end
  end
  def handle_call(:deactivate, _from, state) do
    if State.phase(state) == :active do
      new_state = %State{state | deactivate_worker: start_worker(Deactivator, :deactivate, [])} |> stop_timer()
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :inactive}, state}
    end
  end
  def handle_call({:await_completion_of_adding_consensus_group, name}, from, state) do
    if State.phase(state) in [:active, :activating] do
      {:noreply, State.update_being_added_consensus_groups(state, name, from)}
    else
      {:reply, {:error, :inactive}, state}
    end
  end

  def handle_cast({:start_consensus_group_members, name, rv_config, member_nodes}, state) do
    new_state =
      if State.phase(state) in [:active, :activating] do
        case ProcessAndDiskLogIndexInspector.find_node_having_latest_log_index(name) do
          {:ok, node_or_nil} ->
            # If there's no desired node, spawn leader in this node (neglecting the leader node defined by rendezvous hashing) to avoid potential failures
            node_to_host_initial_leader = node_or_nil || Node.self()
            if node_to_host_initial_leader == Node.self() do
              start_leader_and_tell_other_nodes_to_start_follower(name, rv_config, member_nodes, state)
            else
              start_consensus_group_members({__MODULE__, node_to_host_initial_leader}, name, rv_config, member_nodes)
              State.update_being_added_consensus_groups(state, name, {:leader_delegated_to, node_to_host_initial_leader})
            end
          {:error, :process_exists} ->
            State.update_being_added_consensus_groups(state, name, :process_exists)
        end
      else
        Logger.info("manager process is not active; cannot start member processes for consensus group #{name}")
        state
      end
    {:noreply, new_state}
  end
  def handle_cast({:start_consensus_group_follower, name, leader_node_hint}, state) do
    if State.phase(state) == :active do
      other_node_list =
        case leader_node_hint do
          nil  -> Node.list()
          node -> [node | List.delete(Node.list(), node)] # reorder nodes so that the new follower can find leader immediately (most of the time)
        end
      other_node_members = Enum.map(other_node_list, fn n -> {name, n} end)
      # To avoid blocking the Manager process, we spawn a temporary process solely for `Supervisor.start_child/2`.
      spawn_link(fn -> start_follower_with_retry(other_node_members, name, 3) end)
    end
    {:noreply, state}
  end

  defp start_leader_and_tell_other_nodes_to_start_follower(name, rv_config, member_nodes, state) do
    additional_args = [{:create_new_consensus_group, rv_config}, name]
    case Supervisor.start_child(ConsensusMemberSup, additional_args) do
      {:ok, _pid} ->
        tell_other_nodes_to_start_followers_with_delay(name, member_nodes)
        State.update_being_added_consensus_groups(state, name, :leader_started)
      {:error, reason} ->
        Logger.info("error in starting 1st member process of consensus group #{name}: #{inspect(reason)}")
        state
    end
  end

  defp tell_other_nodes_to_start_followers_with_delay(name, member_nodes) do
    # Concurrently spawning multiple followers may lead to race conditions (adding a node can only be done one-by-one).
    # Although this race condition can be automatically resolved by retries and thus is harmless,
    # we should try to minimize amount of unnecessary error logs.
    List.delete(member_nodes, Node.self())
    |> Enum.with_index()
    |> Enum.each(fn {node, i} ->
      start_consensus_group_follower(name, node, Node.self(), i * 100)
    end)
  end

  defp start_follower_with_retry(_, name, 0) do
    Logger.error("give up adding follower for #{name} due to consecutive failures")
    {:error, :cannot_start_child}
  end
  defp start_follower_with_retry(other_node_members, name, tries_remaining) do
    case Supervisor.start_child(ConsensusMemberSup, [{:join_existing_consensus_group, other_node_members}, name]) do
      {:ok, pid}                        -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason}                  ->
        case extract_dead_follower_pid_from_reason(reason) do
          nil               -> :ok
          dead_follower_pid -> spawn(fn -> try_to_remove_dead_follower(name, dead_follower_pid) end)
        end
        :timer.sleep(500)
        start_follower_with_retry(other_node_members, name, tries_remaining - 1)
    end
  end

  defp extract_dead_follower_pid_from_reason(%RaftedValue.AddFollowerError{pid: pid}                            ), do: pid
  defp extract_dead_follower_pid_from_reason({:timeout, {_mod, _fun, [_server, {:add_follower, pid}, _timeout]}}), do: pid
  defp extract_dead_follower_pid_from_reason(_                                                                  ), do: nil

  defp try_to_remove_dead_follower(name, dead_follower_pid) do
    case RaftFleet.whereis_leader(name) do
      nil    -> :ok
      leader ->
        :timer.sleep(100) # reduce possibility of race condition: `:uncommitted_membership_change`
        RaftedValue.remove_follower(leader, dead_follower_pid)
    end
  end

  def handle_info(:adjust_members, %State{adjust_worker: worker} = state) do
    new_state =
      if worker do
        state # don't invoke multiple workers
      else
        %State{state | adjust_worker: start_worker(ConsensusMemberAdjuster, :adjust, [])}
      end
    if State.phase(state) == :active do
      {:noreply, start_timer(new_state)}
    else
      {:noreply, new_state}
    end
  end
  def handle_info({:DOWN, _ref, :process, pid, info}, %State{activate_worker: pid} = state) do
    log_abnormal_exit_reason(info, :activate)
    {:noreply, start_timer(%State{state | activate_worker: nil})}
  end
  def handle_info({:DOWN, _ref, :process, pid, info}, %State{deactivate_worker: pid} = state) do
    log_abnormal_exit_reason(info, :deactivate)
    {:noreply, %State{state | deactivate_worker: nil}}
  end
  def handle_info({:DOWN, _ref, :process, pid, info}, %State{adjust_worker: pid} = state) do
    log_abnormal_exit_reason(info, :adjust)
    {:noreply, %State{state | adjust_worker: nil}}
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | adjust_timer: Process.send_after(self(), :adjust_members, Config.balancing_interval())}
  end
  defp stop_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | adjust_timer: nil}
  end

  defp start_worker(mod, fun, args) do
    {pid, _} = spawn_monitor(mod, fun, args)
    pid
  end

  defp log_abnormal_exit_reason(:normal, _), do: :ok
  defp log_abnormal_exit_reason(reason, worker_type) do
    Logger.error("#{worker_type} worker died unexpectedly: #{inspect(reason)}")
  end

  defun start_consensus_group_members(server       :: GenServer.server \\ __MODULE__,
                                      name         :: atom,
                                      rv_config    :: RaftedValue.Config.t,
                                      member_nodes :: [node]) :: :ok do
    GenServer.cast(server, {:start_consensus_group_members, name, rv_config, member_nodes})
  end

  defun start_consensus_group_follower(name :: atom, node :: node, leader_node_hint :: nil | node, delay :: non_neg_integer \\ 0) :: :ok do
    send_fun = fn ->
      GenServer.cast({__MODULE__, node}, {:start_consensus_group_follower, name, leader_node_hint})
    end
    if delay == 0 do
      send_fun.()
    else
      spawn(fn ->
        :timer.sleep(delay)
        send_fun.()
      end)
    end
    :ok
  end
end
