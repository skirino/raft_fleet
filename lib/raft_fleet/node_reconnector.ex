use Croma

defmodule RaftFleet.NodeReconnector do
  use GenServer
  alias RaftedValue.Monotonic
  alias RaftFleet.{Config, NodesPerZone}

  defmodule State do
    require Logger

    use Croma.Struct, fields: [
      this_node_active?:  Croma.Boolean,
      other_active_nodes: Croma.TypeGen.list_of(Croma.Atom),
      unhealthy_since:    Croma.Map, # %{node => Monotonic.t}
    ]

    defun this_node_activated(state :: t) :: t do
      %__MODULE__{state | this_node_active?: true}
    end

    defun this_node_deactivated(state :: t) :: t do
      %__MODULE__{state | this_node_active?: false}
    end

    defun other_node_activated(%__MODULE__{other_active_nodes: nodes} = state :: t, node :: node) :: t do
      new_nodes = if node in nodes, do: nodes, else: [node | nodes]
      %__MODULE__{state | other_active_nodes: new_nodes}
    end

    defun update_active_nodes(state :: t, nodes_per_zone :: NodesPerZone.t) :: t do
      all_nodes = Enum.flat_map(nodes_per_zone, fn {_z, ns} -> ns end)
      %__MODULE__{state |
        this_node_active?:  Node.self() in all_nodes,
        other_active_nodes: List.delete(all_nodes, Node.self()),
      }
    end

    defun refresh(state :: t) :: t do
      new_state = try_reconnect(state)
      purge_failing_nodes(new_state)
      new_state
    end

    defunp try_reconnect(%__MODULE__{this_node_active?: active?, other_active_nodes: nodes, unhealthy_since: map1} = state :: t) :: t do
      if active? do
        map2 = Map.take(map1, nodes) # Remove inactive nodes from `map1`
        map3 =
          Enum.reduce(nodes, map2, fn(n, m) ->
            if Node.connect(n) do
              Map.delete(m, n)
            else
              Map.put_new_lazy(m, n, &Monotonic.millis/0)
            end
          end)
        %__MODULE__{state | unhealthy_since: map3}
      else
        state
      end
    end

    defunp purge_failing_nodes(%__MODULE__{unhealthy_since: map}) :: :ok do
      case map_size(map) do
        0 -> :ok
        _ ->
          window = Config.node_purge_failure_time_window()
          threshold_time = Monotonic.millis() - window
          failing_nodes = for {n, since} <- map, since < threshold_time, do: n
          spawn(fn ->
            Enum.shuffle(failing_nodes) # randomize order of nodes to repair (to avoid repeatedly failing to handle the same node)
            |> Enum.each(fn n ->
              Logger.info("purge node #{n} as it has been disconnected for longer than #{window}ms")
              RaftFleet.remove_dead_pids_located_in_dead_node(n)
            end)
          end)
      end
    end

    def unreachable_nodes(%__MODULE__{unhealthy_since: map}) do
      offset = System.time_offset(:milliseconds)
      Map.new(map, fn {n, monotonic} -> {n, div(offset + monotonic, 1000)} end)
    end
  end

  defun start_link() :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    start_timer()
    {:ok, %State{this_node_active?: false, other_active_nodes: [], unhealthy_since: %{}}}
  end

  def handle_call(:unreachable_nodes, _from, state) do
    {:reply, State.unreachable_nodes(state), state}
  end

  def handle_cast(:this_node_activated, state) do
    {:noreply, State.this_node_activated(state)}
  end
  def handle_cast(:this_node_deactivated, state) do
    {:noreply, State.this_node_deactivated(state)}
  end
  def handle_cast({:other_node_activated, node}, state) do
    {:noreply, State.other_node_activated(state, node)}
  end

  def handle_info(:timeout, state) do
    start_timer()
    spawn_monitor(&call_active_nodes/0) # Use temporary process in order to keep NodeReconnector responsive
    {:noreply, state}
  end
  def handle_info({:DOWN, _monitor_ref, :process, _pid, reason}, state) do
    new_state =
      case reason do
        {:shutdown, npz} when is_map(npz) -> State.update_active_nodes(state, npz)
        _error                            -> state
      end
      |> State.refresh()
    {:noreply, new_state}
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, Config.node_purge_reconnect_interval())
  end

  defp call_active_nodes() do
    result =
      try do
        RaftFleet.active_nodes()
      catch
        :error, _ -> :error
      end
    exit({:shutdown, result})
  end
end
