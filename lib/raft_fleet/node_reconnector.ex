use Croma

defmodule RaftFleet.NodeReconnector do
  use GenServer
  alias RaftedValue.Monotonic
  alias RaftFleet.Config

  defmodule State do
    require Logger

    use Croma.Struct, fields: [
      this_node_active?:  Croma.Boolean,
      other_active_nodes: Croma.TypeGen.list_of(Croma.Atom),
      unhealthy_since:    Croma.Map, # %{node => Monotonic.t}
    ]

    defun refresh(state :: t) :: t do
      new_state = update_active_nodes(state) |> try_reconnect()
      purge_failing_nodes(new_state)
      new_state
    end

    defunp update_active_nodes(state :: t) :: t do
      try do
        nodes = RaftFleet.active_nodes() |> Enum.flat_map(fn {_zone, nodes} -> nodes end)
        %__MODULE__{state |
          this_node_active?:  Node.self() in nodes,
          other_active_nodes: List.delete(nodes, Node.self()),
        }
      catch
        _, _ -> state
      end
    end

    defunp try_reconnect(%__MODULE__{this_node_active?: active?, other_active_nodes: nodes, unhealthy_since: map1} = state :: t) :: t do
      if active? do
        map2 = Map.take(map1, nodes) # Remove inactive nodes from `map1`
        map3 =
          Enum.reduce(nodes, map2, fn(n, m) ->
            if Node.connect(n) do
              Map.delete(m, n)
            else
              Map.put_new(m, n, Monotonic.millis())
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
            Enum.shuffle(failing_nodes) # randomize order of nodes to repair (to avoid repeatedly failing to add the same node)
            |> Enum.each(fn n ->
              Logger.info("purge node #{n} as it has been disconnected for longer than #{window}ms")
              RaftFleet.remove_dead_pids_located_in_dead_node(n)
            end)
          end)
      end
    end
  end

  defun start_link() :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    start_timer()
    {:ok, %State{this_node_active?: false, other_active_nodes: [], unhealthy_since: %{}}}
  end

  def handle_info(:timeout, state) do
    start_timer()
    {:noreply, State.refresh(state)}
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, Config.node_purge_reconnect_interval())
  end
end
