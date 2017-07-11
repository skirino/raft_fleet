use Croma

defmodule RaftFleet.ProcessAndDiskLogIndexInspector do
  @moduledoc """
  A GenServer that reports "eligibility" of a node for hosting the 1st member of a new consensus group.

  When starting a new consensus group, a manager process collects reports from all active nodes and determine where to spawn the 1st member process.
  The report includes:
  - whether there exists a process whose registered name equals to the consensus group name
  - last log index in locally stored files (if any)
  """

  use GenServer
  alias Croma.Result, as: R
  alias RaftedValue.LogIndex
  alias RaftFleet.Config

  defun start_link() :: {:ok, pid} do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    {:ok, nil}
  end

  def handle_call({:get_log_index, name}, _from, nil) do
    {:reply, make_reply(name), nil}
  end

  defunp make_reply(name :: atom) :: {:ok, nil | LogIndex.t} | {:error, :process_exists} do
    case Process.whereis(name) do
      nil ->
        case Config.persistence_dir_parent() do
          nil    -> {:ok, nil}
          parent ->
            dir = Path.join(parent, Atom.to_string(name))
            {:ok, RaftedValue.read_last_log_index(dir)}
        end
      _pid -> {:error, :process_exists}
    end
  end

  defun find_node_having_latest_log_index(name :: atom) :: {:ok, nil | node} | {:error, :process_exists} do
    nodes = RaftFleet.active_nodes() |> Enum.flat_map(fn {_, ns} -> ns end)
    {node_result_pairs, _bad_nodes} = GenServer.multi_call(nodes, __MODULE__, {:get_log_index, name}, 2000)
    if Enum.any?(node_result_pairs, &match?({_node, {:error, :process_exists}}, &1)) do
      {:error, :process_exists}
    else
      Enum.map(node_result_pairs, fn {n, {:ok, i}} -> {n, i} end)
      |> Enum.reject(&match?({_node, nil}, &1))
      |> find_most_preferable_node()
      |> R.pure()
    end
  end

  defunp find_most_preferable_node(pairs :: [{node, LogIndex.t}]) :: nil | node do
    case pairs do
      [] -> nil
      _  ->
        Enum.reduce(pairs, fn(pair1, pair2) ->
          if prefer_left?(pair1, pair2), do: pair1, else: pair2
        end)
        |> elem(0)
    end
  end

  defp prefer_left?({n1, i1}, {n2, i2}) do
    cond do
      i1 > i2           -> true
      i1 < i2           -> false
      n1 == Node.self() -> true  # prefer local node if available
      n2 == Node.self() -> false
      :otherwise        -> Enum.random([true, false])
    end
  end

  def handle_info(_msg, nil) do
    {:noreply, nil}
  end
end
