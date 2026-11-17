use Croma

defmodule RaftFleet.ProcessAndDiskLogIndexInspector do
  @moduledoc """
  A GenServer that reports "eligibility" of a node for hosting the 1st member of a new consensus group.

  When starting a new consensus group, a manager process collects reports from all active nodes and determine where to spawn the 1st member process.
  The report includes:
  - whether there exists a process whose registered name equals to the consensus group name
  - term and index of the last log entry in locally stored files (if any)

  The node whose log is the most up-to-date (compared by `{term, index}`, as in Raft's leader election) is chosen.
  The chosen log is guaranteed to contain all committed entries only if the reports include a majority of
  the members in the latest configuration of the consensus group.
  Note that this condition is not verified here (we don't know the latest members),
  i.e. restoring a consensus group from disk is done on a best-effort basis:
  nodes that don't respond in time or don't have log files are simply ignored.
  """

  use GenServer
  alias Croma.Result, as: R
  alias RaftedValue.{TermNumber, LogIndex}
  alias RaftFleet.PerMemberOptions

  defun start_link([]) :: {:ok, pid} do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  def init(:ok) do
    {:ok, nil}
  end

  def handle_call({:get_log_index, name}, _from, nil) do
    {:reply, make_reply(name), nil}
  end

  defunp make_reply(name :: atom) :: {:ok, nil | {TermNumber.t, LogIndex.t}} | {:error, :process_exists} do
    case Process.whereis(name) do
      nil ->
        case PerMemberOptions.build(name) |> Keyword.get(:persistence_dir) do
          nil -> {:ok, nil}
          dir -> {:ok, RaftedValue.read_last_log_term_and_index(dir)}
        end
      _pid -> {:error, :process_exists}
    end
  end

  defun find_node_having_latest_log_index(name :: atom) :: {:ok, nil | node} | {:error, :process_exists} do
    case RaftFleet.active_nodes() |> Enum.flat_map(fn {_, ns} -> ns end) do
      [n] when n -> {:ok, n}
      nodes      ->
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
  end

  defunp find_most_preferable_node(pairs :: [{node, LogIndex.t | {TermNumber.t, LogIndex.t}}]) :: nil | node do
    case pairs do
      [] -> nil
      _  ->
        # Nodes running older version of raft_fleet reply only with last log index (without term).
        # In that case we fall back to comparing indices only.
        if Enum.all?(pairs, &match?({_node, {_term, _index}}, &1)) do
          pairs
        else
          Enum.map(pairs, fn
            {n, {_term, i}} -> {n, i}
            {n, i}          -> {n, i} # Msg returned from an older raft_fleet. This clause will be removed in a future version
          end)
        end
        |> Enum.reduce(fn(pair1, pair2) ->
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
