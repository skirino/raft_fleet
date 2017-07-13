use Croma

defmodule RaftFleet.Util do
  alias RaftFleet.LeaderPidCache

  def find_leader_and_cache(name) do
    case find_leader(name) do
      nil -> nil
      pid ->
        LeaderPidCache.set(name, pid)
        pid
    end
  end

  defp find_leader(name) do
    [Node.self() | Node.list()]
    |> Enum.map(fn node -> try_status({name, node}) end)
    |> Enum.filter(&match?(%{leader: p} when is_pid(p), &1))
    |> case do
      [] -> nil
      ss -> Enum.max_by(ss, &(&1.current_term)).leader
    end
  end

  def try_status(server) do
    try do
      RaftedValue.status(server)
    catch
      :exit, _ -> nil
    end
  end
end
