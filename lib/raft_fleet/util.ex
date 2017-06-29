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
    statuses =
      [Node.self() | Node.list()]
      |> Enum.map(fn node -> try_status({name, node}) end)
      |> Enum.reject(&is_nil/1)
    if Enum.empty?(statuses) do
      nil
    else
      max_term = Enum.map(statuses, &(&1[:current_term])) |> Enum.max()
      Enum.filter(statuses, &(&1[:current_term] == max_term))
      |> Enum.map(&(&1[:leader]))
      |> Enum.reject(&is_nil/1)
      |> List.first()
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
