use Croma

defmodule RaftFleet.Util do
  alias RaftFleet.LeaderPidCache

  defun find_leader_and_cache(name :: atom) :: nil | pid do
    case find_leader(name) do
      nil -> nil
      pid ->
        LeaderPidCache.set(name, pid)
        pid
    end
  end

  defunp find_leader(name :: atom) :: nil | pid do
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
      |> Map.delete(:config) # `:config` is not important and thus removed here
    catch
      :exit, _ -> nil
    end
  end

  def retrieve_member_statuses(group) do
    [Node.self() | Node.list()]
    |> Enum.map(fn n -> {n, try_status({group, n})} end)
    |> Enum.reject(&match?({_, nil}, &1))
  end
end
