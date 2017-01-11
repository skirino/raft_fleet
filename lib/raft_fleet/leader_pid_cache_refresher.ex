use Croma

defmodule RaftFleet.LeaderPidCacheRefresher do
  @moduledoc """
  A GenServer that periodically fetches all locally-stored leader pid caches and refresh stale ones.
  """

  use GenServer
  alias RaftFleet.{Config, LeaderPidCache, Util}

  defun start_link :: {:ok, pid} do
    GenServer.start_link(__MODULE__, :ok, [])
  end

  def init(:ok) do
    {:ok, nil, Config.leader_pid_cache_refresh_interval}
  end

  def handle_info(:timeout, nil) do
    refresh_all()
    {:noreply, nil, Config.leader_pid_cache_refresh_interval}
  end
  def handle_info(_delayed_reply, nil) do
    {:noreply, nil, Config.leader_pid_cache_refresh_interval}
  end

  defp refresh_all do
    cache_keys0 = LeaderPidCache.keys |> MapSet.new
    if MapSet.size(cache_keys0) == 0 do
      # Cache is not yet used (probably `RaftFleet.activate/1` is not called); nothing to do.
      :ok
    else
      nodes            = Node.list |> MapSet.new
      cache_keys       = MapSet.delete(cache_keys0, RaftFleet.Cluster)
      group_names      = RaftFleet.consensus_groups |> MapSet.new(fn {name, _} -> name end)
      common_names     = MapSet.intersection(cache_keys, group_names)
      cache_only_names = MapSet.difference(cache_keys, group_names)
      non_cached_names = MapSet.difference(group_names, cache_keys)
      Enum.each(common_names    , &confirm_leader_or_find(&1, nodes))
      Enum.each(cache_only_names, &LeaderPidCache.unset/1)
      Enum.each(non_cached_names, &Util.find_leader_and_cache/1)
    end
  end

  defp confirm_leader_or_find(name, nodes) do
    with pid when is_pid(pid)   <- LeaderPidCache.get(name),
         true                   <- MapSet.member?(nodes, node(pid)),
         %{state_name: :leader} <- Util.try_status(pid),
      do: :ok
    else
      _ -> Util.find_leader_and_cache(name)
  end
end
