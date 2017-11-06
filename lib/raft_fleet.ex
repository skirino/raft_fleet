use Croma

defmodule RaftFleet do
  @moduledoc """
  Public interface functions of `raft_fleet`.

  See also `RaftFleet.Config` for available application configs.
  """

  use Application
  alias Supervisor.Spec
  alias RaftedValue.Data
  alias RaftFleet.{Cluster, Manager, LeaderPidCache, LeaderPidCacheRefresher, ProcessAndDiskLogIndexInspector, ZoneId, Util}

  @impl true
  def start(_type, _args) do
    LeaderPidCache.init()
    children = [
      Spec.supervisor(RaftFleet.ConsensusMemberSup, []),
      Spec.worker(Manager, []),
      Spec.worker(LeaderPidCacheRefresher, []),
      Spec.worker(ProcessAndDiskLogIndexInspector, []),
    ]
    opts = [strategy: :one_for_one, name: RaftFleet.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def stop(_state) do
    :ets.delete(:raft_fleet_leader_pid_cache)
  end

  @default_timeout        500
  @default_retry          3
  @default_retry_interval 1_000

  @doc """
  Activates `Node.self()`.

  When `:raft_fleet` is started as an OTP application, the node is not active;
  to host consensus group members each node must be explicitly activated.
  `zone` is an ID of data center zone which this node belongs to.
  `zone` is used to determine which nodes to replicate data:
  RaftFleet tries to place members of a consensus group across multiple zones for maximum availability.

  Node activation by calling this function should be done after the node is fully connected to the other existing nodes;
  otherwise there is a possibility (although it is small) that the cluster forms partitioned subset of active nodes.
  """
  defun activate(zone :: ZoneId.t) :: :ok | {:error, :not_inactive} do
    GenServer.call(Manager, {:activate, zone})
  end

  @doc """
  Deactivates `Node.self()`.

  Call this function before you remove an ErlangVM from your cluster.
  Note that calling this function does not immediately remove consensus member processes in this node;
  these processes will be gradually migrated to other nodes by periodic rebalancing.
  """
  defun deactivate() :: :ok | {:error, :inactive} do
    GenServer.call(Manager, :deactivate)
  end

  @doc """
  Queries the current nodes which have been activated using `activate/1` in the cluster.

  This function sends a query to a leader of the `RaftFleet.Cluster` consensus group, which is managed internally by raft_fleet.
  The returned value is grouped by zone IDs which have been passed to `activate/1`.
  This function exits if no active node exists in the cluster.
  """
  defun active_nodes() :: %{ZoneId.t => [node]} do
    {:ok, ret} = RaftFleet.query(Cluster, :active_nodes)
    ret
  end

  @doc """
  Registers a new consensus group identified by `name`.

  `name` is used as a registered name for member processes of the new consensus group.
  `n_replica` is the number of replicas (Raft member processes implemented as `RaftedValue.Server`).
  For explanation of `rv_config` see `RaftedValue.make_config/2`.

  If you configure `raft_fleet` to persist Raft logs & snapshots (see `:persistence_dir_parent` in `RaftFleet.Config`)
  and the consensus group with `name` had been removed by `remove_consensus_group/1`,
  then `add_consensus_group/3` will restore the state of the consensus group from the snapshot and log files.

  The caller is blocked until the newly spawned leader becomes ready.
  `await_timeout` specifies how many milliseconds to wait for the initialization.
  """
  defun add_consensus_group(name      :: g[atom],
                            n_replica :: g[pos_integer],
                            rv_config = %RaftedValue.Config{},
                            await_timeout :: g[pos_integer] \\ 5_000) :: :ok | {:error, :already_added | :no_leader | any} do
    ref = make_ref()
    call_with_retry(Cluster, @default_retry + 1, @default_retry_interval, fn pid ->
      leader_node = node(pid)
      command_arg = {:add_group, name, n_replica, rv_config, leader_node}
      case RaftedValue.command(pid, command_arg, @default_timeout, ref) do
        {:ok, r}        -> {:ok, {leader_node, r}}
        {:error, _} = e -> e
      end
    end)
    |> case do
      {:ok, {leader_node, {:ok, _nodes}}} ->
        try do
          msg = {:await_completion_of_adding_consensus_group, name}
          case GenServer.call({Manager, leader_node}, msg, await_timeout) do
            {:ok, :leader_started}                        -> :ok
            {:ok, {:leader_delegated_to, delegated_node}} ->
              {:ok, :leader_started} = GenServer.call({Manager, delegated_node}, msg, await_timeout)
              :ok
            {:error, :process_exists} ->
              remove_consensus_group(name)
              {:error, :process_exists}
          end
        catch
          :exit, reason -> {:error, reason}
        end
      {:ok, {_leader_node, {:error, _reason} = e}} -> e
      {:error, :no_leader} = e                     -> e
    end
  end

  @doc """
  Removes an existing consensus group identified by `name`.

  Removing a consensus group will eventually trigger terminations of all members of the group.
  The replicated value held by the group will be discarded.

  Note that `remove_consensus_group/1` does not immediately terminate existing member processes;
  they will be terminated afterward by background worker process (see also `:balancing_interval` in `RaftFleet.Config`).
  Note also that, if Raft logs and snapshots has been created (see `:persistence_dir_parent` in `RaftFleet.Config`),
  `remove_consensus_group/1` does not remove these files.
  """
  defun remove_consensus_group(name :: g[atom]) :: :ok | {:error, :not_found | :no_leader} do
    case command(Cluster, {:remove_group, name}) do
      {:ok, ret} -> ret
      error      -> error
    end
  end

  @doc """
  Queries already registered consensus groups.

  This function sends a query to a leader of the `RaftFleet.Cluster` consensus group, which is managed internally by raft_fleet.
  The returned value is a map whose keys and values are consensus group name and number of replicas of the group.
  This function exits if no active node exists in the cluster.
  """
  defun consensus_groups() :: %{atom => pos_integer} do
    {:ok, ret} = RaftFleet.query(Cluster, :consensus_groups)
    ret
  end

  @doc """
  Executes a command on the replicated value identified by `name`.

  The target consensus group identified by `name` must be registered beforehand using `add_consensus_group/3`.
  This function automatically resolves the leader process of the consensus group,
  caches PID of the current leader in local ETS table and send the given command to the leader.

  `timeout` is used in each synchronous messaging.
  In order to tolerate temporal absences of leaders during Raft leader elections, it retries requests up to `retry`.
  Before retrying requests this function sleeps for `retry_interval` milliseconds.
  Thus for worst case this function blocks the caller for `timeout * (retry + 1) + retry_interval * retry`.
  Note that for complete masking of leader elections `retry_interval * retry` must be sufficiently longer than
  the time scale for leader elections (`:election_timeout` in `RaftedValue.Config.t`).

  See also `RaftedValue.command/4`.
  """
  defun command(name           :: g[atom],
                command_arg    :: Data.command_arg,
                timeout        :: g[pos_integer]     \\ @default_timeout,
                retry          :: g[non_neg_integer] \\ @default_retry,
                retry_interval :: g[pos_integer]     \\ @default_retry_interval) :: {:ok, Data.command_ret} | {:error, :no_leader} do
    ref = make_ref()
    call_with_retry(name, retry + 1, retry_interval, fn pid ->
      RaftedValue.command(pid, command_arg, timeout, ref)
    end)
  end

  @doc """
  Executes a read-only query on the replicated value identified by `name`.

  See `command/5` for explanations of `name`, `timeout`, `retry` and `retry_interval`.
  See also `RaftedValue.query/3`.
  """
  defun query(name           :: g[atom],
              query_arg      :: Data.query_arg,
              timeout        :: g[pos_integer]     \\ @default_timeout,
              retry          :: g[non_neg_integer] \\ @default_retry,
              retry_interval :: g[pos_integer]     \\ @default_retry_interval) :: {:ok, Data.query_ret} | {:error, :no_leader} do
    call_with_retry(name, retry + 1, retry_interval, fn pid ->
      RaftedValue.query(pid, query_arg, timeout)
    end)
  end

  defp call_with_retry(name, tries_remaining, retry_interval, f) do
    if tries_remaining == 0 do
      {:error, :no_leader}
    else
      run_with_catch = fn pid ->
        try do
          f.(pid)
        catch
          :exit, _ -> {:error, :exit}
        end
      end
      retry = fn ->
        :timer.sleep(retry_interval)
        call_with_retry(name, tries_remaining - 1, retry_interval, f)
      end
      find_leader_and_exec = fn ->
        case Util.find_leader_and_cache(name) do
          nil        -> retry.()
          leader_pid ->
            case run_with_catch.(leader_pid) do
              {:ok, _} = ok -> ok
              {:error, _}   -> retry.()
            end
        end
      end

      case LeaderPidCache.get(name) do
        nil        -> find_leader_and_exec.()
        leader_pid ->
          case run_with_catch.(leader_pid) do
            {:ok, _} = ok -> ok
            {:error, _}   ->
              LeaderPidCache.unset(name)
              find_leader_and_exec.()
          end
      end
    end
  end

  @doc """
  Tries to find the current leader of the consensus group specified by `name`.

  Usually you don't have to use this function as `command/5` and `query/5` automatically resolves where the leader resides.
  This function is useful when you want to inspect status of a consensus group by using e.g. `RaftedValue.status/1`.
  """
  defun whereis_leader(name :: g[atom]) :: nil | pid do
    case LeaderPidCache.get(name) do
      nil -> Util.find_leader_and_cache(name)
      pid -> pid
    end
  end

  @doc """
  Inspects members of consensus groups and finds a group (if any) in which no leader exists.

  If one found, returns the name of the consensus group and also statuses (as maps) of existing members.
  If no consensus group is in trouble, returns `:ok`.

  Target consensus groups are:
  - `RaftFleet.Cluster`, which is a special consensus group that manages metadata for other consensus groups
  - all registered consensus groups (i.e., the ones returned by `RaftFleet.consensus_groups/0`)

  This function is primarily intended to be used within remote console.
  Use this function to detect problematic consensus group in your cluster.
  """
  defun find_consensus_group_with_no_established_leader() :: :ok | {group_name :: atom, [{node, map}]} do
    case inspect_statuses_of_consensus_group(RaftFleet.Cluster) do
      {:error, pairs} -> {RaftFleet.Cluster, pairs}
      {:ok, _}        ->
        RaftFleet.consensus_groups()
        |> Enum.find_value(:ok, fn {group, _n_desired_members} ->
          case inspect_statuses_of_consensus_group(group) do
            {:ok, _}        -> nil
            {:error, pairs} -> {group, pairs}
          end
        end)
    end
  end

  @doc """
  Removes member pids that reside in the specified dead node from all existing consensus groups.

  Target consensus groups are:
  - `RaftFleet.Cluster`, which is a special consensus group that manages metadata for other consensus groups
  - all registered consensus groups (i.e., the ones returned by `RaftFleet.consensus_groups/0`)

  If a target consensus group does not have an established leader, then this function
  tries to remove dead pids (if any) by using `RaftedValue.force_remove_member/2`.

  This function crashes if the `RaftFleet.Cluster` consensus group does not have a leader.
  Each of the effects of this function is idempotent; you can freely call this function multiple times in case of failure.

  This function is primarily intended to be used within remote console.
  Use this function to resolve issues when e.g. some node suddenly died without cleaning up itself.
  The caller must be sure that the `dead_node` has definitely died.
  """
  defun remove_dead_pids_located_in_dead_node(dead_node :: g[node]) :: :ok do
    remove_pid_located_in_dead_node_from_consensus_group(RaftFleet.Cluster, dead_node)
    {:ok, :ok} = RaftFleet.command(RaftFleet.Cluster, {:remove_node, dead_node})
    RaftFleet.consensus_groups()
    |> Enum.each(fn {group, _n_desired_members} ->
      remove_pid_located_in_dead_node_from_consensus_group(group, dead_node)
    end)
  end

  defunp remove_pid_located_in_dead_node_from_consensus_group(group :: atom, dead_node :: node) :: :ok do
    require Logger
    case inspect_statuses_of_consensus_group(group) do
      {:ok, {leader, pairs}} ->
        member_pids_belonging_to_dead_node(pairs, dead_node)
        |> Enum.each(fn dead_pid ->
          Logger.info("removing a member #{inspect(dead_pid)} which resides in #{dead_node}")
          RaftedValue.remove_follower(leader, dead_pid)
        end)
      {:error, pairs} ->
        # There's no leader in this case; we resort to `RaftedValue.force_remove_member/2`.
        responding_member_pids = Enum.map(pairs, fn {_, s} -> s.from end)
        member_pids_belonging_to_dead_node(pairs, dead_node)
        |> Enum.each(fn dead_pid ->
          Logger.info("forcibly removing a member #{inspect(dead_pid)} which resides in #{dead_node}")
          Enum.each(responding_member_pids, fn member_pid ->
            ret = RaftedValue.force_remove_member(member_pid, dead_pid)
            Logger.info("forcibly removing a member #{inspect(dead_pid)} which resides in #{dead_node} #{inspect(ret)}")
          end)
        end)
    end
  end

  defp member_pids_belonging_to_dead_node(pairs, dead_node) do
    Enum.flat_map(pairs, fn {_, s} -> s.members end)
    |> Enum.uniq()
    |> Enum.filter(&(node(&1) == dead_node))
  end

  defunp inspect_statuses_of_consensus_group(group :: atom) :: {:ok, {pid, [{node, map}]}} | {:error, [{node, map}]} do
    case Util.retrieve_member_statuses(group) do
      []    -> {:error, []}
      pairs ->
        {most_supported_leader, count} =
          Enum.reduce(pairs, %{}, fn({_, s}, m) ->
            Map.update(m, s.leader, 1, &(&1 + 1))
          end)
          |> Enum.max_by(fn {_, c} -> c end)
        if is_pid(most_supported_leader) and 2 * count > length(pairs) do
          {:ok, {most_supported_leader, pairs}} # majority of members agree on that leader
        else
          {:error, pairs}
        end
    end
  end
end
