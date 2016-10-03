use Croma

defmodule RaftFleet do
  @moduledoc """
  Public interface functions of `RaftFleet`.
  """

  use Application
  alias Supervisor.Spec
  alias RaftedValue.Data
  alias RaftFleet.{Cluster, Manager, LeaderPidCache, LeaderPidCacheRefresher, ZoneId, Util}

  def start(_type, _args) do
    LeaderPidCache.init
    children = [
      Spec.supervisor(RaftFleet.ConsensusMemberSup, []),
      Spec.worker(Manager, []),
      Spec.worker(LeaderPidCacheRefresher, []),
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
  defun deactivate :: :ok | {:error, :inactive} do
    GenServer.call(Manager, :deactivate)
  end

  @doc """
  Queries the current nodes which have been activated using `activate/1` in the cluster.

  This function sends a query to a leader of the "cluster consensus group", which is managed internally by raft_fleet.
  The returned value is grouped by zone IDs which have been passed to `activate/1`.
  This function exits if no active node exists in the cluster.
  """
  defun active_nodes :: %{ZoneId.t => [node]} do
    {:ok, ret} = RaftFleet.query(Cluster, :active_nodes)
    ret
  end

  @doc """
  Registers a new consensus group identified by `name`.

  `name` is used as a registered name for member processes of the new consensus group.
  `n_replica` is the number of replicas (Raft member processes implemented as `RaftedValue.Server`).
  For explanation of `rv_config` see `RaftedValue.make_config/2`.
  """
  defun add_consensus_group(name      :: g[atom],
                            n_replica :: g[pos_integer],
                            rv_config = %RaftedValue.Config{}) :: :ok | {:error, :already_added | :no_leader | any} do
    ref = make_ref
    call_result =
      call_with_retry(Cluster, @default_retry + 1, @default_retry_interval, fn pid ->
        leader_node = node(pid)
        command_arg = {:add_group, name, n_replica, rv_config, leader_node}
        case RaftedValue.command(pid, command_arg, @default_timeout, ref) do
          {:ok, r}        -> {:ok, {leader_node, r}}
          {:error, _} = e -> e
        end
      end)
    case call_result do
      {:ok, {leader_node, {:ok, _nodes}}} ->
        try do
          GenServer.call({Manager, leader_node}, {:await_completion_of_adding_consensus_group, name})
        catch
          :exit, reason -> # Something went wrong! Try to rollback the added consensus group
            remove_consensus_group(name)
            {:error, reason}
        end
      {:ok, {_leader_node, {:error, _reason} = e}} -> e
      {:error, :no_leader} = e                     -> e
    end
  end

  @doc """
  Removes an existing consensus group identified by `name`.

  Removing a consensus group will eventually trigger terminations of all members of the group.
  The replicated value held by the group will be discarded.

  Note that calling `add_consensus_group/3` right after `remove_consensus_group/1` with the same `name`
  may lead to confusing situation since `remove_consensus_group/1` don't immediately terminate existing member processes.
  """
  defun remove_consensus_group(name :: g[atom]) :: :ok | {:error, :not_found | :no_leader} do
    case command(Cluster, {:remove_group, name}) do
      {:ok, ret} -> ret
      error      -> error
    end
  end

  @doc """
  Queries already registered consensus groups.

  This function sends a query to a leader of the "cluster consensus group", which is managed internally by raft_fleet.
  The returned value is a map whose keys and values are consensus group name and number of replicas of the group.
  This function exits if no active node exists in the cluster.
  """
  defun consensus_groups :: %{atom => pos_integer} do
    {:ok, ret} = RaftFleet.query(Cluster, :consensus_groups)
    ret
  end

  @doc """
  Executes a command on the replicated value identified by `name`.

  The target consensus group identified by `name` must be registered beforehand using `add_consensus_group/3`.
  This function automatically resolves the leader process of the consensus group,
  caches PID of the current leader in local ETS table and send the given command to the leader.

  `timeout` is used in each synchronous messaging.
  In order to tolerate temporaral absences of leaders during Raft leader elections, it retries requests up to `retry`.
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
    ref = make_ref
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
  """
  defun whereis_leader(name :: g[atom]) :: nil | pid do
    case LeaderPidCache.get(name) do
      nil -> Util.find_leader_and_cache(name)
      pid -> pid
    end
  end
end
