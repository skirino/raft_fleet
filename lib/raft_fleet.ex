use Croma

defmodule RaftFleet do
  @moduledoc """
  Public interface functions of `RaftFleet`.
  """

  use Application
  alias Supervisor.Spec
  alias RaftedValue.Data
  alias RaftFleet.{Manager, LeaderPidCache, ZoneId}

  def start(_type, _args) do
    LeaderPidCache.init
    children = [
      Spec.worker(Manager, []),
      Spec.supervisor(RaftFleet.ConsensusMemberSup, []),
    ]
    opts = [strategy: :one_for_one, name: RaftFleet.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Activates `Node.self()`.

  When `:raft_fleet` is started as an OTP application, the node is not active;
  to host consensus group members each node must be explicitly activated.
  `zone` is an ID of data center zone which this node belongs to.
  `zone` is used to determine which nodes to replicate data:
  RaftFleet tries to place members of a consensus group across multiple zones for maximum availability.
  """
  defun activate(zone :: ZoneId.t) :: :ok | {:error, :activated} do
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
  Registers a new consensus group identified by `name`.

  `n_replica` is the number of replicas (Raft member processes implemented as `RaftedValue.Server`).
  For explanation of `rv_config` see `RaftedValue.make_config/2`.
  """
  defun add_consensus_group(name      :: g[atom],
                            n_replica :: g[pos_integer],
                            rv_config = %RaftedValue.Config{}) :: :ok | {:error, :already_added} do
    {:ok, ret} = RaftFleet.command(RaftFleet.Cluster, {:add_group, name, n_replica, rv_config})
    ret
  end

  @doc """
  Removes an existing consensus group identified by `name`.

  Removing a consensus group will trigger terminations of all members of the group.
  The replicated value held by the group will be discarded.
  """
  defun remove_consensus_group(name :: g[atom]) :: :ok | {:error, :not_found} do
    {:ok, ret} = RaftFleet.command(RaftFleet.Cluster, {:remove_group, name})
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
                timeout        :: g[pos_integer]     \\ 1_000,
                retry          :: g[non_neg_integer] \\ 2,
                retry_interval :: g[pos_integer]     \\ 1_000) :: {:ok, Data.command_ret} | {:error, :no_leader} do
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
              timeout        :: g[pos_integer]     \\ 1_000,
              retry          :: g[non_neg_integer] \\ 2,
              retry_interval :: g[pos_integer]     \\ 1_000) :: {:ok, Data.query_ret} | {:error, :no_leader} do
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
        case find_leader(name) do
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

  defp find_leader(name) do
    statuses =
      [Node.self | Node.list] |> Enum.map(fn node ->
        try do
          RaftedValue.status({name, node})
        catch
          :exit, _ -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)
    if Enum.empty?(statuses) do
      nil
    else
      max_term = Enum.map(statuses, &(&1[:current_term])) |> Enum.max
      pid_or_nil =
        Enum.filter(statuses, &(&1[:current_term] == max_term))
        |> Enum.map(&(&1[:leader]))
        |> Enum.reject(&is_nil/1)
        |> List.first
      if pid_or_nil do
        LeaderPidCache.set(name, pid_or_nil)
      end
      pid_or_nil
    end
  end
end
