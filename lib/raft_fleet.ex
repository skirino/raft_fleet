use Croma

defmodule RaftFleet do
  use Application
  alias Supervisor.Spec
  alias RaftedValue.Data
  alias RaftFleet.{Manager, LeaderPidCache, ZoneId}

  def start(_type, _args) do
    LeaderPidCache.init
    children = [
      Spec.worker(Manager, []),
      Spec.supervisor(RaftFleet.MemberSup, []),
    ]
    opts = [strategy: :one_for_one, name: RaftFleet.Supervisor]
    Supervisor.start_link(children, opts)
  end

  #
  # Public API
  #
  defun command(name        :: g[atom],
                command_arg :: Data.command_arg) :: {:ok, Data.command_ret} | {:error, :no_leader} do
    ref = make_ref
    call_with_retry(name, 3, fn pid ->
      RaftedValue.command(pid, command_arg, 500, ref)
    end)
  end

  defun query(name      :: g[atom],
              query_arg :: Data.query_arg) :: {:ok, Data.query_ret} | {:error, :no_leader} do
    call_with_retry(name, 3, fn pid ->
      RaftedValue.query(pid, query_arg, 500)
    end)
  end

  defp call_with_retry(name, tries, f) do
    if tries == 0 do
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
        :timer.sleep(500)
        call_with_retry(name, tries - 1, f)
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
              LeaderPidCache.set(name, nil)
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

  defun activate(zone :: ZoneId.t) :: :ok | {:error, :activated} do
    GenServer.call(Manager, {:activate, zone})
  end

  defun deactivate :: :ok | {:error, :inactive} do
    GenServer.call(Manager, :deactivate)
  end

  defun add_consensus_group(group_name :: g[atom],
                            n_replica  :: g[pos_integer],
                            config     :: RaftedValue.Config.t) :: :ok | {:error, :already_added} do
    {:ok, ret} = RaftFleet.command(RaftFleet.Cluster, {:add_group, group_name, n_replica, config})
    ret
  end

  defun remove_consensus_group(group_name :: g[atom]) :: :ok | {:error, :not_found} do
    {:ok, ret} = RaftFleet.command(RaftFleet.Cluster, {:remove_group, group_name})
    ret
  end
end
