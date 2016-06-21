use Croma

defmodule RaftFleet.MemberSup do
  defun start_link :: {:ok, pid} do
    spec = Supervisor.Spec.worker(RaftedValue, [], [restart: :temporary])
    Supervisor.start_link([spec], [strategy: :simple_one_for_one, name: __MODULE__])
  end

  defun start_consensus_group_leader(name :: ConsensusGroupName.t, node :: node, rv_config :: RaftedValue.Config.t) :: Supervisor.on_start_child do
    Supervisor.start_child({__MODULE__, node}, [{:create_new_consensus_group, rv_config}, name])
  end

  defun start_consensus_group_follower(name :: ConsensusGroupName.t, node :: node) :: Supervisor.on_start_child do
    other_node_members =
      [Node.self | Node.list]
      |> List.delete(node)
      |> Enum.map(fn n -> {name, n} end)
    Supervisor.start_child({__MODULE__, node}, [{:join_existing_consensus_group, other_node_members}, name])
  end

  defun remove(name :: g[atom], node :: g[node]) :: :ok | {:error, :not_found} do
    try do
      status = RaftedValue.status({name, node})
      pid = status[:from]
      case status[:state_name] do
        :leader ->
          case List.delete(status[:members], pid) do
            []        -> :gen_fsm.stop(pid)
            followers ->
              new_leader = Enum.random(followers)
              :ok = RaftedValue.replace_leader(pid, new_leader)
              :timer.sleep(3000)
              :ok = RaftedValue.remove_follower(new_leader, pid)
          end
        _ ->
          :ok = RaftedValue.remove_follower(status[:leader], pid)
      end
    catch
      :exit, {:noproc, _} -> {:error, :not_found}
    end
  end
end
