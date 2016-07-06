use Croma

defmodule RaftFleet.ConsensusMemberSup do
  defun start_link :: {:ok, pid} do
    spec = Supervisor.Spec.worker(RaftedValue, [], [restart: :temporary])
    Supervisor.start_link([spec], [strategy: :simple_one_for_one, name: __MODULE__])
  end
end
