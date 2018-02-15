use Croma

defmodule RaftFleet.ConsensusMemberSup do
  alias RaftFleet.PerMemberOptions

  defmodule RaftedValueWrapper do
    defun start_link(group_info :: RaftedValue.consensus_group_info, name :: atom) :: GenServer.on_start do
      RaftedValue.start_link(group_info, PerMemberOptions.build(name))
    end
  end

  defun start_link() :: {:ok, pid} do
    spec = Supervisor.Spec.worker(RaftedValueWrapper, [], [restart: :temporary])
    Supervisor.start_link([spec], [strategy: :simple_one_for_one, name: __MODULE__])
  end
end
