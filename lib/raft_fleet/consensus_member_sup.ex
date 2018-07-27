use Croma

defmodule RaftFleet.ConsensusMemberSup do
  alias RaftFleet.PerMemberOptions

  @behaviour :supervisor

  defmodule RaftedValueWrapper do
    defun start_link(group_info :: RaftedValue.consensus_group_info, name :: atom) :: GenServer.on_start do
      RaftedValue.start_link(group_info, PerMemberOptions.build(name))
    end
  end

  defun start_link() :: {:ok, pid} do
    :supervisor.start_link({:local, __MODULE__}, __MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    sup_flags = %{
      strategy:  :simple_one_for_one,
      intensity: 3,
      period:    5,
    }
    worker_spec = %{
      id:       RaftedValueWrapper,
      start:    {RaftedValueWrapper, :start_link, []},
      type:     :worker,
      restart:  :temporary,
      shutdown: 5000,
    }
    {:ok, {sup_flags, [worker_spec]}}
  end

  defun child_spec([]) :: Supervisor.child_spec do
    %{
      id:       __MODULE__,
      start:    {__MODULE__, :start_link, []},
      type:     :supervisor,
      restart:  :permanent,
      shutdown: :infinity,
    }
  end
end
