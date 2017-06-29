use Croma

defmodule RaftFleet.ConsensusMemberSup do
  alias RaftFleet.Config

  defmodule RaftedValueWrapper do
    defun start_link(group_info :: RaftedValue.consensus_group_info, name :: atom) :: GenServer.on_start do
      options =
        case Config.persistence_dir_parent() do
          nil -> [name: name]
          dir -> [name: name, persistence_dir: Path.join(dir, Atom.to_string(name))]
        end
      RaftedValue.start_link(group_info, options)
    end
  end

  defun start_link() :: {:ok, pid} do
    spec = Supervisor.Spec.worker(RaftedValueWrapper, [], [restart: :temporary])
    Supervisor.start_link([spec], [strategy: :simple_one_for_one, name: __MODULE__])
  end
end
