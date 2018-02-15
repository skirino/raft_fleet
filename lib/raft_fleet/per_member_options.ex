use Croma

defmodule RaftFleet.PerMemberOptions do
  alias RaftFleet.Config

  defun build(name :: atom):: [RaftedValue.option] do
    case Config.per_member_options_maker() do
      nil -> build_default(name)
      mod -> mod.make(name)
    end
    |> Keyword.put(:name, name)
  end

  defp build_default(name) do
    case Config.persistence_dir_parent() do
      nil -> []
      dir -> [persistence_dir: Path.join(dir, Atom.to_string(name))] # This default is for backward compatibility; `:persistence_dir_parent` is deprecated.
    end
  end
end

defmodule RaftFleet.PerMemberOptionsMaker do
  @callback make(consensus_group_name :: atom) :: [RaftedValue.option]
end
