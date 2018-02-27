use Croma

defmodule RaftFleet.PerMemberOptions do
  alias RaftFleet.Config

  defun build(name :: atom):: [RaftedValue.option] do
    case Config.per_member_options_maker() do
      nil -> []
      mod -> mod.make(name)
    end
    |> Keyword.put(:name, name)
  end
end

defmodule RaftFleet.PerMemberOptionsMaker do
  @callback make(consensus_group_name :: atom) :: [RaftedValue.option]
end
