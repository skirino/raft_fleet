use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.Hash do
  @pow_2_32 4294967296
  use Croma.SubtypeOfInt, min: 0, max: @pow_2_32 - 1

  defun calc(value :: term) :: t do
    :erlang.phash2(value, @pow_2_32)
  end
end

defmodule RaftFleet.ZoneId do
  @type t :: any
  def valid?(_), do: true
end

defmodule RaftFleet.ConsensusGroups do
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: Croma.PosInteger
end

defmodule RaftFleet.ConsensusNodesPair do
  use Croma.SubtypeOfTuple, elem_modules: [Croma.Atom, TG.list_of(Croma.Atom)]
end

defmodule RaftFleet.MembersPerLeaderNode do
  alias RaftFleet.ConsensusNodesPair
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: TG.list_of(ConsensusNodesPair)
end

defmodule RaftFleet.UnhealthyMembersCounts do
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: Croma.NonNegInteger
end

defmodule RaftFleet.UnhealthyMembersCountsMap do
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: RaftFleet.UnhealthyMembersCounts

  defun remove_node(umcm :: t, n :: node) :: t do
    Map.delete(umcm, n)
    |> Map.new(fn {node, counts} -> {node, Map.delete(counts, n)} end)
  end

  defun most_unhealthy_node(m :: t, threshold :: non_neg_integer) :: nil | node do
    Map.values(m)
    |> Enum.reduce(%{}, fn(map, acc) ->
      Map.merge(map, acc, fn(_k, v1, v2) -> v1 + v2 end)
    end)
    |> Enum.filter(fn {_node, count} -> threshold < count end)
    |> Enum.sort_by(fn {node, count} -> {count, node} end, &>=/2) # use order of `node` to break ties
    |> List.first()
    |> case do
      {node, _count} -> node
      nil            -> nil
    end
  end
end

if Mix.env() == :test do
  # To run code within slave nodes during tests, all modules must be compiled into beam files (i.e. they can't load .exs files)
  defmodule RaftFleet.JustAnInt do
    @behaviour RaftedValue.Data
    def new(), do: 0
    def command(i, {:set, j}), do: {i, j    }
    def command(i, :inc     ), do: {i, i + 1}
    def query(i, :get), do: i
  end
end
