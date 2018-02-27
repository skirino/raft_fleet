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
