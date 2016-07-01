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
  def validate(t), do: {:ok, t}
end

defmodule RaftFleet.ConsensusGroupName do
  @type t :: any
  def validate(t), do: {:ok, t}
end

defmodule RaftFleet.ConsensusGroups do
  alias RaftFleet.ConsensusGroupName
  use Croma.SubtypeOfMap, key_module: ConsensusGroupName, value_module: Croma.PosInteger
end

defmodule RaftFleet.ConsensusNodesPair do
  alias RaftFleet.ConsensusGroupName
  use Croma.SubtypeOfTuple, elem_modules: [ConsensusGroupName, TG.list_of(Croma.Atom)]
end

defmodule RaftFleet.MembersPerLeaderNode do
  alias RaftFleet.ConsensusNodesPair
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: TG.list_of(ConsensusNodesPair)
end

defmodule RaftFleet.UnhealthyRaftMembers do
  use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: RaftedValue.PidSet
end

defmodule RaftFleet.Config do
  @default_balancing_interval (if Mix.env == :test, do: 1_000, else: 300_000) # `Mix.env` must be evaluated during compilation
  defun balancing_interval :: pos_integer do
    Application.get_env(:raft_fleet, :balancing_interval, @default_balancing_interval)
  end

  defun node_purge_threshold_failing_members :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_threshold_failing_members, 3)
  end

  defun node_purge_failure_time_window :: pos_integer do
    Application.get_env(:raft_fleet, :node_purge_failure_time_window, 300_000)
  end
end

if Mix.env == :test do
  # To run code within ct_slave nodes, all modules must be compiled into beam files (i.e. they can't load .exs files)
  defmodule RaftFleet.JustAnInt do
    @behaviour RaftedValue.Data
    def new, do: 0
    def command(i, {:set, j}), do: {i, j    }
    def command(i, :inc     ), do: {i, i + 1}
    def query(i, :get), do: i
  end
end
