use Croma

defmodule RaftFleet.RaftedValueConfigMaker do
  alias RaftedValue.Config

  @callback make(consensus_group_name :: atom) :: Config.t
end
