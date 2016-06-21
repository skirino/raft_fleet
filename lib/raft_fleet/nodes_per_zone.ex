use Croma
alias Croma.TypeGen, as: TG

defmodule RaftFleet.NodesPerZone do
  alias RaftFleet.{Hash, ZoneId}
  alias RaftFleet.ConsensusGroupName, as: CGName
  use Croma.SubtypeOfMap, key_module: ZoneId, value_module: TG.list_of(Croma.Atom)

  defun lrw_members(nodes_per_zone :: t, group :: CGName.t, n :: pos_integer) :: [node] do
    if map_size(nodes_per_zone) == 0 do
      []
    else
      lrw_count_per_zone(Map.keys(nodes_per_zone), group, n)
      |> Enum.flat_map(fn {zone, n_in_zone} ->
        lrw_nodes_in_zone(nodes_per_zone[zone], group, n_in_zone)
      end)
    end
  end

  defunp lrw_count_per_zone(zones :: [ZoneId.t], group :: CGName.t, n :: pos_integer) :: [{ZoneId.t, pos_integer}] do
    sorted = Enum.sort_by(zones, fn node -> Hash.calc({node, group}) end)
    len = length(zones)
    if n <= len do
      Enum.take(sorted, n) |> Enum.map(&{&1, 1})
    else
      n_div = div(n, len)
      n_rem = rem(n, len)
      {pairs, _} =
        Enum.reduce(sorted, {[], 0}, fn(node, {acc, i}) ->
          count = if i < n_rem, do: n_div + 1, else: n_div
          {[{node, count} | acc], i + 1}
        end)
      Enum.reverse(pairs)
    end
  end

  defunp lrw_nodes_in_zone(nodes :: [node], group :: CGName.t, n :: pos_integer) :: [node] do
    Enum.sort_by(nodes, fn node -> Hash.calc({node, group}) end)
    |> Enum.take(n)
  end
end
