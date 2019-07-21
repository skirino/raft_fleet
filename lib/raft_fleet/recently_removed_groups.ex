use Croma

defmodule RaftFleet.RecentlyRemovedGroups do
  alias RaftFleet.NodesPerZone

  defmodule NodesMap do
    defmodule Pair do
      use Croma.SubtypeOfTuple, elem_modules: [Croma.PosInteger, Croma.TypeGen.nilable(Croma.PosInteger)]
    end
    use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: Pair
  end

  defmodule IndexToGroupName do
    use Croma.SubtypeOfMap, key_module: Croma.PosInteger, value_module: Croma.Atom
  end

  defmodule GroupNameToIndices do
    use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: Croma.TypeGen.list_of(Croma.PosInteger)
  end

  use Croma.Struct, fields: [
    active_nodes:     NodesMap, # This field contains not only "currently active nodes" but also "nodes that have been active until recently"
    min_index:        Croma.TypeGen.nilable(Croma.PosInteger),
    max_index:        Croma.TypeGen.nilable(Croma.PosInteger),
    index_to_group:   IndexToGroupName,
    group_to_indices: GroupNameToIndices,
  ]

  defun empty() :: t do
    %__MODULE__{active_nodes: %{}, min_index: nil, max_index: nil, index_to_group: %{}, group_to_indices: %{}}
  end

  defun add(%__MODULE__{min_index: min, max_index: max, index_to_group: i2g, group_to_indices: g2is} = t, group :: atom) :: t do
    case {min, max} do
      {nil, nil} -> %__MODULE__{t | min_index: 1, max_index: 1, index_to_group: %{1 => group}, group_to_indices: %{group => [1]}}
      _          ->
        i = max + 1
        new_i2g  = Map.put(i2g, i, group)
        new_g2is = Map.update(g2is, group, [i], &[i | &1])
        %__MODULE__{t | max_index: i, index_to_group: new_i2g, group_to_indices: new_g2is}
    end
  end

  defun cleanup_ongoing?(%__MODULE__{min_index: min, group_to_indices: g2is}, group :: atom) :: boolean do
    Map.get(g2is, group, [])
    |> Enum.any?(fn i -> min < i end)
  end

  # `cancel/2` is not used anymore; just kept here for backward compatibility (i.e. for hot code upgrade).
  # Should be removed in a future release.
  defun cancel(%__MODULE__{index_to_group: i2g, group_to_indices: g2is} = t, group :: atom) :: t do
    {is, new_g2is} = Map.pop(g2is, group, [])
    new_i2g = Enum.reduce(is, i2g, fn(i, m) -> Map.delete(m, i) end)
    %__MODULE__{t | index_to_group: new_i2g, group_to_indices: new_g2is}
  end

  defun names_for_node(%__MODULE__{active_nodes: nodes, min_index: min, max_index: max, index_to_group: i2g}, node_from :: node) :: {[atom], nil | pos_integer} do
    case min do
      nil -> {[], nil}
      _   ->
        index_from =
          case Map.get(nodes, node_from) do
            nil         -> min
            {_t, nil  } -> min
            {_t, index} -> index + 1
          end
        if index_from <= max do
          names = Enum.map(index_from .. max, fn i -> Map.get(i2g, i) end) |> Enum.reject(&is_nil/1)
          {names, max}
        else
          {[], nil}
        end
    end
  end

  defun update(t            :: t,
               npz          :: NodesPerZone.t,
               node_from    :: node,
               index_or_nil :: nil | pos_integer,
               now          :: pos_integer,
               wait_time    :: pos_integer) :: t do
    t
    |> touch_currently_active_nodes(npz, now)
    |> forget_about_nodes_that_had_been_deactivated(now - wait_time)
    |> set_node_index(node_from, index_or_nil)
    |> proceed_min_index()
  end

  defp touch_currently_active_nodes(%__MODULE__{active_nodes: nodes} = t, npz, now) do
    currently_active_nodes = Enum.flat_map(npz, fn {_z, ns} -> ns end)
    new_nodes =
      Enum.reduce(currently_active_nodes, nodes, fn(n, ns) ->
        index_or_nil =
          case Map.get(ns, n) do
            nil            -> nil
            {_time, index} -> index
          end
        Map.put(ns, n, {now, index_or_nil})
      end)
    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp forget_about_nodes_that_had_been_deactivated(%__MODULE__{active_nodes: nodes} = t, threshold) do
    new_nodes = Enum.reject(nodes, fn {_n, {t, _i}} -> t < threshold end) |> Map.new()
    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp set_node_index(%__MODULE__{active_nodes: nodes} = t, node_from, index_or_nil) do
    new_nodes =
      case index_or_nil do
        nil                          -> nodes
        index when is_integer(index) -> update_node_index(nodes, node_from, index)
      end
    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp update_node_index(nodes, node_from, index) do
    case Map.get(nodes, node_from) do
      nil            -> nodes
      {time, _index} -> Map.put(nodes, node_from, {time, index})
    end
  end

  defp proceed_min_index(%__MODULE__{active_nodes: nodes, min_index: min, index_to_group: i2g0, group_to_indices: g2is0} = t) do
    smallest_node_index =
      Enum.map(nodes, fn {_n, {_t, index}} -> index || min end)
      |> Enum.min(fn -> min end)
    if min < smallest_node_index do
      {new_i2g, new_g2is} =
        Enum.reduce(min .. (smallest_node_index - 1), {i2g0, g2is0}, fn(i, {i2g, g2is}) ->
          case Map.pop(i2g, i) do
            {nil, _   } -> {i2g, g2is}
            {g  , i2g2} ->
              g2is2 =
                case Map.fetch!(g2is, g) |> List.delete(i) do
                  [] -> Map.delete(g2is, g)
                  is -> Map.put(g2is, g, is)
                end
              {i2g2, g2is2}
          end
        end)
      %__MODULE__{t | min_index: smallest_node_index, index_to_group: new_i2g, group_to_indices: new_g2is}
    else
      t
    end
  end
end
