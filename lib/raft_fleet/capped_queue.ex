use Croma

defmodule RaftFleet.CappedQueue do
  use Croma.SubtypeOfTuple, elem_modules: [Croma.NonNegInteger, Croma.NonNegInteger, Croma.Tuple]

  defun new(max_length :: g[non_neg_integer]) :: t do
    {max_length, 0, :queue.new}
  end

  defun enqueue({max, len, q} :: t, v :: any) :: t do
    q2 = :queue.in(v, q)
    if len < max do
      {max, len + 1, q2}
    else
      {_, q3} = :queue.out(q2)
      {max, len, q3}
    end
  end

  defun filter({max, len, {l1, l2}} :: t, f :: (any -> boolean)) :: t do
    {r1, n1} = filter_count(l1, f)
    {r2, n2} = filter_count(l2, f)
    q = {[], Enum.reverse(r2, r1)}
    {max, len - n1 - n2, q}
  end

  defunp filter_count(l :: [any], f :: (any -> boolean)) :: {[any], non_neg_integer} do
    Enum.reduce(l, {[], 0}, fn(e, {reversed, count}) ->
      case f.(e) do
        true  -> {[e | reversed], count}
        false -> {reversed, count + 1}
      end
    end)
  end

  defun underlying_queue({_, _, q} :: t) :: :queue.queue, do: q
end
