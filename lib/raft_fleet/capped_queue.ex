use Croma

defmodule RaftFleet.CappedQueue do
  @type t :: {non_neg_integer, non_neg_integer, :queue.queue}

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

  defun filter({max, _, q} :: t, f :: (any -> boolean)) :: t do
    q2   = :queue.filter(f, q)
    len2 = :queue.len(q2)
    {max, len2, q2}
  end

  defun underlying_queue({_, _, q} :: t) :: :queue.queue, do: q
end
