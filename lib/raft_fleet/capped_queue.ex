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

  defun underlying_queue({_, _, q} :: t) :: :queue.queue, do: q
end
