use Croma

defmodule RaftFleet.RemoteMessageGateway do
  @moduledoc """
  Replacement of `:gen_fsm.send_event/2` and `:gen_fsm.reply/2` to be used as `:communication_module` by raft member processes.

  This is introduced in order
  - to work-around slow message passing to unreachable nodes using temporary processes, and
  - to catch exit due to nonexisting locally-registered name.
  In other words, it behaves like `:gen_server.cast/2`.
  """

  def send_event(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_event", event})
  end

  def reply({pid, tag}, reply) do
    do_send(pid, {tag, reply})
  end

  defp do_send(dest, msg) do
    try do
      case :erlang.send(dest, msg, [:noconnect]) do
        :noconnect ->
          spawn(:erlang, :send, [dest, msg])
          :ok
        _otherwise -> :ok
      end
    catch
      _, _ -> :ok
    end
  end
end
