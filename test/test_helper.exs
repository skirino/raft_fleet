ExUnit.start()

defmodule SlaveNode do
  defmacro at(call, nodename) do
    {{:., _, [mod, fun]}, _, args} = call
    quote bind_quoted: [nodename: nodename, mod: mod, fun: fun, args: args] do
      if nodename == Node.self do
        apply(mod, fun, args)
      else
        :rpc.call(nodename, mod, fun, args)
      end
    end
  end

  def with_slaves(shortnames, f) do
    try do
      Enum.each(shortnames, &start_slave/1)
      f.()
    after
      Enum.each(shortnames, &stop_slave/1)
    end
  end

  def start_slave(shortname) do
    {:ok, longname} = :ct_slave.start(shortname)
    true     = :code.set_path(:code.get_path)               |> at(longname)
    {:ok, _} = :application.ensure_all_started(:raft_fleet) |> at(longname)
    pid = Process.whereis(RaftFleet.Supervisor)             |> at(longname)
    :erlang.group_leader(:erlang.group_leader, pid)
    Supervisor.which_children(pid) |> Enum.each(fn {_, p, _, _} -> :erlang.group_leader(:erlang.group_leader, p) end)
  end

  def stop_slave(shortname) do
    {:ok, _} = :ct_slave.stop(shortname)
  end
end
