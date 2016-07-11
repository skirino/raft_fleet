ExUnit.start()

defmodule SlaveNode do
  import ExUnit.Assertions
  alias RaftFleet.{Manager, ConsensusMemberSup}

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
    nodes_before = Node.list
    {:ok, hostname} = :inet.gethostname
    {:ok, longname} = :slave.start(hostname, shortname)
    true     = :code.set_path(:code.get_path)               |> at(longname)
    {:ok, _} = :application.ensure_all_started(:raft_fleet) |> at(longname)
    Enum.each(nodes_before, fn n ->
      Node.connect(n) |> at(longname)
    end)
  end

  def stop_slave(shortname) do
    {:ok, hostname} = :inet.gethostname
    :ok = :slave.stop(:"#{shortname}@#{hostname}")
  end

  def activate_node(node, zone_fun) do
    assert Supervisor.which_children(ConsensusMemberSup) |> at(node) == []
    assert RaftFleet.deactivate                          |> at(node) == {:error, :inactive}
    assert RaftFleet.activate(zone_fun.(node))           |> at(node) == :ok
    assert RaftFleet.activate(zone_fun.(node))           |> at(node) == {:error, :not_inactive}
    wait_for_activation(node, 5)
  end

  defp wait_for_activation(_, 0), do: raise "activation not completed!"
  defp wait_for_activation(node, tries_remaining) do
    :timer.sleep(1_000)
    try do
      state = :sys.get_state({Manager, node})
      if Manager.State.phase(state) == :active do
        :ok
      else
        wait_for_activation(node, tries_remaining - 1)
      end
    catch
      :exit, {:noproc, _} -> wait_for_activation(node, tries_remaining - 1)
    end
  end

  def deactivate_node(node) do
    %{from: pid} = RaftedValue.status({RaftFleet.Cluster, node})
    assert Process.alive?(pid)  |> at(node)
    assert RaftFleet.deactivate |> at(node) == :ok
    assert RaftFleet.deactivate |> at(node) == {:error, :inactive}
    Process.monitor(pid)
    assert_receive({:DOWN, _monitor_ref, :process, ^pid, _reason}, 10_000)
  end

  def with_active_nodes(nodes, zone_fun, f) do
    try do
      Enum.shuffle(nodes) |> Enum.each(&activate_node(&1, zone_fun))
      f.()
    after
      Enum.shuffle(nodes) |> Enum.each(&deactivate_node/1)
      kill_all_consensus_members_in_local_node
    end
  end

  def kill_all_consensus_members_in_local_node do
    Supervisor.which_children(ConsensusMemberSup)
    |> Enum.each(fn {_, pid, _, _} -> :gen_fsm.stop(pid) end)
  end

  def zone(node, n) do
    i = Atom.to_string(node) |> String.split("@") |> hd |> String.to_integer |> rem(n)
    "zone#{i}"
  end
end
