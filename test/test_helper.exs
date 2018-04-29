ExUnit.start()

defmodule SlaveNode do
  import ExUnit.Assertions
  alias RaftFleet.{Manager, ConsensusMemberSup}

  defmacro at(call, nodename) do
    {{:., _, [mod, fun]}, _, args} = call
    quote bind_quoted: [nodename: nodename, mod: mod, fun: fun, args: args] do
      if nodename == Node.self() do
        apply(mod, fun, args)
      else
        :rpc.call(nodename, mod, fun, args)
      end
    end
  end

  def with_slaves(shortnames, persist \\ :random, f) do
    Enum.each(shortnames, &start_slave(&1, persist))
    ret = f.()
    Enum.each(shortnames, &stop_slave/1)
    ret
  end

  def start_slave(shortname, persist \\ :random) do
    nodes_before = Node.list()
    {:ok, hostname} = :inet.gethostname()
    {:ok, longname} = :slave.start_link(hostname, shortname)
    true            = :code.set_path(:code.get_path()) |> at(longname)
    case persist do
      :random -> PersistenceSetting.randomly_pick_whether_to_persist(longname)
      :yes    -> PersistenceSetting.turn_on_persistence(longname)
      :no     -> :ok
    end
    {:ok, _} = Application.ensure_all_started(:raft_fleet) |> at(longname)
    Enum.each(nodes_before, fn n ->
      Node.connect(n) |> at(longname)
    end)
  end

  def stop_slave(shortname) do
    :ok = :slave.stop(shortname_to_longname(shortname))
  end

  def shortname_to_longname(shortname) do
    {:ok, hostname} = :inet.gethostname()
    :"#{shortname}@#{hostname}"
  end

  def activate_node(node, zone_fun) do
    assert Supervisor.which_children(ConsensusMemberSup) |> at(node) == []
    assert RaftFleet.deactivate()                        |> at(node) == {:error, :inactive}
    assert RaftFleet.activate(zone_fun.(node))           |> at(node) == :ok
    assert RaftFleet.activate(zone_fun.(node))           |> at(node) == {:error, :not_inactive}
    wait_for_activation(node, 10)
  end

  defp wait_for_activation(_, 0), do: raise "activation not completed!"
  defp wait_for_activation(node, tries_remaining) do
    try do
      state = :sys.get_state({Manager, node})
      if Manager.State.phase(state) == :active do
        :ok
      else
        :timer.sleep(1_000)
        wait_for_activation(node, tries_remaining - 1)
      end
    catch
      :exit, {:noproc, _} ->
        :timer.sleep(1_000)
        wait_for_activation(node, tries_remaining - 1)
    end
  end

  def deactivate_node(node) do
    %{from: pid} =
      try do
        RaftedValue.status({RaftFleet.Cluster, node})
      catch
        :exit, _ ->
          # retry once again
          :timer.sleep(5_000)
          RaftedValue.status({RaftFleet.Cluster, node})
      end
    assert Process.alive?(pid)    |> at(node)
    assert RaftFleet.deactivate() |> at(node) == :ok
    assert RaftFleet.deactivate() |> at(node) == {:error, :inactive}
    ref = Process.monitor(pid)
    assert_receive({:DOWN, ^ref, :process, ^pid, _reason}, 15_000)

    if node == Node.self() do
      # Wait for worker process to exit (if any)
      state = :sys.get_state(Manager)
      [state.adjust_worker, state.activate_worker, state.deactivate_worker]
      |> Enum.reject(&is_nil/1)
      |> Enum.each(fn p ->
        r = Process.monitor(p)
        assert_receive({:DOWN, ^r, :process, ^p, _reason}, 15_000)
      end)
    end
  end

  def with_active_nodes(nodes, zone_fun, f) do
    Enum.shuffle(nodes) |> Enum.each(&activate_node(&1, zone_fun))
    f.()
    Enum.shuffle(nodes) |> Enum.each(&deactivate_node/1)
  end

  def zone(node, n) do
    i = Atom.to_string(node) |> String.split("@") |> hd() |> String.to_integer() |> rem(n)
    "zone#{i}"
  end
end

defmodule PersistenceSetting do
  import SlaveNode, only: [at: 2]

  def randomly_pick_whether_to_persist(longname \\ Node.self()) do
    case :rand.uniform(2) do
      1 -> :ok
      2 -> turn_on_persistence(longname)
    end
  end

  def turn_on_persistence(longname) do
    Application.put_env(:raft_fleet, :per_member_options_maker, RaftFleet.PerMemberOptionsMaker.Persist) |> at(longname)
  end
end

defmodule TestCaseTemplate do
  use ExUnit.CaseTemplate

  setup_all do
    Node.start(:"1", :shortnames)
    :ok
  end

  setup do
    # For clean testing we restart :raft_fleet
    case Application.stop(:raft_fleet) do
      :ok                                   -> :ok
      {:error, {:not_started, :raft_fleet}} -> :ok
    end
    PersistenceSetting.randomly_pick_whether_to_persist()
    File.rm_rf!("tmp")
    :ok = Application.start(:raft_fleet)
    on_exit(fn ->
      Application.delete_env(:raft_fleet, :per_member_options_maker)
      File.rm_rf!("tmp")
      :timer.sleep(1000) # try to avoid slave start failures in travis
    end)
  end
end
