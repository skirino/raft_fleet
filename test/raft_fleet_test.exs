defmodule RaftFleetTest do
  use ExUnit.Case
  import SlaveNode
  alias RaftFleet.MemberSup

  setup_all do
    Node.start(:"1", :shortnames)
    :ok
  end

  defp zone(node, n) do
    i = Atom.to_string(node) |> String.split("@") |> hd |> String.to_integer |> rem(n)
    "zone#{i}"
  end

  defp activate_nodes(nodes, zone_fun, f) do
    try do
      Enum.shuffle(nodes)
      |> Enum.each(fn n ->
        :timer.sleep(100) # hack to avoid duplicated leaders
        assert Supervisor.which_children(MemberSup) |> at(n) == []
        assert RaftFleet.deactivate                 |> at(n) == {:error, :inactive}
        assert RaftFleet.activate(zone_fun.(n))     |> at(n) == :ok
        assert RaftFleet.activate(zone_fun.(n))     |> at(n) == {:error, :activated}
      end)
      f.()
    after
      Enum.shuffle(nodes)
      |> Enum.each(fn n ->
        %{from: pid} = RaftedValue.status({RaftFleet.Cluster, n})
        assert Process.alive?(pid)  |> at(n)
        assert RaftFleet.deactivate |> at(n) == :ok
        assert RaftFleet.deactivate |> at(n) == {:error, :inactive}
        :timer.sleep(2000)
        refute Process.alive?(pid)  |> at(n)
      end)

      # cleanup children in local node
      Supervisor.which_children(MemberSup)
      |> Enum.each(fn {_, pid, _, _} -> :gen_fsm.stop(pid) end)
    end
  end

  defp start_consensus_group(name) do
    conf = RaftedValue.make_config(RaftFleet.JustAnInt)
    {:ok, _} = RaftFleet.add_consensus_group(name, 3, conf)
    assert RaftFleet.add_consensus_group(name, 3, conf) == {:error, :already_added}
    :timer.sleep(10)
    spawn_link(fn -> client_process_loop(name, 0) end)
  end

  defp client_process_loop(name, n) do
    :timer.sleep(:rand.uniform(100))
    assert RaftFleet.command(name, :inc) == {:ok, n}
    client_process_loop(name, n + 1)
  end

  defp count_consensus_group_members(nodes) do
    Enum.map(nodes, fn n ->
      length(Supervisor.which_children({MemberSup, n}))
    end)
    |> Enum.sum
  end

  defp with_consensus_groups_and_their_clients(nodes, n_groups, f) do
    assert count_consensus_group_members(nodes) == 0 # with no consensus group there should be no RaftedValue.Server processes
    assert RaftFleet.command(:nonexisting_consensus_group, :inc) == {:error, :no_leader}
    assert RaftFleet.query(:nonexisting_consensus_group, :get)   == {:error, :no_leader}

    # add consensus groups (at first only leaders are spawned)
    consensus_names = Enum.map(1 .. n_groups, fn i -> :"consensus_group#{i}" end)
    client_pids     = Enum.map(consensus_names, &start_consensus_group/1)

    # follower processes should automatically be spawned afterwards
    :timer.sleep(2100)
    expected_n_processes = min(3, length(nodes)) * length(consensus_names)
    assert count_consensus_group_members(nodes) == expected_n_processes

    f.()

    # remove consensus groups
    Enum.each(client_pids, fn pid ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end)
    Enum.each(consensus_names, fn name ->
      assert RaftFleet.remove_consensus_group(name) == :ok
      assert RaftFleet.remove_consensus_group(name) == {:error, :not_found}
    end)

    # processes should be cleaned-up
    :timer.sleep(1100)
    assert count_consensus_group_members(nodes) == 0
  end

  defp run_basic_setup_test(node_names, zone_fun) do
    with_slaves(node_names, fn ->
      all_nodes = [Node.self | Node.list]
      activate_nodes(all_nodes, zone_fun, fn ->
        with_consensus_groups_and_their_clients(all_nodes, 100, fn ->
          :ok
        end)
      end)
    end)
  end

  cluster_node_zone_configurations = [
    {1, 1},
    {2, 1},
    {3, 1},
    {2, 2},
    {4, 2},
    {3, 3},
    {6, 3},
  ]

  Enum.each(cluster_node_zone_configurations, fn {n_nodes, n_zones} ->
    slave_shortnames = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl
    test "startup/shutdown of statically defined nodes: #{n_nodes} node in #{n_zones} zone" do
      run_basic_setup_test(unquote(slave_shortnames), &zone(&1, unquote(n_zones)))
    end
  end)
end
