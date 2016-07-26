defmodule RaftFleetTest do
  use ExUnit.Case
  @moduletag timeout: 200_000

  import SlaveNode
  alias RaftFleet.ConsensusMemberSup

  setup_all do
    Node.start(:"1", :shortnames) # Note that epmd is not automatically started by just executing `mix test`
    :ok
  end

  setup do
    # For clean testing we restart :raft_fleet
    :ok = Application.stop(:raft_fleet)
    :ok = Application.start(:raft_fleet)
  end

  @n_consensus_groups 100
  @rv_config          RaftedValue.make_config(RaftFleet.JustAnInt)

  defp start_consensus_group(name) do
    assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
    assert RaftFleet.add_consensus_group(name, 3, @rv_config) == {:error, :already_added}
    :timer.sleep(10)
    spawn_link(fn -> client_process_loop(name, 0) end)
  end

  defp client_process_loop(name, n) do
    :timer.sleep(:rand.uniform(1_000))
    assert RaftFleet.command(name, :inc) == {:ok, n}
    client_process_loop(name, n + 1)
  end

  defp assert_members_well_distributed(n_groups) do
    {:ok, {participating_nodes, _, _}} = RaftFleet.query(RaftFleet.Cluster, {:consensus_groups, Node.self})
    {members, leaders} =
      Enum.map(participating_nodes, fn n ->
        children = Supervisor.which_children({ConsensusMemberSup, n}) # should not exit; all participating nodes should be alive
        statuses = Enum.map(children, fn {_, pid, _, _} -> RaftedValue.status(pid) end)
        assert Enum.all?(statuses, &Enum.empty?(&1[:unresponsive_followers]))
        n_leaders = Enum.count(statuses, &(&1[:state_name] == :leader))
        {length(children), n_leaders}
      end)
      |> Enum.unzip
    expected_total = min(3, length(participating_nodes)) * n_groups
    assert_flat_distribution(members, expected_total)
    assert_flat_distribution(leaders, n_groups)

    ([Node.self | Node.list] -- participating_nodes)
    |> Enum.each(fn n ->
      assert Supervisor.which_children({ConsensusMemberSup, n}) == []
    end)
  end

  defp assert_flat_distribution(list, total) do
    assert Enum.sum(list) == total
    average = div(total, length(list))
    assert Enum.min(list) >= div(average, 4)
    assert Enum.max(list) <= average * 4
  end

  defp with_consensus_groups_and_their_clients(f) do
    assert_members_well_distributed(0) # with no consensus group there should be no RaftedValue.Server process
    assert RaftFleet.command(:nonexisting_consensus_group, :inc) == {:error, :no_leader}
    assert RaftFleet.query(:nonexisting_consensus_group, :get)   == {:error, :no_leader}

    # add consensus groups (at first only leaders are spawned)
    consensus_names = Enum.map(1 .. @n_consensus_groups, fn i -> :"consensus_group#{i}" end)
    client_pids     = Enum.map(consensus_names, &start_consensus_group/1)

    # follower processes should automatically be spawned afterwards
    :timer.sleep(10_000)
    assert_members_well_distributed(@n_consensus_groups)

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
    :timer.sleep(3_000)
    assert_members_well_distributed(0)
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
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl
    test "startup/shutdown of statically defined nodes: #{n_nodes} node(s) in #{n_zones} zone(s)" do
      run_basic_setup_test(unquote(slaves), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_basic_setup_test(node_names, zone_fun) do
    with_slaves(node_names, fn ->
      with_active_nodes([Node.self | Node.list], zone_fun, fn ->
        with_consensus_groups_and_their_clients(fn ->
          :ok
        end)
      end)
    end)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 2 end) # at least 2 nodes are necessary
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves1 = Enum.map(          1 ..     n_nodes, fn i -> :"#{i}" end) |> tl
    slaves2 = Enum.map(n_nodes + 1 .. 2 * n_nodes, fn i -> :"#{i}" end)
    test "dynamically adding/removing node should invoke rebalancing of consensus members: #{n_nodes} => #{2 * n_nodes} => #{n_nodes} in #{n_zones} zone(s)" do
      run_node_addition_and_removal_test(unquote(slaves1), unquote(slaves2), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_node_addition_and_removal_test(node_names1, node_names2, zone_fun) do
    Enum.each(node_names1, &start_slave/1)
    nodes1 = [Node.self | Node.list]
    Enum.each(nodes1, &activate_node(&1, zone_fun))

    with_consensus_groups_and_their_clients(fn ->
      Enum.each(node_names2, &start_slave/1)
      nodes2 = [Node.self | Node.list] -- nodes1
      Enum.each(nodes2, &activate_node(&1, zone_fun))

      # after several adjustments consensus members should be re-distributed
      :timer.sleep(15_000)
      assert_members_well_distributed(@n_consensus_groups)

      # deactivate/remove nodes one by one; clients should be able to interact with consensus leaders
      Enum.each(nodes1, fn n ->
        deactivate_node(n)
        :timer.sleep(5_000)
        assert_members_well_distributed(@n_consensus_groups)
      end)
    end)

    Enum.each(Node.list -- nodes1, &deactivate_node/1)
    Enum.each(node_names1 ++ node_names2, &stop_slave/1)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 3 end) # at least 2 nodes are necessary after 1 node failure
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl
    slave_that_fails = List.last(slaves)
    test "node failure should invoke purging of the node and rebalancing of consensus members: #{n_nodes} nodes in #{n_zones} zone(s)" do
      run_node_failure_test(unquote(slaves), unquote(slave_that_fails), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_node_failure_test(node_names, node_to_fail, zone_fun) do
    Enum.each(node_names, &start_slave/1)
    nodes = [Node.self | Node.list]
    Enum.each(nodes, &activate_node(&1, zone_fun))

    with_consensus_groups_and_their_clients(fn ->
      stop_slave(node_to_fail)
      :timer.sleep(15_000)
      assert_members_well_distributed(@n_consensus_groups)

      status = RaftedValue.status(RaftFleet.Cluster)
      assert status[:state_name] == :leader
      assert length(status[:members]) == length(Node.list) + 1
      assert Enum.all?(status[:members], fn pid -> node(pid) != node_to_fail end)
    end)

    Enum.each([Node.self | Node.list], &deactivate_node/1)
    Enum.each(node_names -- [node_to_fail], &stop_slave/1)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 3 end) # at least 2 nodes are necessary after 1 node failure
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl
    test "should automatically recover from accidental death of a cluster consensus member: #{n_nodes} nodes in #{n_zones} zone(s)" do
      run_cluster_consensus_member_failure_test(unquote(slaves), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_cluster_consensus_member_failure_test(node_names, zone_fun) do
    with_slaves(node_names, fn ->
      all_nodes = [Node.self | Node.list]
      with_active_nodes(all_nodes, zone_fun, fn ->
        Enum.each(all_nodes, fn n ->
          assert Process.whereis(RaftFleet.Cluster) |> at(n)
        end)

        :timer.sleep(1_000)
        target_pid = Process.whereis(RaftFleet.Cluster) |> at(Enum.random(all_nodes))
        Process.exit(target_pid, :kill)
        :timer.sleep(5_000)

        statuses =
          Enum.map(all_nodes, fn n ->
            RaftedValue.status({RaftFleet.Cluster, n})
          end)
        status_of_leader = Enum.find(statuses, &match?(%{state_name: :leader}, &1))
        assert status_of_leader[:unresponsive_followers] == []
      end)
    end)
  end

  test "active_nodes/0, consensus_groups/0 and whereis_leader/1" do
    # before activate/1
    catch_error RaftFleet.active_nodes
    catch_error RaftFleet.consensus_groups
    assert      RaftFleet.whereis_leader(:consensus1) == nil

    with_slaves([:"2", :"3"], fn ->
      with_active_nodes([Node.self | Node.list], &zone(&1, 2), fn ->
        nodes = RaftFleet.active_nodes
        assert Map.keys(nodes) |> Enum.sort == ["zone0", "zone1"]
        assert Map.values(nodes) |> List.flatten |> Enum.sort == Enum.sort([Node.self | Node.list])

        assert RaftFleet.consensus_groups                                == %{}
        assert RaftFleet.add_consensus_group(:consensus1, 3, @rv_config) == :ok
        assert RaftFleet.add_consensus_group(:consensus2, 3, @rv_config) == :ok
        assert RaftFleet.add_consensus_group(:consensus3, 3, @rv_config) == :ok
        assert RaftFleet.consensus_groups                                == %{consensus1: 3, consensus2: 3, consensus3: 3}
        assert RaftFleet.remove_consensus_group(:consensus1)             == :ok
        assert RaftFleet.remove_consensus_group(:consensus2)             == :ok
        assert RaftFleet.remove_consensus_group(:consensus3)             == :ok
        assert RaftFleet.consensus_groups                                == %{}

        assert is_pid(RaftFleet.whereis_leader(:consensus1))
        assert is_pid(RaftFleet.whereis_leader(:consensus2))
        assert is_pid(RaftFleet.whereis_leader(:consensus3))
      end)
    end)
  end
end
