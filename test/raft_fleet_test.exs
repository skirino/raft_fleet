defmodule RaftFleetTest do
  use TestCaseTemplate
  @moduletag timeout: 200_000

  import SlaveNode
  alias RaftFleet.ConsensusMemberSup

  @n_consensus_groups 60
  @rv_config_options  [
    election_timeout: 2000, # In travis disk I/O is sometimes rather slow, resulting in more frequent leader elections
  ]
  @rv_config RaftedValue.make_config(RaftFleet.JustAnInt, @rv_config_options)

  test "add_consensus_group/3 right after remove_consensus_group/1 should return {:error, :process_exists} and should not do nothing" do
    with_active_nodes([Node.self()], &zone(&1, 1), fn ->
      Enum.each(1 .. 10, fn i ->
        name = :"consensus#{i}"
        assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
        assert RaftFleet.consensus_groups()                       == %{name => 3}
        assert RaftFleet.remove_consensus_group(name)             == :ok
        assert RaftFleet.consensus_groups()                       == %{}
        case RaftFleet.add_consensus_group(name, 3, @rv_config) do
          :ok -> # on rare occasion member process is terminated by adjuster process
            assert RaftFleet.consensus_groups()           == %{name => 3}
            assert RaftFleet.remove_consensus_group(name) == :ok
            assert RaftFleet.consensus_groups()           == %{}
          {:error, :process_exists} -> # matches this pattern most of the time
            assert RaftFleet.consensus_groups() == %{}
        end
      end)
    end)
  end

  defp start_consensus_group(name) do
    assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
    assert RaftFleet.add_consensus_group(name, 3, @rv_config) == {:error, :already_added}
    :timer.sleep(100)
    spawn_link(fn -> client_process_loop(name, 0) end)
  end

  defp client_process_loop(name, n) do
    :timer.sleep(:rand.uniform(1_000))
    assert RaftFleet.command(name, :inc, 500, 5, 1500) == {:ok, n} # increase number of retries, to make tests more robust (especially for travis)
    client_process_loop(name, n + 1)
  end

  defp assert_members_well_distributed(n_groups) do
    {:ok, {participating_nodes, _, _}} = RaftFleet.query(RaftFleet.Cluster, {:consensus_groups, Node.self()})
    {members, leaders} =
      Enum.map(participating_nodes, fn n ->
        children = Supervisor.which_children({ConsensusMemberSup, n}) # should not exit; all participating nodes should be alive
        statuses = Enum.map(children, fn {_, pid, _, _} -> RaftedValue.status(pid) end)
        Enum.each(statuses, fn s ->
          assert s.unresponsive_followers == []
        end)
        n_leaders = Enum.count(statuses, &(&1.state_name == :leader))
        {length(children), n_leaders}
      end)
      |> Enum.unzip()
    expected_total = min(3, length(participating_nodes)) * n_groups
    assert_flat_distribution(members, expected_total)
    assert_flat_distribution(leaders, n_groups)

    ([Node.self() | Node.list()] -- participating_nodes)
    |> Enum.each(fn n ->
      assert Supervisor.which_children({ConsensusMemberSup, n}) == []
    end)
  end

  defp assert_flat_distribution(list, total) do
    assert Enum.sum(list) == total
    average = div(total, length(list))
    assert Enum.min(list) >= div(average, 8)
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
    :timer.sleep(30_000)
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
    :timer.sleep(5_000)
    assert_members_well_distributed(0)
  end

  cluster_node_zone_configurations = [
    {1, 1},
    {3, 1},
    {4, 2},
    {3, 3},
    {6, 3},
  ]

  Enum.each(cluster_node_zone_configurations, fn {n_nodes, n_zones} ->
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl()
    test "startup/shutdown of statically defined nodes: #{n_nodes} node(s) in #{n_zones} zone(s)" do
      run_basic_setup_test(unquote(slaves), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_basic_setup_test(node_names, zone_fun) do
    with_slaves(node_names, fn ->
      with_active_nodes([Node.self() | Node.list()], zone_fun, fn ->
        with_consensus_groups_and_their_clients(fn ->
          :ok
        end)
      end)
    end)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 2 end) # at least 2 nodes are necessary
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves1 = Enum.map(          1 ..     n_nodes, fn i -> :"#{i}" end) |> tl()
    slaves2 = Enum.map(n_nodes + 1 .. 2 * n_nodes, fn i -> :"#{i}" end)
    test "dynamically adding/removing node should invoke rebalancing of consensus members: #{n_nodes} => #{2 * n_nodes} => #{n_nodes} in #{n_zones} zone(s)" do
      run_node_addition_and_removal_test(unquote(slaves1), unquote(slaves2), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_node_addition_and_removal_test(node_names1, node_names2, zone_fun) do
    Enum.each(node_names1, &start_slave/1)
    nodes1 = [Node.self() | Node.list()]
    Enum.each(nodes1, &activate_node(&1, zone_fun))

    with_consensus_groups_and_their_clients(fn ->
      Enum.each(node_names2, &start_slave/1)
      nodes2 = [Node.self() | Node.list()] -- nodes1
      Enum.each(nodes2, &activate_node(&1, zone_fun))

      # after several adjustments consensus members should be re-distributed
      :timer.sleep(30_000)
      assert_members_well_distributed(@n_consensus_groups)

      # deactivate/remove nodes one by one; clients should be able to interact with consensus leaders
      Enum.each(nodes1, fn n ->
        deactivate_node(n)
        :timer.sleep(8_000)
        assert_members_well_distributed(@n_consensus_groups)
      end)
    end)

    Enum.each(Node.list() -- nodes1, &deactivate_node/1)
    Enum.each(node_names1 ++ node_names2, &stop_slave/1)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 3 end) # at least 2 nodes are necessary after 1 node failure
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl()
    slave_that_fails = List.last(slaves)
    test "node failure should invoke purging of the node and rebalancing of consensus members: #{n_nodes} nodes in #{n_zones} zone(s)" do
      run_node_failure_test(unquote(slaves), unquote(slave_that_fails), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_node_failure_test(node_names, node_to_fail, zone_fun) do
    Enum.each(node_names, &start_slave/1)
    nodes = [Node.self() | Node.list()]
    Enum.each(nodes, &activate_node(&1, zone_fun))

    with_consensus_groups_and_their_clients(fn ->
      stop_slave(node_to_fail)
      :timer.sleep(30_000) # members in `node_to_fail` are recognized as unhealthy, `node_purge_failure_time_window` elapses, then rebalances
      assert_members_well_distributed(@n_consensus_groups)

      refute node_to_fail in Node.list()
      status = RaftedValue.status(RaftFleet.Cluster)
      assert length(status.members) == length(Node.list()) + 1
      assert Enum.all?(status.members, fn pid -> node(pid) != node_to_fail end)
    end)

    Enum.each([Node.self() | Node.list()], &deactivate_node/1)
    Enum.each(node_names -- [node_to_fail], &stop_slave/1)
  end

  Enum.filter(cluster_node_zone_configurations, fn {n_nodes, _} -> n_nodes >= 3 end) # at least 2 nodes are necessary after 1 node failure
  |> Enum.each(fn {n_nodes, n_zones} ->
    slaves = Enum.map(1 .. n_nodes, fn i -> :"#{i}" end) |> tl()
    test "should automatically recover from accidental death of a cluster consensus member: #{n_nodes} nodes in #{n_zones} zone(s)" do
      run_cluster_consensus_member_failure_test(unquote(slaves), &zone(&1, unquote(n_zones)))
    end
  end)

  defp run_cluster_consensus_member_failure_test(node_names, zone_fun) do
    with_slaves(node_names, fn ->
      all_nodes = [Node.self() | Node.list()]
      with_active_nodes(all_nodes, zone_fun, fn ->
        Enum.each(all_nodes, fn n ->
          assert Process.whereis(RaftFleet.Cluster) |> at(n)
        end)

        :timer.sleep(1_000)
        target_pid = Process.whereis(RaftFleet.Cluster) |> at(Enum.random(all_nodes))
        Process.exit(target_pid, :kill)
        :timer.sleep(8_000)

        statuses = Enum.map(all_nodes, &RaftedValue.status({RaftFleet.Cluster, &1}))
        status_of_leader = Enum.find(statuses, &match?(%{state_name: :leader}, &1))
        assert Enum.sort(Enum.map(status_of_leader.members, &node/1)) == Enum.sort(all_nodes)
        assert status_of_leader.unresponsive_followers                == []
      end)
    end)
  end

  test "active_nodes/0, consensus_groups/0 and whereis_leader/1" do
    # before activate/1
    catch_error RaftFleet.active_nodes()
    catch_error RaftFleet.consensus_groups()
    assert      RaftFleet.whereis_leader(:consensus1) == nil

    with_slaves([:"2", :"3"], fn ->
      with_active_nodes([Node.self() | Node.list()], &zone(&1, 2), fn ->
        nodes = RaftFleet.active_nodes()
        assert Map.keys(nodes) |> Enum.sort() == ["zone0", "zone1"]
        assert Map.values(nodes) |> List.flatten() |> Enum.sort() == Enum.sort([Node.self() | Node.list()])

        assert RaftFleet.consensus_groups()                              == %{}
        assert RaftFleet.add_consensus_group(:consensus1, 3, @rv_config) == :ok
        assert RaftFleet.add_consensus_group(:consensus2, 3, @rv_config) == :ok
        assert RaftFleet.add_consensus_group(:consensus3, 3, @rv_config) == :ok
        assert RaftFleet.consensus_groups()                              == %{consensus1: 3, consensus2: 3, consensus3: 3}

        :timer.sleep(100)
        assert is_pid(RaftFleet.whereis_leader(:consensus1))
        assert is_pid(RaftFleet.whereis_leader(:consensus2))
        assert is_pid(RaftFleet.whereis_leader(:consensus3))

        assert RaftFleet.remove_consensus_group(:consensus1)             == :ok
        assert RaftFleet.remove_consensus_group(:consensus2)             == :ok
        assert RaftFleet.remove_consensus_group(:consensus3)             == :ok
        assert RaftFleet.consensus_groups()                              == %{}
      end)
    end)
  end

  test "Manager.start_consensus_group_follower/4 should retry with removing failed follower" do
    with_slaves([:"2", :"3", :"4", :"5", :"6"], fn ->
      all_nodes = [Node.self() | Node.list()]
      with_active_nodes(all_nodes, &zone(&1, 2), fn ->
        name = :consensus1
        assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
        [leader_node | _] = expected_nodes = wait_until_members_fully_migrate(name)
        disable_periodic_adjuster_process(fn ->
          [target_node] = RaftFleet.NodesPerZone.lrw_members(RaftFleet.active_nodes(), name, 4) -- expected_nodes
          cause_error_in_newly_added_follower(fn ->
            RaftFleet.Manager.start_consensus_group_follower(name, target_node, leader_node)
            :timer.sleep(700) # should fail twice during this sleep
          end)
          _ = wait_until_members_fully_migrate(name, 4, 8)
        end)
      end)
    end)
  end

  defp disable_periodic_adjuster_process(f) do
    all_nodes = [Node.self() | Node.list()]
    Enum.each(all_nodes, fn n ->
      Application.put_env(:raft_fleet, :balancing_interval, 300_000) |> at(n)
    end)
    :timer.sleep(1000)
    f.()
    Enum.each(all_nodes, fn n ->
      Application.delete_env(:raft_fleet, :balancing_interval) |> at(n)
    end)
  end

  defp cause_error_in_newly_added_follower(f) do
    all_nodes = [Node.self() | Node.list()]
    Enum.each(all_nodes, fn n ->
      value = Enum.random([:raise, :timeout])
      Application.put_env(:raft_fleet, :rafted_value_test_inject_fault_after_add_follower, value) |> at(n)
    end)
    f.()
    Enum.each(all_nodes, fn n ->
      Application.delete_env(:raft_fleet, :rafted_value_test_inject_fault_after_add_follower) |> at(n)
    end)
  end

  test "state of a removed consensus group should be restored from latest snapshot/log files on restart" do
    slave_shortnames = Enum.map(2 .. 12, fn i -> :"#{i}" end)
    with_slaves(slave_shortnames, :yes, fn ->
      with_active_nodes([Node.self() | Node.list()], &zone(&1, 2), fn ->
        name = :consensus1
        assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
        [leader_node | _] = wait_until_members_fully_migrate(name)

        Enum.each(0 .. 9, fn i ->
          assert RaftFleet.command(name, :inc) == {:ok, i}
        end)
        pids = RaftedValue.status({name, leader_node}).members
        assert RaftFleet.remove_consensus_group(name) == :ok
        Enum.each(pids, &monitor_wait/1)

        assert RaftFleet.add_consensus_group(name, 3, @rv_config) == :ok
        _ = wait_until_members_fully_migrate(name)
        {:leader, state} = :sys.get_state({name, leader_node})
        assert state.data == 10 # The state should be correctly restored, regardless of whether Cluster leader node was included in the consensus or not
      end)
    end)
  end

  defp monitor_wait(pid) do
    ref = Process.monitor(pid)
    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end
  end

  defp wait_until_members_fully_migrate(name, n_members \\ 3, max_tries \\ 12) do
    expected_nodes = RaftFleet.NodesPerZone.lrw_members(RaftFleet.active_nodes(), name, n_members)
    wait_until_members_match(name, expected_nodes, max_tries, 0)
    expected_nodes
  end

  defp wait_until_members_match(name, expected_nodes, max_tries, tries) do
    if tries >= max_tries do
      raise "Consensus members do not converge! #{name}"
    else
      case members_match?(name, expected_nodes) do
        true  -> :ok
        false ->
          :timer.sleep(1000)
          wait_until_members_match(name, expected_nodes, max_tries, tries + 1)
      end
    end
  end

  defp members_match?(name, [leader_node | _] = expected_nodes) do
    try do
      %{state_name: state, members: ms, unresponsive_followers: unresponsive} = RaftedValue.status({name, leader_node})
      state == :leader and unresponsive == [] and MapSet.new(ms, &node/1) == MapSet.new(expected_nodes)
    catch
      :exit, _ -> false
    end
  end
end
