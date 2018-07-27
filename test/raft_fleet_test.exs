defmodule RaftFleetTest do
  use TestCaseTemplate
  @moduletag timeout: 300_000

  import SlaveNode
  alias RaftFleet.ConsensusMemberSup

  @n_consensus_groups 60
  @rv_config RaftedValue.make_config(RaftFleet.JustAnInt, [
    heartbeat_timeout: 500,
    election_timeout: 2500, # In travis disk I/O is sometimes rather slow, resulting in more frequent leader elections
  ])

  test "add_consensus_group/3 right after remove_consensus_group/1 should return {:error, :process_exists} and should not do anything" do
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

  test "repeatedly calling remove_consensus_group/1 should eventually remove processes for target consensus groups" do
    with_active_nodes([Node.self()], &zone(&1, 1), fn ->
      names = Enum.map(1 .. 300, fn i -> :"consensus#{i}" end)
      Enum.each(names, fn name ->
        :ok = RaftFleet.add_consensus_group(name, 3, @rv_config)
      end)
      Enum.each(names, fn name ->
        assert is_pid(Process.whereis(name))
      end)
      Enum.each(names, fn name ->
        :ok = RaftFleet.remove_consensus_group(name)
      end)
      # Wait for at least 1 interval of adjuster
      :timer.sleep(5000)
      Enum.each(names, fn name ->
        assert is_nil(Process.whereis(name))
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
    # assert Enum.min(list) >= div(average, 8) # for 12 nodes this assertion may fail; we skip this assertion
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
    Enum.each(consensus_names, &wait_until_members_fully_migrate/1)
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
    :timer.sleep(10_000)
    assert_members_well_distributed(0)
  end

  cluster_node_zone_configurations = [
#    {1, 1},
    {3, 3},
    {4, 2},
#    {6, 3},
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
          :timer.sleep(1_000)
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
        :timer.sleep(10_000)
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
    node_to_fail_longname = shortname_to_longname(node_to_fail)
    Enum.each(node_names, &start_slave/1)
    Enum.each([Node.self() | Node.list()], &activate_node(&1, zone_fun))

    with_consensus_groups_and_their_clients(fn ->
      stop_slave(node_to_fail)
      :timer.sleep(50_000) # `node_to_fail` is recognized as unhealthy, `node_purge_failure_time_window` elapses, then purge

      refute node_to_fail_longname in Node.list()
      active_nodes = RaftFleet.active_nodes() |> Enum.flat_map(fn {_z, ns} -> ns end)
      refute node_to_fail_longname in active_nodes
      status = RaftedValue.status(RaftFleet.Cluster)
      assert length(status.members) == length([Node.self() | Node.list()])
      assert Enum.all?(status.members, fn pid -> node(pid) != node_to_fail_longname end)

      :timer.sleep(20_000)
      assert_members_well_distributed(@n_consensus_groups)
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
        :timer.sleep(10_000)

        statuses = Enum.map(all_nodes, &RaftedValue.status({RaftFleet.Cluster, &1}))
        status_of_leader = Enum.find(statuses, &match?(%{state_name: :leader}, &1))
        assert Enum.sort(Enum.map(status_of_leader.members, &node/1)) == Enum.sort(all_nodes)
        assert status_of_leader.unresponsive_followers                == []
      end)
    end)
  end

  test "should purge nodes that failed right after it's activated" do
    state = :sys.get_state(RaftFleet.NodeReconnector)
    refute state.this_node_active?
    assert state.other_active_nodes      == []
    assert RaftFleet.unreachable_nodes() == %{}

    start_slave(:"2")
    [n2] = Node.list()
    start_slave(:"3")
    [n3] = Node.list() -- [n2]
    :ok = RaftFleet.activate("z")
    :timer.sleep(500)
    :ok = RaftFleet.activate("z") |> at(n2)
    :timer.sleep(500)
    :ok = RaftFleet.activate("z") |> at(n3)
    :timer.sleep(2000)
    active_nodes1 = RaftFleet.active_nodes() |> Enum.flat_map(fn {_z, ns} -> ns end)
    assert Node.self() in active_nodes1
    assert n2          in active_nodes1
    assert n3          in active_nodes1
    :timer.sleep(500)
    state = :sys.get_state(RaftFleet.NodeReconnector)
    assert state.this_node_active?
    assert n2 in state.other_active_nodes
    assert n3 in state.other_active_nodes
    :timer.sleep(8_000) # at least 1 timeout in NodeReconnector before stopping :"2"

    time_before_failure = System.system_time(:second)
    stop_slave(:"2")
    unreachable_since = wait_until_node_recognized_as_unreachable(n2, n3)
    assert unreachable_since >= time_before_failure
    assert unreachable_since <= System.system_time(:second)

    :timer.sleep(32_000) # wait for `node_purge_failure_time_window`
    active_nodes2 = RaftFleet.active_nodes() |> Enum.flat_map(fn {_z, ns} -> ns end)
    assert Node.self() in active_nodes2
    assert n3          in active_nodes2
    refute n2          in active_nodes2
    :timer.sleep(8_000) # at least 1 more timeout in NodeReconnector

    deactivate_node(n3)
    stop_slave(:"3")
    deactivate_node(Node.self())
    state = :sys.get_state(RaftFleet.NodeReconnector)
    refute state.this_node_active?
    assert RaftFleet.unreachable_nodes() == %{}
  end

  defp wait_until_node_recognized_as_unreachable(node2, node3, tries \\ 0) do
    if tries > 100 do
      raise "NodeReconnector couldn't detect failing node '#{node2}'!"
    else
      :timer.sleep(1_000)
      # We don't care whether Node.self() or node3 detects that node2 is failing.
      unreachable_nodes1 = RaftFleet.unreachable_nodes()
      unreachable_nodes3 = RaftFleet.unreachable_nodes() |> at(node3)
      case Map.merge(unreachable_nodes1, unreachable_nodes3) do
        %{^node2 => since} -> since
        _                  -> wait_until_node_recognized_as_unreachable(node2, node3, tries + 1)
      end
    end
  end

  test "find_consensus_group_with_no_established_leader/0 and remove_dead_pids_located_in_dead_node/1" do
    # Use 5-member consensus groups to cover all branches
    shortnames = [:"2", :"3", :"4", :"5"]
    node_self = Node.self()
    [node2, node3, node4, node5] = Enum.map(shortnames, &shortname_to_longname/1)
    Enum.each(shortnames, &start_slave/1)
    Enum.each([node_self | Node.list()], fn n ->
      activate_node(n, &zone(&1, 2))
      # Set longer interval to suppress automatic cleanup by NodeReconnector
      Application.put_env(:raft_fleet, :node_purge_reconnect_interval, 600_000) |> at(n)
    end)

    config = Map.put(@rv_config, :election_timeout, 1_000)
    assert RaftFleet.add_consensus_group(:consensus1, 5, config) == :ok
    wait_until_members_fully_migrate(:consensus1, 5)
    assert RaftFleet.find_consensus_group_with_no_established_leader() == :ok

    Enum.each(tl(shortnames), &stop_slave/1)
    assert Node.list() == [node2]

    # `RaftFleet.Cluster` is in trouble as only 2/5 are alive
    :timer.sleep(5_000)
    {RaftFleet.Cluster, pairs0} = RaftFleet.find_consensus_group_with_no_established_leader()
    assert Keyword.keys(pairs0)                                  == [node_self, node2]
    assert length(RaftedValue.status(RaftFleet.Cluster).members) == 5
    assert length(RaftedValue.status(:consensus1      ).members) == 5
    catch_error RaftFleet.remove_dead_pids_located_in_dead_node(node5)

    # `RaftFleet.Cluster` is still in trouble as only 2/4 are alive
    :timer.sleep(2_000)
    {RaftFleet.Cluster, pairs1} = RaftFleet.find_consensus_group_with_no_established_leader()
    assert Keyword.keys(pairs1)                                   == [node_self, node2]
    assert length(RaftedValue.status(RaftFleet.Cluster).members)  == 4
    assert length(RaftedValue.status(:consensus1      ).members)  == 5
    assert RaftFleet.remove_dead_pids_located_in_dead_node(node4) == :ok # this cleans up dead pids in both `RaftFleet.Cluster` and `:consensus1`

    # members in `:consensus1` still remember `node5`
    :timer.sleep(2_000)
    {:consensus1, pairs2} = RaftFleet.find_consensus_group_with_no_established_leader()
    assert Keyword.keys(pairs2)                                   == [node_self, node2]
    assert length(RaftedValue.status(RaftFleet.Cluster).members)  == 3
    assert length(RaftedValue.status(:consensus1      ).members)  == 4
    assert RaftFleet.remove_dead_pids_located_in_dead_node(node5) == :ok

    # members in both `RaftFleet.Cluster` and `:consensus1` still remember `node3`
    :timer.sleep(2_000)
    assert RaftFleet.find_consensus_group_with_no_established_leader() == :ok
    assert length(RaftedValue.status(RaftFleet.Cluster).members)       == 3
    assert length(RaftedValue.status(:consensus1      ).members)       == 3
    assert RaftFleet.remove_dead_pids_located_in_dead_node(node3)      == :ok # this cleans up dead pids in both `RaftFleet.Cluster` and `:consensus1`

    # now all is well, no dead pids are included
    :timer.sleep(2_000)
    assert RaftFleet.find_consensus_group_with_no_established_leader() == :ok
    assert length(RaftedValue.status(RaftFleet.Cluster).members)       == 2
    assert length(RaftedValue.status(:consensus1      ).members)       == 2
    deactivate_node(node_self)
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
      mod = Enum.random([RaftFleet.PerMemberOptionsMaker.Raise, RaftFleet.PerMemberOptionsMaker.Timeout])
      Application.put_env(:raft_fleet, :per_member_options_maker, mod) |> at(n)
    end)
    f.()
    Enum.each(all_nodes, fn n ->
      Application.delete_env(:raft_fleet, :per_member_options_maker) |> at(n)
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
