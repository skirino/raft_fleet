defmodule RaftFleet.ConsensusMemberAdjusterTest do
  use TestCaseTemplate
  @moduletag timeout: 300_000

  import SlaveNode
  alias RaftFleet.{ConsensusMemberAdjuster, Manager, ConsensusMemberSup}

  @group_name :consensus_group
  @rv_config RaftedValue.make_config(RaftFleet.JustAnInt, [
    heartbeat_timeout: 500,
    election_timeout: 2500, # In travis disk I/O is sometimes rather slow, resulting in more frequent leader elections
  ])

  defp consensus_members() do
    [Node.self() | Node.list()]
    |> Enum.map(fn n ->
      Process.whereis(@group_name) |> at(n)
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp i2node(i) do
    s = Integer.to_string(i)
    [Node.self() | Node.list()]
    |> Enum.find(fn n ->
      Atom.to_string(n) |> String.starts_with?(s)
    end)
  end

  defp start_leader_and_followers(leader_node, follower_nodes, group_name \\ @group_name) do
    Manager.start_consensus_group_members(group_name, @rv_config, []) |> at(leader_node)
    Enum.each(follower_nodes, fn n ->
      :timer.sleep(1000)
      Manager.start_consensus_group_follower(group_name, n, leader_node)
    end)
    :timer.sleep(2000)
  end

  defp call_adjust_one_step(group_name \\ @group_name) do
    desired_member_nodes = Enum.map([1, 2, 3], &i2node/1)
    ConsensusMemberAdjuster.adjust_one_step([Node.self() | Node.list()], group_name, desired_member_nodes)
    :timer.sleep(2000)
  end

  defp kill_all_consensus_members() do
    [Node.self() | Node.list()]
    |> Enum.flat_map(fn n -> Supervisor.which_children({ConsensusMemberSup, n}) end)
    |> Enum.each(fn {_, pid, _, _} -> Process.exit(pid, :kill) end)
  end

  defp with_active_slaves(shortnames, f) do
    with_slaves(shortnames, fn ->
      with_active_nodes([Node.self() | Node.list()], &zone(&1, 1), fn ->
        f.()
      end)
    end)
  end

  test "adjust_one_step/3" do
    with_active_slaves([:"2", :"3", :"4"], fn ->
      [
        {[1]         , 2, 1},
        {[1, 2]      , 3, 1},
        {[1, 4]      , 3, 1},
        {[1, 2, 4]   , 4, 1},
        {[1, 2, 3, 4], 3, 1},
        {[1, 2, 3]   , 3, 1},
        {[2]         , 2, 2},
        {[2, 3]      , 3, 2},
        {[2, 3, 4]   , 4, 2},
        {[2, 3, 1]   , 3, 1},
        {[2, 3, 4, 1], 4, 1},
        {[4, 1, 2, 3], 4, 1},
      ]
      |> Enum.each(fn {[leader_node | follower_nodes], n_expected, expected_leader_node} ->
        start_leader_and_followers(i2node(leader_node), Enum.map(follower_nodes, &i2node/1))
        call_adjust_one_step()
        assert length(consensus_members()) == n_expected
        leader_status = RaftedValue.status({@group_name, i2node(expected_leader_node)})
        assert leader_status.state_name             == :leader
        assert length(leader_status.members)        == n_expected
        assert leader_status.unresponsive_followers == []

        kill_all_consensus_members()
        Path.wildcard("tmp/*/*")
        |> Enum.reject(&String.ends_with?(&1, Atom.to_string(RaftFleet.Cluster)))
        |> Enum.each(&File.rm_rf!/1)
      end)
    end)
  end

  test "adjust_one_step/3 should remove pid that is definitely dead" do
    with_active_slaves([:"2", :"3"], fn ->
      start_leader_and_followers(Node.self(), Node.list())

      %{members: members1} = RaftedValue.status(@group_name)
      assert length(members1) == 3
      target_node = hd(Node.list())
      target_pid  = Enum.find(members1, fn pid -> node(pid) == target_node end)
      Process.exit(target_pid, :kill)
      :timer.sleep(6_000) # wait until the killed process is recognized as "unresponsive" by the consensus leader

      call_adjust_one_step()
      %{members: members2} = RaftedValue.status(@group_name)
      assert members2 == List.delete(members1, target_pid)

      call_adjust_one_step()
      %{members: members3} = RaftedValue.status(@group_name)
      assert length(members3) == 3
      [new_pid] = members3 -- members2
      assert node(new_pid) == target_node

      kill_all_consensus_members()
    end)
  end

  test "adjust_one_step/3 should remove stale member in inactive and disconnected node (with an established leader)" do
    with_active_slaves([:"2", :"3"], fn ->
      nodes_2_3 = Node.list()
      n4 =
        with_slaves([:"4"], fn ->
          [n4] = Node.list() -- nodes_2_3
          activate_node(n4, &zone(&1, 1))
          start_leader_and_followers(Node.self(), Node.list())
          n4
        end)
      :timer.sleep(6_000) # wait until the killed process is recognized as "unresponsive" by the consensus leader

      %{leader: l, members: members1} = RaftedValue.status(@group_name)
      assert is_pid(l)
      assert length(members1) == 4
      call_adjust_one_step()
      %{leader: ^l, members: members2} = RaftedValue.status(@group_name)
      assert length(members2) == 3
      [p4] = members1 -- members2
      assert node(p4) == n4

      kill_all_consensus_members()
      RaftFleet.remove_dead_pids_located_in_dead_node(n4) # clean up `RaftFleet.Cluster` consensus group
    end)
  end

  test "adjust_one_step/3 should remove stale member in inactive and disconnected node (with no established leader)" do
    with_active_slaves([:"2"], fn ->
      [n2] = Node.list()
      nodes_3_4 =
        with_slaves([:"3", :"4"], fn ->
          ns34 = Node.list() -- [n2]
          Enum.each(ns34, fn n -> activate_node(n, &zone(&1, 1)) end)
          start_leader_and_followers(Node.self(), Node.list())
          ns34
        end)

      :timer.sleep(5_000) # wait until the leader steps down
      %{leader: nil, members: members1} = RaftedValue.status(@group_name)
      assert length(members1) == 4
      :timer.sleep(5_000) # wait until max election timeout elapses after the member in `n2` last received a heartbeat
      %{leader: nil, members: members2} = RaftedValue.status({@group_name, n2})
      assert length(members2) == 4
      call_adjust_one_step()
      :timer.sleep(4_000) # wait until a new leader is elected
      %{leader: l, members: members3} = RaftedValue.status(@group_name)
      assert is_pid(l)
      assert length(members3) == 3
      %{leader: ^l, members: members4} = RaftedValue.status({@group_name, n2})
      assert length(members4) == 3

      kill_all_consensus_members()
      Enum.each(nodes_3_4, fn n ->
        RaftFleet.remove_dead_pids_located_in_dead_node(n) # clean up `RaftFleet.Cluster` consensus group
      end)
    end)
  end

  test "adjust_one_step/3 should recover from majority failure by removing dead pid" do
    with_active_slaves([:"2", :"3"], fn ->
      start_leader_and_followers(Node.self(), Node.list())
      %{leader: l, members: members} = RaftedValue.status(@group_name)
      assert is_pid(l)
      assert node(l) == Node.self()
      Enum.each(members -- [l], fn m ->
        :ok = :gen_statem.stop(m)
      end)
      :timer.sleep(6_000) # wait until the remaining leader steps down

      %{leader: nil, members: ^members} = RaftedValue.status(@group_name)
      call_adjust_one_step()
      %{leader: nil, members: members2} = RaftedValue.status(@group_name)
      assert length(members2) == 2
      call_adjust_one_step()
      %{members: [^l]} = RaftedValue.status(@group_name)
      :timer.sleep(5_000) # wait for election timeout to check that the remaining process correctly becomes leader
      %{leader: ^l, members: [^l]} = RaftedValue.status(@group_name)

      kill_all_consensus_members()
    end)
  end

  test "adjust_one_step/3 should remove consensus group when all members are definitely dead" do
    with_active_slaves([:"2", :"3"], fn ->
      # To avoid complication due to periodic adjustment process, we don't add_consensus_group here;
      # to judge whether `:remove_group` is done, we look into Raft logs.
      raft_log_includes_removal_of_group? = fn ->
        {_, state} = :sys.get_state(RaftFleet.Cluster)
        Enum.any?(state.logs.map, &match?({i, {_term, i, :command, {_from, {:remove_group, @group_name}, _ref}}}, &1))
      end

      start_leader_and_followers(Node.self(), Node.list())
      Enum.each([Node.self() | Node.list()], fn n ->
        pid = Process.whereis(@group_name) |> at(n)
        Process.exit(pid, :kill)
      end)
      refute raft_log_includes_removal_of_group?.()
      call_adjust_one_step()
      :timer.sleep(2_000)
      assert raft_log_includes_removal_of_group?.()
    end)
  end

  test "adjust_one_step/3 should not start new follower in node with a dead member and instead remove the dead member" do
    with_active_slaves([:"2", :"3", :"4"], fn ->
      [
        {[1, 2, 3]   , 1, [1, 2]      , [] }, # `3` is definitely dead, remove
        {[1, 2, 4]   , 1, [1, 2, 3, 4], [4]}, # `3` is missing, add
        {[2, 1, 3]   , 2, [1, 2]      , [] }, # want to add `3` but there's dead pid, remove it first
        {[2, 1, 4]   , 2, [1, 2, 3, 4], [4]}, # `3` is missing, add
        {[1, 2, 3, 4], 1, [1, 2, 3]   , [] }, # `4` is definitely dead, remove
        {[1, 2, 4, 3], 1, [1, 2, 3]   , [3]}, # `4` is extra, remove
        {[2, 1, 3, 4], 1, [1, 2, 3, 4], [4]}, # nothing to add, replace leader to `1`
        {[2, 1, 4, 3], 2, [1, 2, 4]   , [] }, # want to add `3` but there's dead pid, remove it first
      ]
      |> Enum.each(fn {[leader_node | follower_nodes], expected_leader_node, expected_member_nodes, unresponsive_member_nodes} ->
        start_leader_and_followers(i2node(leader_node), Enum.map(follower_nodes, &i2node/1))
        follower_pid_to_kill = Process.whereis(@group_name) |> at(i2node(List.last(follower_nodes)))
        assert :gen_statem.stop(follower_pid_to_kill) == :ok
        :timer.sleep(6_000) # wait until the leader recognizes the killed pid as "unhealthy"

        status1 = RaftedValue.status({@group_name, i2node(leader_node)})
        assert status1.state_name                                == :leader
        assert Enum.map(status1.members, &node/1) |> Enum.sort() == Enum.map([leader_node | follower_nodes], &i2node/1) |> Enum.sort()
        assert status1.unresponsive_followers                    == [follower_pid_to_kill]
        call_adjust_one_step()
        status2 = RaftedValue.status({@group_name, i2node(expected_leader_node)})
        assert status2.state_name                                               == :leader
        assert Enum.map(status2.members, &node/1) |> Enum.sort()                == Enum.map(expected_member_nodes, &i2node/1)
        assert Enum.map(status2.unresponsive_followers, &node/1) |> Enum.sort() == Enum.map(unresponsive_member_nodes, &i2node/1)

        kill_all_consensus_members()
        Path.wildcard("tmp/*/*")
        |> Enum.reject(&String.ends_with?(&1, Atom.to_string(RaftFleet.Cluster)))
        |> Enum.each(&File.rm_rf!/1)
      end)
    end)
  end

  test "adjust_one_step/3 should find out members in deactivated-but-still-connected nodes and migrate them to active nodes" do
    with_active_slaves([:"2", :"3", :"4", :"5", :"6"], fn ->
      nodes_half1 = Enum.map([1, 2, 3], &i2node/1)
      nodes_half2 = Enum.map([4, 5, 6], &i2node/1)
      start_leader_and_followers(hd(nodes_half2), tl(nodes_half2))

      # Simulate that `nodes_half2` are deactivated at this point; only `nodes_half1` are participating now
      Enum.each(1..7, fn _ ->
        ConsensusMemberAdjuster.adjust_one_step(nodes_half1, @group_name, nodes_half1)
        :timer.sleep(1_000)
      end)
      :timer.sleep(6_000)

      nodes_with_members2 = consensus_members() |> Enum.map(&node/1)
      assert Enum.sort(nodes_with_members2) == Enum.sort(nodes_half1)
    end)
  end
end
