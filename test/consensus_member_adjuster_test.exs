defmodule RaftFleet.ConsensusMemberAdjusterTest do
  use Croma.TestCase
  import SlaveNode
  alias RaftFleet.{Manager, ConsensusMemberSup}

  setup_all do
    Node.start(:"1", :shortnames)
    :ok
  end

  setup do
    # For clean testing we restart :raft_fleet
    :ok = Application.stop(:raft_fleet)
    :ok = Application.start(:raft_fleet)
  end

  @group_name :consensus_group
  @rv_config  RaftedValue.make_config(RaftFleet.JustAnInt)

  defp consensus_members do
    [Node.self | Node.list] |> Enum.map(fn n ->
      Process.whereis(@group_name) |> at(n)
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp i2node(i) do
    s = Integer.to_string(i)
    [Node.self | Node.list]
    |> Enum.find(fn n ->
      Atom.to_string(n) |> String.starts_with?(s)
    end)
  end

  defp start_leader_and_followers(leader_node, follower_nodes, group_name \\ @group_name) do
    Manager.start_consensus_group_members(group_name, @rv_config, []) |> at(leader_node)
    Enum.each(follower_nodes, fn n ->
      :timer.sleep(100)
      Manager.start_consensus_group_follower(group_name, n, leader_node)
    end)
    :timer.sleep(100)
  end

  defp call_adjust_one_step(group_name \\ @group_name) do
    desired_member_nodes = Enum.map([1, 2, 3], &i2node/1)
    ConsensusMemberAdjuster.adjust_one_step([Node.self | Node.list], group_name, desired_member_nodes)
    :timer.sleep(500)
  end

  defp kill_all_consensus_members do
    [Node.self | Node.list]
    |> Enum.flat_map(fn n -> Supervisor.which_children({ConsensusMemberSup, n}) end)
    |> Enum.each(fn {_, pid, _, _} -> Process.exit(pid, :kill) end)
  end

  defp with_active_slaves(shortnames, f) do
    with_slaves(shortnames, fn ->
      with_active_nodes([Node.self | Node.list], &zone(&1, 1), fn ->
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
        assert RaftedValue.status({@group_name, i2node(expected_leader_node)})[:state_name] == :leader

        kill_all_consensus_members()
      end)
    end)
  end

  test "adjust_one_step/3 should remove pid that is definitely dead" do
    with_active_slaves([:"2", :"3"], fn ->
      start_leader_and_followers(Node.self, Node.list)

      %{members: members1} = RaftedValue.status(@group_name)
      assert length(members1) == 3
      target_node = hd(Node.list)
      target_pid  = Enum.find(members1, fn pid -> node(pid) == target_node end)
      Process.exit(target_pid, :kill)
      :timer.sleep(2_000) # wait until the killed process is recognized as "unresponsive" by the consensus leader

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

  test "adjust_one_step/3 should remove consensus group whose majority of members are definitely dead" do
    with_active_slaves([:"2", :"3"], fn ->
      # To avoid complication due to periodic adjustment process, we don't add_consensus_group here;
      # for testing whether `:remove_group` is done, we look into Raft logs.
      raft_log_includes_removal_of_group? = fn(group_name) ->
        {_, state} = :sys.get_state(RaftFleet.Cluster)
        Enum.any?(state.logs.map, &match?({i, {_term, i, :command, {_from, {:remove_group, ^group_name}, _ref}}}, &1))
      end

      exec = fn(target_nodes, group_name) ->
        start_leader_and_followers(Node.self, Node.list, group_name)
        refute raft_log_includes_removal_of_group?.(group_name)

        Enum.each(target_nodes, fn n ->
          pid = Process.whereis(group_name) |> at(n)
          Process.exit(pid, :kill)
        end)
        :timer.sleep(2_000) # wait until the remaining leader (if any) steps down

        call_adjust_one_step(group_name)
        assert raft_log_includes_removal_of_group?.(group_name)
      end

      [Node.self | Node.list]                        |> exec.(:consensus_group_1)
      [Node.self | Node.list] |> Enum.take_random(2) |> exec.(:consensus_group_2)
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
        :timer.sleep(500)
      end)
      :timer.sleep(500)

      nodes_with_members2 = consensus_members() |> Enum.map(&node/1)
      assert Enum.sort(nodes_with_members2) == Enum.sort(nodes_half1)
    end)
  end
end
