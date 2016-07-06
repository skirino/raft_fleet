defmodule RaftFleet.ConsensusMemberAdjusterTest do
  use Croma.TestCase
  import SlaveNode
  alias RaftFleet.{Manager, ConsensusMemberSup}

  setup_all do
    Node.start(:"1", :shortnames)
    :ok
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

  defp kill_all_consensus_members do
    [Node.self | Node.list]
    |> Enum.flat_map(fn n -> Supervisor.which_children({ConsensusMemberSup, n}) end)
    |> Enum.each(fn {_, pid, _, _} -> Process.exit(pid, :kill) end)
  end

  test "adjust_one_step/3" do
    with_slaves([:"2", :"3", :"4"], fn ->
      desired_member_nodes = Enum.map([1, 2, 3], &i2node/1)
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
        Manager.start_consensus_group_leader(@group_name, @rv_config) |> at(i2node(leader_node))
        Enum.each(follower_nodes, fn n ->
          :timer.sleep(100)
          Manager.start_consensus_group_follower(@group_name, i2node(n))
        end)
        :timer.sleep(100)

        ConsensusMemberAdjuster.adjust_one_step([Node.self | Node.list], @group_name, desired_member_nodes)
        :timer.sleep(100)
        assert length(consensus_members) == n_expected
        assert RaftedValue.status({@group_name, i2node(expected_leader_node)})[:state_name] == :leader

        kill_all_consensus_members
      end)
    end)
  end

  test "adjust_one_step/3 should remove pid that is definitely dead" do
    with_slaves([:"2", :"3"], fn ->
      desired_member_nodes = Enum.map([1, 2, 3], &i2node/1)

      Manager.start_consensus_group_leader(@group_name, @rv_config)
      Enum.each(Node.list, fn n ->
        :timer.sleep(100)
        Manager.start_consensus_group_follower(@group_name, n)
      end)
      :timer.sleep(100)

      %{members: members1} = RaftedValue.status(@group_name)
      assert length(members1) == 3
      target_node = hd(Node.list)
      target_pid  = Enum.find(members1, fn pid -> node(pid) == target_node end)
      Process.exit(target_pid, :kill)
      :timer.sleep(2_000) # wait until the killed process is recognized by the consensus leader

      ConsensusMemberAdjuster.adjust_one_step([Node.self | Node.list], @group_name, desired_member_nodes)
      :timer.sleep(100)
      %{members: members2} = RaftedValue.status(@group_name)
      assert members2 == List.delete(members1, target_pid)

      ConsensusMemberAdjuster.adjust_one_step([Node.self | Node.list], @group_name, desired_member_nodes)
      :timer.sleep(100)
      %{members: members3} = RaftedValue.status(@group_name)
      assert length(members3) == 3
      [new_pid] = members3 -- members2
      assert node(new_pid) == target_node

      kill_all_consensus_members
    end)
  end
end
