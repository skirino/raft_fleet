defmodule RaftFleet.MemberAdjusterTest do
  use Croma.TestCase
  import SlaveNode
  alias RaftFleet.{Manager, MemberSup}

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

  test "MemberAdjuster.adjust_one_step/3" do
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
        :timer.sleep(500)

        MemberAdjuster.adjust_one_step([Node.self | Node.list], @group_name, desired_member_nodes)
        :timer.sleep(500)
        assert length(consensus_members) == n_expected
        assert RaftedValue.status({@group_name, i2node(expected_leader_node)})[:state_name] == :leader

        [Node.self | Node.list]
        |> Enum.flat_map(fn n -> Supervisor.which_children({MemberSup, n}) end)
        |> Enum.each(fn {_, pid, _, _} -> Process.exit(pid, :kill) end)
      end)
    end)
  end
end
