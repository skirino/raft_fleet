defmodule RaftFleet.ClusterRecoveryTest do
  use ExUnit.Case
  alias RaftFleet.ConsensusMemberSup

  defmodule RVConfigMaker do
    @behaviour RaftFleet.RaftedValueConfigMaker

    @impl true
    def make(name) do
      case name do
        RaftFleet.Cluster -> RaftFleet.Cluster.default_rv_config()
        _                 ->
          RaftedValue.make_config(RaftFleet.JustAnInt, [
            heartbeat_timeout: 500,
            election_timeout: 2500, # In travis disk I/O is sometimes rather slow, resulting in more frequent leader elections
          ])
      end
    end
  end

  setup do
    # For clean testing we restart :raft_fleet
    case Application.stop(:raft_fleet) do
      :ok                                   -> :ok
      {:error, {:not_started, :raft_fleet}} -> :ok
    end
    PersistenceSetting.turn_on_persistence(Node.self())
    Application.put_env(:raft_fleet, :rafted_value_config_maker, RVConfigMaker)
    File.rm_rf!("tmp")
    :ok = Application.start(:raft_fleet)
    on_exit(fn ->
      Application.delete_env(:raft_fleet, :per_member_options_maker)
      File.rm_rf!("tmp")
      :timer.sleep(1000) # try to avoid slave start failures in travis
    end)
  end

  defp start_activate_stop(f) do
    :ok = Application.ensure_started(:raft_fleet)
    :ok = RaftFleet.activate("zone")
    f.()
    :ok = Application.stop(:raft_fleet)
  end

  test "recovery from files should restore all previously existed consensus groups" do
    start_activate_stop(fn ->
      :ok
    end)

    start_activate_stop(fn ->
      assert RaftFleet.consensus_groups == %{}
      assert Supervisor.which_children(ConsensusMemberSup) == []
      assert RaftFleet.add_consensus_group(:c1) == :ok
      [{_, pid, _, _}] = Supervisor.which_children(ConsensusMemberSup)
      assert Process.info(pid)[:registered_name] == :c1
    end)
    refute Process.whereis(:c1)

    start_activate_stop(fn ->
      assert RaftFleet.consensus_groups() == %{c1: 3}
      [{_, pid, _, _}] = Supervisor.which_children(ConsensusMemberSup)
      assert Process.info(pid)[:registered_name] == :c1
      :ok = RaftFleet.remove_consensus_group(:c1)
    end)
    refute Process.whereis(:c1)

    start_activate_stop(fn ->
      assert RaftFleet.consensus_groups() == %{}
      assert Supervisor.which_children(ConsensusMemberSup) == []
    end)
  end
end
