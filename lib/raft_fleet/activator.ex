use Croma

defmodule RaftFleet.Activator do
  alias RaftFleet.Cluster

  @tries 5
  @sleep 1_000

  def activate(zone) do
    run_steps([:start_cluster_consensus_member, {:add_node, zone}], @tries)
  end

  defp run_steps(_, 0), do: raise "Failed to complete all steps of node activation!"
  defp run_steps([], _), do: :ok
  defp run_steps([s | ss], tries_remaining) do
    case step(s) do
      :ok    -> run_steps(ss, tries_remaining)
      :error ->
        :timer.sleep(@sleep)
        run_steps([s | ss], tries_remaining - 1)
    end
  end

  defp step(:start_cluster_consensus_member) do
    case Supervisor.start_child(RaftFleet.Supervisor, Cluster.Server.child_spec()) do
      {:ok, _}    -> :ok
      {:error, _} -> :error
    end
  end
  defp step({:add_node, zone}) do
    case RaftFleet.command(Cluster, {:add_node, Node.self(), zone}) do
      {:ok, _}    -> :ok
      {:error, _} -> :error
    end
  end
end
