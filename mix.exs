defmodule RaftFleet.Mixfile do
  use Mix.Project

  @github_url "https://github.com/skirino/raft_fleet"

  def project() do
    [
      app:             :raft_fleet,
      version:         "0.5.2",
      elixir:          "~> 1.4",
      build_embedded:  Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps:            deps(),
      description:     "A fleet of Raft consensus groups",
      package:         package(),
      source_url:      @github_url,
      homepage_url:    @github_url,
      test_coverage:   [tool: Coverex.Task, coveralls: true],
    ]
  end

  def application() do
    [
      applications: [:croma, :rafted_value, :logger],
      mod: {RaftFleet, []},
    ]
  end

  defp deps() do
    [
      {:croma       , "~> 0.7"},
      {:rafted_value, "~> 0.4"},
      {:coverex     , "~> 1.4" , only: :test},
      {:dialyze     , "~> 0.2" , only: :dev },
      {:ex_doc      , "~> 0.14", only: :dev },
    ]
  end

  defp package() do
    [
      files:       ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Shunsuke Kirino"],
      licenses:    ["MIT"],
      links:       %{"GitHub repository" => @github_url},
    ]
  end
end
