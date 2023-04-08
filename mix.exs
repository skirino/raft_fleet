defmodule RaftFleet.Mixfile do
  use Mix.Project

  @github_url "https://github.com/skirino/raft_fleet"

  def project() do
    [
      app:               :raft_fleet,
      version:           "0.10.2",
      elixir:            "~> 1.7",
      build_embedded:    Mix.env() == :prod,
      start_permanent:   Mix.env() == :prod,
      deps:              deps(),
      description:       "A fleet of Raft consensus groups",
      package:           package(),
      source_url:        @github_url,
      homepage_url:      @github_url,
      test_coverage:     [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test],
      dialyzer:          [ignore_warnings: ".dialyzer_ignore.exs"],
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
      {:croma       , "~> 0.10"},
      {:rafted_value, "~> 0.9"},
      {:dialyxir    , "~> 1.3" , [only: :dev , runtime: false]},
      {:ex_doc      , "~> 0.29", [only: :dev , runtime: false]},
      {:excoveralls , "~> 0.16", [only: :test, runtime: false]},
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
