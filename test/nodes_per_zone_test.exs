defmodule RaftFleet.NodesPerZoneTest do
  use Croma.TestCase
  alias RaftFleet.Hash

  test "lrw_members/3 should choose member nodes according to hash values" do
    z1 = Hash.calc({"zone1", :cg_name})
    z2 = Hash.calc({"zone2", :cg_name})
    z3 = Hash.calc({"zone3", :cg_name})
    assert z3 < z2 and z2 < z1
    a = Hash.calc({:a, :cg_name})
    b = Hash.calc({:b, :cg_name})
    c = Hash.calc({:c, :cg_name})
    assert c < b and b < a
    d = Hash.calc({:d, :cg_name})
    e = Hash.calc({:e, :cg_name})
    f = Hash.calc({:f, :cg_name})
    assert d < f and f < e
    g = Hash.calc({:g, :cg_name})
    h = Hash.calc({:h, :cg_name})
    i = Hash.calc({:i, :cg_name})
    assert g < i and i < h

    nodes0 = %{}
    assert NodesPerZone.lrw_members(nodes0, :cg_name, 1) == []
    assert NodesPerZone.lrw_members(nodes0, :cg_name, 2) == []

    nodes1 = %{"zone1" => [:a, :b, :c]}
    assert NodesPerZone.lrw_members(nodes1, :cg_name, 1) == [:c]
    assert NodesPerZone.lrw_members(nodes1, :cg_name, 2) == [:c, :b]
    assert NodesPerZone.lrw_members(nodes1, :cg_name, 3) == [:c, :b, :a]
    assert NodesPerZone.lrw_members(nodes1, :cg_name, 4) == [:c, :b, :a]

    nodes2 = %{"zone1" => [:a, :b, :c], "zone2" => [:d, :e, :f]}
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 1) == [:d]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 2) == [:d, :c]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 3) == [:d, :f, :c]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 4) == [:d, :f, :c, :b]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 5) == [:d, :f, :e, :c, :b]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 6) == [:d, :f, :e, :c, :b, :a]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 7) == [:d, :f, :e, :c, :b, :a]

    nodes3 = %{"zone1" => [:a, :b, :c], "zone2" => [:d, :e, :f], "zone3" => [:g, :h, :i]}
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  1) == [:g]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  2) == [:g, :d]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  3) == [:g, :d, :c]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  4) == [:g, :i, :d, :c]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  5) == [:g, :i, :d, :f, :c]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  6) == [:g, :i, :d, :f, :c, :b]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  7) == [:g, :i, :h, :d, :f, :c, :b]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  8) == [:g, :i, :h, :d, :f, :e, :c, :b]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  9) == [:g, :i, :h, :d, :f, :e, :c, :b, :a]
    assert NodesPerZone.lrw_members(nodes3, :cg_name, 10) == [:g, :i, :h, :d, :f, :e, :c, :b, :a]
  end
end
