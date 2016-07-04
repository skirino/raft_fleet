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
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 3) == [:d, :c, :b]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 4) == [:d, :c, :b, :f]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 5) == [:d, :c, :b, :f, :a]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 6) == [:d, :c, :b, :f, :a, :e]
    assert NodesPerZone.lrw_members(nodes2, :cg_name, 7) == [:d, :c, :b, :f, :a, :e]

    nodes3 = %{"zone1" => [:a, :b, :c], "zone2" => [:d, :e, :f], "zone3" => [:g, :h, :i]}
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  1) == [:d]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  2) == [:d, :g]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  3) == [:d, :g, :c]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  4) == [:d, :g, :c, :b]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  5) == [:d, :g, :c, :b, :f]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  6) == [:d, :g, :c, :b, :f, :i]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  7) == [:d, :g, :c, :b, :f, :i, :a]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  8) == [:d, :g, :c, :b, :f, :i, :a, :h]
    assert NodesPerZone.lrw_members(nodes3, :cg_name,  9) == [:d, :g, :c, :b, :f, :i, :a, :h, :e]
    assert NodesPerZone.lrw_members(nodes3, :cg_name, 10) == [:d, :g, :c, :b, :f, :i, :a, :h, :e]

    nodes4 = %{"zone1" => [:a, :b], "zone2" => [:c]}
    assert NodesPerZone.lrw_members(nodes4, :cg_name, 1) == [:c]
    assert NodesPerZone.lrw_members(nodes4, :cg_name, 2) == [:c, :b]
    assert NodesPerZone.lrw_members(nodes4, :cg_name, 3) == [:c, :b, :a]
    assert NodesPerZone.lrw_members(nodes4, :cg_name, 4) == [:c, :b, :a]

    nodes5 = %{"zone1" => [:a], "zone2" => [:b, :c]}
    assert NodesPerZone.lrw_members(nodes5, :cg_name, 1) == [:c]
    assert NodesPerZone.lrw_members(nodes5, :cg_name, 2) == [:c, :a]
    assert NodesPerZone.lrw_members(nodes5, :cg_name, 3) == [:c, :a, :b]
    assert NodesPerZone.lrw_members(nodes5, :cg_name, 4) == [:c, :a, :b]
  end
end
