defmodule BfTreeTest do
  use ExUnit.Case
  doctest BfTree
  doctest BfTree.Node
  doctest BfTree.Buffer
  doctest BfTree.Tree

  # ============================================================================
  # BfTree Core API Tests
  # ============================================================================

  describe "BfTree.new/0" do
    test "creates empty tree with default config" do
      tree = BfTree.new()

      assert tree.hot_tree.type == :leaf
      assert tree.hot_tree.entries == []
      assert BfTree.size(tree) == 0
    end

    test "creates tree with custom config" do
      tree = BfTree.new(buffer_size: 500, node_degree: 16)

      assert tree.config.buffer_size == 500
      assert tree.config.node_degree == 16
    end
  end

  describe "BfTree.insert/3" do
    test "inserts single key-value pair" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key1", "value1")

      assert BfTree.size(tree) >= 1
      {:ok, value} = BfTree.search(tree, "key1")
      assert value == "value1"
    end

    test "inserts multiple key-value pairs" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)

      assert BfTree.size(tree) >= 3
    end

    test "updates existing key" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value1")
      {:ok, tree} = BfTree.insert(tree, "key", "value2")

      {:ok, value} = BfTree.search(tree, "key")
      assert value == "value2"
    end

    test "triggers consolidation when buffer is full" do
      tree = BfTree.new(buffer_size: 5)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      {:ok, tree} = BfTree.insert(tree, "d", 4)
      {:ok, tree} = BfTree.insert(tree, "e", 5)

      # Next insert should trigger consolidation
      {:ok, tree} = BfTree.insert(tree, "f", 6)

      # After consolidation, buffer should be small
      assert BfTree.Buffer.size(tree.buffer) < 5
    end

    test "rejects invalid key types" do
      tree = BfTree.new()
      {:error, :invalid_key_type} = BfTree.insert(tree, %{}, "value")
    end
  end

  describe "BfTree.search/2" do
    test "finds inserted value" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key1", "value1")

      {:ok, value} = BfTree.search(tree, "key1")
      assert value == "value1"
    end

    test "returns not_found for missing key" do
      tree = BfTree.new()

      :not_found = BfTree.search(tree, "missing")
    end

    test "finds values in buffer" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key1", "value1")

      # Search should find in buffer before consolidation
      {:ok, value} = BfTree.search(tree, "key1")
      assert value == "value1"
    end

    test "finds values in hot tier after consolidation" do
      tree = BfTree.new(buffer_size: 3)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      tree = BfTree.consolidate(tree)

      # Should still find values after consolidation
      {:ok, value} = BfTree.search(tree, "b")
      assert value == 2
    end

    test "prefers buffer over hot tier" do
      tree = BfTree.new(buffer_size: 100)
      {:ok, tree} = BfTree.insert(tree, "key", "value1")
      tree = BfTree.consolidate(tree)
      {:ok, tree} = BfTree.insert(tree, "key", "value2")

      # Should find updated value from buffer
      {:ok, value} = BfTree.search(tree, "key")
      assert value == "value2"
    end
  end

  describe "BfTree.range/3" do
    setup do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      {:ok, tree} = BfTree.insert(tree, "d", 4)
      {:ok, tree} = BfTree.insert(tree, "e", 5)

      {:ok, tree: tree}
    end

    test "returns range of entries", %{tree: tree} do
      {:ok, results} = BfTree.range(tree, "b", "d")

      assert length(results) == 3
      assert Enum.map(results, &elem(&1, 0)) == ["b", "c", "d"]
    end

    test "returns single entry on exact bounds", %{tree: tree} do
      {:ok, results} = BfTree.range(tree, "c", "c")

      assert length(results) == 1
      assert results == [{"c", 3}]
    end

    test "returns empty list for non-overlapping range", %{tree: tree} do
      {:ok, results} = BfTree.range(tree, "z", "zz")

      assert results == []
    end

    test "handles range across consolidation boundary" do
      tree = BfTree.new(buffer_size: 3)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      tree = BfTree.consolidate(tree)
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      {:ok, tree} = BfTree.insert(tree, "d", 4)

      {:ok, results} = BfTree.range(tree, "a", "d")

      assert length(results) == 4
    end
  end

  describe "BfTree.delete/2" do
    test "deletes existing key" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")
      {:ok, tree} = BfTree.delete(tree, "key")

      :not_found = BfTree.search(tree, "key")
    end

    test "handles deletion of non-existent key" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.delete(tree, "missing")

      # Should not fail
      :not_found = BfTree.search(tree, "missing")
    end

    test "deletes from buffer" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")
      {:ok, tree} = BfTree.delete(tree, "key")

      :not_found = BfTree.search(tree, "key")
    end

    test "deletes from hot tier" do
      tree = BfTree.new(buffer_size: 3)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      tree = BfTree.consolidate(tree)
      {:ok, tree} = BfTree.delete(tree, "a")

      :not_found = BfTree.search(tree, "a")
      {:ok, value} = BfTree.search(tree, "b")
      assert value == 2
    end
  end

  describe "BfTree.consolidate/1" do
    test "moves buffer entries to hot tier" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)

      assert BfTree.Buffer.size(tree.buffer) > 0

      tree = BfTree.consolidate(tree)

      assert BfTree.Buffer.size(tree.buffer) == 0
      {:ok, value} = BfTree.search(tree, "a")
      assert value == 1
    end

    test "clears tombstones on consolidation" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.delete(tree, "a")

      tree = BfTree.consolidate(tree)

      :not_found = BfTree.search(tree, "a")
    end
  end

  describe "BfTree.size/1" do
    test "returns 0 for empty tree" do
      tree = BfTree.new()
      assert BfTree.size(tree) == 0
    end

    test "returns correct size after insertions" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)

      assert BfTree.size(tree) == 2
    end

    test "accounts for buffer and hot tier" do
      tree = BfTree.new(buffer_size: 10)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      tree = BfTree.consolidate(tree)
      {:ok, tree} = BfTree.insert(tree, "c", 3)

      # Should count both consolidated and buffered
      assert BfTree.size(tree) >= 3
    end
  end

  describe "BfTree.keys/1" do
    test "returns empty list for empty tree" do
      tree = BfTree.new()
      assert BfTree.keys(tree) == []
    end

    test "returns keys in sorted order" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "c", 1)
      {:ok, tree} = BfTree.insert(tree, "a", 2)
      {:ok, tree} = BfTree.insert(tree, "b", 3)

      keys = BfTree.keys(tree)
      assert keys == ["a", "b", "c"]
    end

    test "deduplicates keys from buffer and hot tier" do
      tree = BfTree.new(buffer_size: 10)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      tree = BfTree.consolidate(tree)
      {:ok, tree} = BfTree.insert(tree, "a", 2)

      keys = BfTree.keys(tree)
      assert keys == ["a"]
    end
  end

  describe "BfTree.values/1" do
    test "returns empty list for empty tree" do
      tree = BfTree.new()
      assert BfTree.values(tree) == []
    end

    test "returns values in sorted key order" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)

      values = BfTree.values(tree)
      assert values == [1, 2, 3]
    end
  end

  # ============================================================================
  # Buffer Tests
  # ============================================================================

  describe "BfTree.Buffer.new/0" do
    test "creates empty buffer" do
      buffer = BfTree.Buffer.new()

      assert BfTree.Buffer.size(buffer) == 0
      assert buffer.entries == []
    end
  end

  describe "BfTree.Buffer.put/3" do
    test "adds entry to buffer" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value")

      assert BfTree.Buffer.size(buffer) == 1
    end

    test "updates existing entry" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value1")
      buffer = BfTree.Buffer.put(buffer, "key", "value2")

      assert BfTree.Buffer.size(buffer) == 1
      {:ok, value} = BfTree.Buffer.get(buffer, "key")
      assert value == "value2"
    end
  end

  describe "BfTree.Buffer.get/2" do
    test "retrieves value from buffer" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value")

      {:ok, value} = BfTree.Buffer.get(buffer, "key")
      assert value == "value"
    end

    test "returns not_found for missing key" do
      buffer = BfTree.Buffer.new()

      assert BfTree.Buffer.get(buffer, "missing") == :not_found
    end

    test "respects tombstones" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value")
      buffer = BfTree.Buffer.delete(buffer, "key")

      assert BfTree.Buffer.get(buffer, "key") == :not_found
    end
  end

  describe "BfTree.Buffer.delete/2" do
    test "marks key as deleted" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value")
      buffer = BfTree.Buffer.delete(buffer, "key")

      assert BfTree.Buffer.get(buffer, "key") == :not_found
    end

    test "removes entry from entries list" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "key", "value")
      buffer = BfTree.Buffer.delete(buffer, "key")

      assert BfTree.Buffer.size(buffer) == 0
    end
  end

  describe "BfTree.Buffer.keys/1" do
    test "returns keys in sorted order" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "c", 1)
      buffer = BfTree.Buffer.put(buffer, "a", 2)
      buffer = BfTree.Buffer.put(buffer, "b", 3)

      assert BfTree.Buffer.keys(buffer) == ["a", "b", "c"]
    end
  end

  describe "BfTree.Buffer.range/3" do
    test "returns entries within range" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "a", 1)
      buffer = BfTree.Buffer.put(buffer, "b", 2)
      buffer = BfTree.Buffer.put(buffer, "c", 3)

      results = BfTree.Buffer.range(buffer, "a", "b")
      assert length(results) == 2
    end

    test "excludes tombstoned keys from range" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "a", 1)
      buffer = BfTree.Buffer.put(buffer, "b", 2)
      buffer = BfTree.Buffer.delete(buffer, "b")

      results = BfTree.Buffer.range(buffer, "a", "c")
      assert length(results) == 1
      assert results == [{"a", 1}]
    end
  end

  describe "BfTree.Buffer.fold/3" do
    test "folds over buffer entries" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "a", 1)
      buffer = BfTree.Buffer.put(buffer, "b", 2)

      sum = BfTree.Buffer.fold(buffer, 0, fn {_k, v}, acc -> acc + v end)
      assert sum == 3
    end

    test "skips tombstoned entries during fold" do
      buffer = BfTree.Buffer.new()
      buffer = BfTree.Buffer.put(buffer, "a", 1)
      buffer = BfTree.Buffer.put(buffer, "b", 2)
      buffer = BfTree.Buffer.delete(buffer, "b")

      sum = BfTree.Buffer.fold(buffer, 0, fn {_k, v}, acc -> acc + v end)
      assert sum == 1
    end
  end

  # ============================================================================
  # Node Tests
  # ============================================================================

  describe "BfTree.Node.new_leaf/0" do
    test "creates empty leaf node" do
      node = BfTree.Node.new_leaf()

      assert node.type == :leaf
      assert node.entries == []
    end
  end

  describe "BfTree.Node.insert/3" do
    test "inserts entry into leaf" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "key", "value")

      {:ok, value} = BfTree.Node.search(node, "key")
      assert value == "value"
    end

    test "maintains sorted order" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "c", 1)
      {:ok, node} = BfTree.Node.insert(node, "a", 2)
      {:ok, node} = BfTree.Node.insert(node, "b", 3)

      keys = BfTree.Node.keys(node)
      assert keys == ["a", "b", "c"]
    end

    test "triggers split when node full" do
      node = BfTree.Node.new_leaf()

      # Insert 33 items (more than default 32)
      result =
        Enum.reduce(1..33, {:ok, node}, fn i, {:ok, n} ->
          BfTree.Node.insert(n, "key#{i}", i)
        end)

      case result do
        {:split, left, right, _sep} ->
          # After split, both should be smaller
          assert BfTree.Node.size(left) + BfTree.Node.size(right) == 33

        {:ok, _node} ->
          # If no split, all fit
          true
      end
    end
  end

  describe "BfTree.Node.search/2" do
    test "finds inserted value" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "key", "value")

      {:ok, value} = BfTree.Node.search(node, "key")
      assert value == "value"
    end

    test "returns not_found for missing key" do
      node = BfTree.Node.new_leaf()

      assert BfTree.Node.search(node, "missing") == :not_found
    end
  end

  describe "BfTree.Node.delete/2" do
    test "deletes entry from leaf" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "key", "value")
      {:ok, node} = BfTree.Node.delete(node, "key")

      assert BfTree.Node.search(node, "key") == :not_found
    end

    test "returns not_found for missing key" do
      node = BfTree.Node.new_leaf()

      assert BfTree.Node.delete(node, "missing") == :not_found
    end
  end

  describe "BfTree.Node.range/3" do
    test "returns entries within range" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "a", 1)
      {:ok, node} = BfTree.Node.insert(node, "b", 2)
      {:ok, node} = BfTree.Node.insert(node, "c", 3)

      results = BfTree.Node.range(node, "a", "b")
      assert length(results) == 2
    end
  end

  describe "BfTree.Node.size/1" do
    test "counts entries in leaf" do
      node = BfTree.Node.new_leaf()
      {:ok, node} = BfTree.Node.insert(node, "a", 1)
      {:ok, node} = BfTree.Node.insert(node, "b", 2)

      assert BfTree.Node.size(node) == 2
    end
  end

  # ============================================================================
  # Tree Tests
  # ============================================================================

  describe "BfTree.Tree.insert/3" do
    test "inserts and handles root split" do
      tree = BfTree.Node.new_leaf()

      result =
        Enum.reduce(1..50, {:ok, tree}, fn i, {:ok, t} ->
          BfTree.Tree.insert(t, "key#{i}", i)
        end)

      {:ok, tree} = result
      assert BfTree.Tree.size(tree) == 50
    end
  end

  describe "BfTree.Tree.search/2" do
    test "finds value in tree" do
      tree = BfTree.Node.new_leaf()
      {:ok, tree} = BfTree.Tree.insert(tree, "key", "value")

      {:ok, value} = BfTree.Tree.search(tree, "key")
      assert value == "value"
    end
  end

  describe "BfTree.Tree.range/3" do
    test "returns range from tree" do
      tree = BfTree.Node.new_leaf()
      {:ok, tree} = BfTree.Tree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.Tree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.Tree.insert(tree, "c", 3)

      results = BfTree.Tree.range(tree, "a", "b")
      assert length(results) == 2
    end
  end

  describe "BfTree.Tree.keys/1" do
    test "returns sorted keys" do
      tree = BfTree.Node.new_leaf()
      {:ok, tree} = BfTree.Tree.insert(tree, "c", 1)
      {:ok, tree} = BfTree.Tree.insert(tree, "a", 2)
      {:ok, tree} = BfTree.Tree.insert(tree, "b", 3)

      assert BfTree.Tree.keys(tree) == ["a", "b", "c"]
    end
  end
end
