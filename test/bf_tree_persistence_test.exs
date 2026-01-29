defmodule BfTree.Persistence.SerializerTest do
  use ExUnit.Case
  alias BfTree.Persistence.Serializer

  describe "Serializer.serialize_tree/1 and deserialize_tree/1" do
    test "round-trips an empty tree" do
      tree = BfTree.new()
      binary = Serializer.serialize_tree(tree)
      {:ok, restored} = Serializer.deserialize_tree(binary)

      assert BfTree.size(restored) == BfTree.size(tree)
    end

    test "round-trips a tree with data" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)

      binary = Serializer.serialize_tree(tree)
      {:ok, restored} = Serializer.deserialize_tree(binary)

      {:ok, val_a} = BfTree.search(restored, "a")
      {:ok, val_b} = BfTree.search(restored, "b")
      {:ok, val_c} = BfTree.search(restored, "c")

      assert val_a == 1
      assert val_b == 2
      assert val_c == 3
    end

    test "preserves buffer state" do
      tree = BfTree.new(buffer_size: 100)
      {:ok, tree} = BfTree.insert(tree, "key1", "value1")

      binary = Serializer.serialize_tree(tree)
      {:ok, restored} = Serializer.deserialize_tree(binary)

      {:ok, value} = BfTree.search(restored, "key1")
      assert value == "value1"
    end

    test "preserves config" do
      tree = BfTree.new(buffer_size: 500, node_degree: 16)

      binary = Serializer.serialize_tree(tree)
      {:ok, restored} = Serializer.deserialize_tree(binary)

      assert restored.config.buffer_size == 500
      assert restored.config.node_degree == 16
    end

    test "handles invalid binary gracefully" do
      {:error, _} = Serializer.deserialize_tree("invalid binary data")
    end
  end

  describe "Serializer.serialize_entries/1 and deserialize_entries/1" do
    test "round-trips entry lists" do
      entries = [{"a", 1}, {"b", 2}, {"c", 3}]

      binary = Serializer.serialize_entries(entries)
      restored = Serializer.deserialize_entries(binary)

      assert restored == entries
    end

    test "handles empty lists" do
      entries = []

      binary = Serializer.serialize_entries(entries)
      restored = Serializer.deserialize_entries(binary)

      assert restored == []
    end

    test "preserves entry order" do
      entries = [{"z", 26}, {"a", 1}, {"m", 13}]

      binary = Serializer.serialize_entries(entries)
      restored = Serializer.deserialize_entries(binary)

      assert restored == entries
    end
  end
end

defmodule BfTree.Persistence.ColdTierTest do
  use ExUnit.Case
  alias BfTree.Persistence.ColdTier

  @test_dir "/tmp/bftree_cold_test_#{System.monotonic_time()}"

  setup do
    File.rm_rf(@test_dir)
    on_exit(fn -> File.rm_rf(@test_dir) end)
    :ok
  end

  describe "ColdTier.new/1" do
    test "creates a new cold tier directory" do
      {:ok, tier} = ColdTier.new(@test_dir)

      assert File.dir?(@test_dir)
      assert tier.dir == @test_dir
      assert tier.levels == []
      assert tier.current_level == []
    end

    test "handles directory creation errors" do
      {:error, _} = ColdTier.new("/invalid/path/that/does/not/exist/bftree")
    end
  end

  describe "ColdTier.load/1" do
    test "loads an existing cold tier" do
      {:ok, tier1} = ColdTier.new(@test_dir)
      tier1 = ColdTier.add(tier1, "a", 1)
      {:ok, _tier1} = ColdTier.flush(tier1)

      {:ok, tier2} = ColdTier.load(@test_dir)
      assert tier2.dir == @test_dir
      assert ColdTier.level_count(tier2) == 1
    end

    test "creates empty tier for non-existent dir" do
      empty_dir = "/tmp/bftree_empty_#{System.monotonic_time()}"
      File.rm_rf(empty_dir)

      {:ok, tier} = ColdTier.load(empty_dir)
      assert tier.levels == []

      File.rm_rf(empty_dir)
    end

    test "returns error for invalid path" do
      {:error, {:failed_to_create_dir, _}} = ColdTier.load("/invalid/path/bftree")
    end
  end

  describe "ColdTier.add/3 and ColdTier.current_level_size/1" do
    test "adds entries to current level" do
      {:ok, tier} = ColdTier.new(@test_dir)
      assert ColdTier.current_level_size(tier) == 0

      tier = ColdTier.add(tier, "a", 1)
      assert ColdTier.current_level_size(tier) == 1

      tier = ColdTier.add(tier, "b", 2)
      assert ColdTier.current_level_size(tier) == 2
    end

    test "entries can be retrieved from current level" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "key", "value")

      {:ok, value} = ColdTier.search(tier, "key")
      assert value == "value"
    end
  end

  describe "ColdTier.flush/1" do
    test "flushes current level to disk" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)
      tier = ColdTier.add(tier, "b", 2)

      assert ColdTier.current_level_size(tier) == 2
      assert ColdTier.level_count(tier) == 0

      {:ok, tier} = ColdTier.flush(tier)

      assert ColdTier.current_level_size(tier) == 0
      assert ColdTier.level_count(tier) == 1
    end

    test "returns ok for empty flush" do
      {:ok, tier} = ColdTier.new(@test_dir)
      {:ok, tier} = ColdTier.flush(tier)

      assert ColdTier.current_level_size(tier) == 0
    end

    test "sorts entries before writing" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "c", 3)
      tier = ColdTier.add(tier, "a", 1)
      tier = ColdTier.add(tier, "b", 2)

      {:ok, tier} = ColdTier.flush(tier)

      {:ok, results} = ColdTier.range(tier, "a", "c")
      keys = Enum.map(results, &elem(&1, 0))
      assert keys == ["a", "b", "c"]
    end
  end

  describe "ColdTier.search/2" do
    test "searches in current level" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)

      {:ok, value} = ColdTier.search(tier, "a")
      assert value == 1
    end

    test "searches in persisted levels" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)
      {:ok, tier} = ColdTier.flush(tier)

      {:ok, value} = ColdTier.search(tier, "a")
      assert value == 1
    end

    test "prefers current level over persisted levels" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)
      {:ok, tier} = ColdTier.flush(tier)

      # Add newer value to current level
      tier = ColdTier.add(tier, "a", 2)

      {:ok, value} = ColdTier.search(tier, "a")
      assert value == 2
    end

    test "returns not_found for missing keys" do
      {:ok, tier} = ColdTier.new(@test_dir)
      :not_found = ColdTier.search(tier, "missing")
    end
  end

  describe "ColdTier.range/3" do
    test "queries range from persisted levels" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)
      tier = ColdTier.add(tier, "b", 2)
      tier = ColdTier.add(tier, "c", 3)
      {:ok, tier} = ColdTier.flush(tier)

      {:ok, results} = ColdTier.range(tier, "a", "b")
      assert length(results) == 2
    end

    test "merges current level and persisted levels" do
      {:ok, tier} = ColdTier.new(@test_dir)
      tier = ColdTier.add(tier, "a", 1)
      tier = ColdTier.add(tier, "b", 2)
      {:ok, tier} = ColdTier.flush(tier)

      tier = ColdTier.add(tier, "c", 3)

      {:ok, results} = ColdTier.range(tier, "a", "c")
      keys = Enum.map(results, &elem(&1, 0))
      assert Enum.member?(keys, "a")
      assert Enum.member?(keys, "b")
      assert Enum.member?(keys, "c")
    end
  end

  describe "ColdTier metadata" do
    test "tracks level count accurately" do
      {:ok, tier} = ColdTier.new(@test_dir)
      assert ColdTier.level_count(tier) == 0

      tier = ColdTier.add(tier, "a", 1)
      {:ok, tier} = ColdTier.flush(tier)
      assert ColdTier.level_count(tier) == 1

      tier = ColdTier.add(tier, "b", 2)
      {:ok, tier} = ColdTier.flush(tier)
      assert ColdTier.level_count(tier) == 2
    end
  end
end

defmodule BfTree.Persistence.FileStoreTest do
  use ExUnit.Case
  alias BfTree.Persistence.FileStore

  @test_db "/tmp/bftree_filestore_test_#{System.monotonic_time()}"

  setup do
    File.rm_rf(@test_db)
    on_exit(fn -> File.rm_rf(@test_db) end)
    :ok
  end

  describe "FileStore.init/1" do
    test "creates store directory structure" do
      {:ok, path} = FileStore.init(@test_db)

      assert File.dir?(path)
      assert File.dir?(Path.join(path, "cold"))
    end
  end

  describe "FileStore.save/2 and load/1" do
    test "saves and restores trees" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")

      {:ok, path} = FileStore.save(tree, @test_db)
      assert File.exists?(Path.join(path, "tree.bin"))

      {:ok, restored} = FileStore.load(path)
      {:ok, val} = BfTree.search(restored, "key")
      assert val == "value"
    end

    test "handles empty trees" do
      tree = BfTree.new()

      {:ok, path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(path)

      assert BfTree.size(restored) == 0
    end

    test "preserves multiple entries" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)

      {:ok, path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(path)

      assert BfTree.size(restored) == 3
      {:ok, results} = BfTree.range(restored, "a", "c")
      assert length(results) == 3
    end
  end

  describe "FileStore.checkpoint/2" do
    test "creates a checkpoint" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")

      {:ok, path} = FileStore.init(@test_db)
      {:ok, checkpoint_name} = FileStore.checkpoint(tree, path)

      assert String.starts_with?(checkpoint_name, "checkpoint_")
      assert File.exists?(Path.join(path, checkpoint_name))
    end

    test "multiple checkpoints can coexist" do
      tree = BfTree.new()
      {:ok, path} = FileStore.init(@test_db)

      {:ok, cp1} = FileStore.checkpoint(tree, path)
      {:ok, cp2} = FileStore.checkpoint(tree, path)

      assert cp1 != cp2
      assert File.exists?(Path.join(path, cp1))
      assert File.exists?(Path.join(path, cp2))
    end
  end

  describe "FileStore.load_checkpoint/2" do
    test "loads a specific checkpoint" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")

      {:ok, path} = FileStore.init(@test_db)
      {:ok, checkpoint_name} = FileStore.checkpoint(tree, path)
      {:ok, restored} = FileStore.load_checkpoint(path, checkpoint_name)

      {:ok, value} = BfTree.search(restored, "key")
      assert value == "value"
    end
  end

  describe "FileStore.list_checkpoints/1" do
    test "lists checkpoints in order" do
      {:ok, path} = FileStore.init(@test_db)
      tree = BfTree.new()

      {:ok, cp1} = FileStore.checkpoint(tree, path)
      :timer.sleep(10)
      {:ok, cp2} = FileStore.checkpoint(tree, path)

      checkpoints = FileStore.list_checkpoints(path)

      assert length(checkpoints) == 2
      # Newest first
      assert Enum.at(checkpoints, 0) == cp2
      assert Enum.at(checkpoints, 1) == cp1
    end

    test "returns empty list for empty directory" do
      {:ok, path} = FileStore.init(@test_db)
      checkpoints = FileStore.list_checkpoints(path)

      assert checkpoints == []
    end
  end

  describe "FileStore.delete_checkpoint/2" do
    test "deletes a checkpoint" do
      tree = BfTree.new()
      {:ok, path} = FileStore.init(@test_db)
      {:ok, checkpoint_name} = FileStore.checkpoint(tree, path)

      :ok = FileStore.delete_checkpoint(path, checkpoint_name)

      checkpoint_dir = Path.join(path, checkpoint_name)
      refute File.exists?(checkpoint_dir)
    end
  end

  describe "FileStore.info/1" do
    test "returns store information" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")

      {:ok, path} = FileStore.save(tree, @test_db)
      info = FileStore.info(path)

      assert is_map(info)
      assert info.directory == path
      assert is_integer(info.tree_size_bytes)
      assert is_integer(info.cold_size_bytes)
      assert is_integer(info.total_size_bytes)
      assert is_list(info.checkpoints)
    end
  end
end

# Integration tests combining Phase 1 and Phase 2
defmodule BfTree.IntegrationTest do
  use ExUnit.Case

  alias BfTree.Persistence.FileStore

  @test_db "/tmp/bftree_integration_test_#{System.monotonic_time()}"

  setup do
    File.rm_rf(@test_db)

    on_exit(fn ->
      File.rm_rf(@test_db)
    end)

    :ok
  end

  describe "Phase 1 + Phase 2: Complete Workflow" do
    test "create tree, populate, save, restore, and verify" do
      # Phase 1: Create and populate tree
      tree = BfTree.new(buffer_size: 100, node_degree: 16)

      {:ok, tree} = BfTree.insert(tree, "alice", 100)
      {:ok, tree} = BfTree.insert(tree, "bob", 200)
      {:ok, tree} = BfTree.insert(tree, "charlie", 150)
      {:ok, tree} = BfTree.insert(tree, "diana", 250)
      {:ok, tree} = BfTree.insert(tree, "eve", 120)

      assert BfTree.size(tree) == 5

      # Phase 2: Save to disk
      {:ok, db_path} = FileStore.save(tree, @test_db)
      assert File.dir?(db_path)

      # Verify tree.bin exists
      tree_file = Path.join(db_path, "tree.bin")
      assert File.exists?(tree_file)

      # Phase 2: Load from disk
      {:ok, restored} = FileStore.load(db_path)

      # Verify restored tree has same data
      assert BfTree.size(restored) == 5
      {:ok, alice} = BfTree.search(restored, "alice")
      assert alice == 100

      {:ok, bob} = BfTree.search(restored, "bob")
      assert bob == 200

      {:ok, results} = BfTree.range(restored, "alice", "diana")
      assert length(results) == 4
    end

    test "multiple checkpoints preserve different states" do
      # Create initial tree
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      {:ok, db_path} = FileStore.init(@test_db)

      # Create first checkpoint
      {:ok, cp1} = FileStore.checkpoint(tree, db_path)

      # Modify tree
      {:ok, tree} = BfTree.insert(tree, "key2", "value2")
      {:ok, tree} = BfTree.insert(tree, "key3", "value3")

      # Create second checkpoint
      {:ok, cp2} = FileStore.checkpoint(tree, db_path)

      # Verify checkpoints are different
      assert cp1 != cp2

      # Load first checkpoint
      {:ok, restored_cp1} = FileStore.load_checkpoint(db_path, cp1)
      assert BfTree.size(restored_cp1) == 1
      {:ok, val1} = BfTree.search(restored_cp1, "key1")
      assert val1 == "value1"
      :not_found = BfTree.search(restored_cp1, "key2")

      # Load second checkpoint
      {:ok, restored_cp2} = FileStore.load_checkpoint(db_path, cp2)
      assert BfTree.size(restored_cp2) == 3
      {:ok, val2} = BfTree.search(restored_cp2, "key2")
      assert val2 == "value2"
      {:ok, val3} = BfTree.search(restored_cp2, "key3")
      assert val3 == "value3"
    end

    test "consolidation preserves data through persistence cycle" do
      tree = BfTree.new(buffer_size: 10, node_degree: 8)

      # Insert enough data to trigger consolidation
      {:ok, tree} = BfTree.insert(tree, "a", 1)
      {:ok, tree} = BfTree.insert(tree, "b", 2)
      {:ok, tree} = BfTree.insert(tree, "c", 3)
      {:ok, tree} = BfTree.insert(tree, "d", 4)
      {:ok, tree} = BfTree.insert(tree, "e", 5)

      # Consolidate buffer to hot tier
      tree = BfTree.consolidate(tree)

      # Verify consolidated data
      assert BfTree.size(tree) == 5

      # Save and restore
      {:ok, db_path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(db_path)

      # Verify all data still there after consolidation and persistence
      assert BfTree.size(restored) == 5
      {:ok, results} = BfTree.range(restored, "a", "e")
      keys = Enum.map(results, &elem(&1, 0))
      assert keys == ["a", "b", "c", "d", "e"]
    end

    test "deletions are preserved through save/restore" do
      tree = BfTree.new()

      # Insert data
      {:ok, tree} = BfTree.insert(tree, "alice", 100)
      {:ok, tree} = BfTree.insert(tree, "bob", 200)
      {:ok, tree} = BfTree.insert(tree, "charlie", 150)

      # Delete one entry
      {:ok, tree} = BfTree.delete(tree, "bob")

      # Verify deletion
      assert BfTree.size(tree) == 2
      :not_found = BfTree.search(tree, "bob")

      # Save and restore
      {:ok, db_path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(db_path)

      # Verify deletion persisted
      assert BfTree.size(restored) == 2
      :not_found = BfTree.search(restored, "bob")
      {:ok, alice} = BfTree.search(restored, "alice")
      assert alice == 100
      {:ok, charlie} = BfTree.search(restored, "charlie")
      assert charlie == 150
    end

    test "large dataset workflow" do
      tree = BfTree.new(buffer_size: 1000, node_degree: 32)

      # Insert 100 key-value pairs
      tree =
        Enum.reduce(1..100, tree, fn i, acc_tree ->
          key = "key_#{String.pad_leading(Integer.to_string(i), 5, "0")}"
          {:ok, new_tree} = BfTree.insert(acc_tree, key, i * 10)
          new_tree
        end)

      assert BfTree.size(tree) == 100

      # Save
      {:ok, db_path} = FileStore.save(tree, @test_db)

      # Restore
      {:ok, restored} = FileStore.load(db_path)
      assert BfTree.size(restored) == 100

      # Verify some random entries
      {:ok, val42} = BfTree.search(restored, "key_00042")
      assert val42 == 420

      {:ok, val99} = BfTree.search(restored, "key_00099")
      assert val99 == 990

      # Verify range query
      {:ok, results} = BfTree.range(restored, "key_00010", "key_00020")
      assert length(results) == 11
    end

    test "store info reflects saved state" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "k1", "v1")
      {:ok, tree} = BfTree.insert(tree, "k2", "v2")
      {:ok, tree} = BfTree.insert(tree, "k3", "v3")

      {:ok, db_path} = FileStore.save(tree, @test_db)
      info = FileStore.info(db_path)

      # Verify info structure
      assert is_map(info)
      assert info.directory == db_path
      assert info.tree_size_bytes > 0
      assert info.total_size_bytes >= info.tree_size_bytes

      # Create checkpoint and verify it's listed
      {:ok, cp_name} = FileStore.checkpoint(tree, db_path)
      checkpoints = FileStore.list_checkpoints(db_path)
      assert Enum.member?(checkpoints, cp_name)
    end

    test "round-trip with consolidation and checkpoint" do
      # Start with buffered data
      tree = BfTree.new(buffer_size: 5, node_degree: 8)

      {:ok, tree} = BfTree.insert(tree, "x", 10)
      {:ok, tree} = BfTree.insert(tree, "y", 20)
      {:ok, tree} = BfTree.insert(tree, "z", 30)

      # Consolidate to move data to hot tier
      tree = BfTree.consolidate(tree)

      # Create database
      {:ok, db_path} = FileStore.init(@test_db)

      # Create checkpoint
      {:ok, cp1} = FileStore.checkpoint(tree, db_path)

      # Restore from checkpoint
      {:ok, restored} = FileStore.load_checkpoint(db_path, cp1)

      # Verify data integrity
      assert BfTree.size(restored) == 3
      {:ok, val_x} = BfTree.search(restored, "x")
      assert val_x == 10

      # Continue operations on restored tree
      {:ok, restored} = BfTree.insert(restored, "w", 5)

      # Verify new data
      assert BfTree.size(restored) == 4
      {:ok, val_w} = BfTree.search(restored, "w")
      assert val_w == 5
    end

    test "multiple saves to same database path" do
      tree1 = BfTree.new()
      {:ok, tree1} = BfTree.insert(tree1, "data", "version1")

      # First save
      {:ok, db_path} = FileStore.save(tree1, @test_db)
      {:ok, restored1} = FileStore.load(db_path)
      {:ok, val1} = BfTree.search(restored1, "data")
      assert val1 == "version1"

      # Modify and save again to same path
      tree2 = BfTree.new()
      {:ok, tree2} = BfTree.insert(tree2, "data", "version2")
      {:ok, db_path2} = FileStore.save(tree2, @test_db)

      # Paths should be same
      assert db_path == db_path2

      # Load should get latest version
      {:ok, restored2} = FileStore.load(db_path2)
      {:ok, val2} = BfTree.search(restored2, "data")
      assert val2 == "version2"
    end
  end

  describe "Error Handling & Edge Cases" do
    test "search in restored tree handles missing keys" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "exists", "yes")

      {:ok, db_path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(db_path)

      {:ok, val} = BfTree.search(restored, "exists")
      assert val == "yes"

      :not_found = BfTree.search(restored, "missing")
    end

    test "checkpoint deletion doesn't affect other checkpoints" do
      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")
      {:ok, db_path} = FileStore.init(@test_db)

      {:ok, cp1} = FileStore.checkpoint(tree, db_path)
      {:ok, cp2} = FileStore.checkpoint(tree, db_path)
      {:ok, cp3} = FileStore.checkpoint(tree, db_path)

      # Delete middle checkpoint
      :ok = FileStore.delete_checkpoint(db_path, cp2)

      # Others should still be loadable
      {:ok, restored1} = FileStore.load_checkpoint(db_path, cp1)
      assert BfTree.size(restored1) == 1

      {:ok, restored3} = FileStore.load_checkpoint(db_path, cp3)
      assert BfTree.size(restored3) == 1
    end

    test "range queries work across restored data" do
      tree = BfTree.new()

      # Insert ordered data
      tree =
        Enum.reduce(["apple", "banana", "cherry", "date", "elderberry"], tree, fn key, acc_tree ->
          {:ok, new_tree} = BfTree.insert(acc_tree, key, String.length(key))
          new_tree
        end)

      {:ok, db_path} = FileStore.save(tree, @test_db)
      {:ok, restored} = FileStore.load(db_path)

      # Range query on restored tree
      {:ok, results} = BfTree.range(restored, "banana", "date")
      keys = Enum.map(results, &elem(&1, 0))

      assert length(keys) == 3
      assert Enum.member?(keys, "banana")
      assert Enum.member?(keys, "cherry")
      assert Enum.member?(keys, "date")
    end
  end
end
