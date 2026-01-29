defmodule BfTree.Persistence.ColdTier do
  @moduledoc """
  Cold tier storage for historical data on disk.

  The cold tier stores data in sorted levels, similar to LSM-tree design:
  - Each level is a sorted sequence of {key, value} pairs
  - Levels are immutable after creation
  - Queries use binary search with approximate indexing
  - Automatic compaction merges levels over time

  File structure:
  ```
  cold_tier_dir/
  ├── metadata.bin           (level count, sizes)
  ├── level_0.bin            (newest/smallest level)
  ├── level_1.bin
  ├── level_2.bin
  └── ...
  ```
  """

  alias BfTree.Persistence.Serializer

  @type level :: non_neg_integer
  @type level_entry :: {BfTree.key(), BfTree.value()}

  @doc """
  Creates a new cold tier at the specified directory.

  Returns `{:ok, cold_tier}` or `{:error, reason}`.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> is_map(tier)
      true
  """
  @spec new(Path.t()) :: {:ok, map} | {:error, term}
  def new(dir) when is_binary(dir) do
    case File.mkdir_p(dir) do
      :ok ->
        tier = %{
          dir: dir,
          levels: [],
          current_level: [],
          metadata_file: Path.join(dir, "metadata.bin")
        }

        {:ok, tier}

      {:error, reason} ->
        {:error, {:failed_to_create_dir, reason}}
    end
  end

  @doc """
  Loads an existing cold tier from disk.

  If the directory doesn't exist, creates an empty cold tier.

  Returns `{:ok, cold_tier}` or `{:error, reason}`.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> {:ok, loaded} = BfTree.Persistence.ColdTier.load("/tmp/bftree_cold")
      iex> loaded.dir
      "/tmp/bftree_cold"
  """
  @spec load(Path.t()) :: {:ok, map} | {:error, term}
  def load(dir) when is_binary(dir) do
    case File.mkdir_p(dir) do
      :ok ->
        metadata_file = Path.join(dir, "metadata.bin")

        case load_metadata(metadata_file) do
          {:ok, levels_info} ->
            tier = %{
              dir: dir,
              levels: levels_info,
              current_level: [],
              metadata_file: metadata_file
            }

            {:ok, tier}

          :not_found ->
            # No metadata, empty cold tier
            {:ok, new_empty(dir)}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, {:failed_to_create_dir, reason}}
    end
  end

  @doc """
  Adds an entry to the current level (in-memory staging area).

  Multiple entries are batched before flushing to disk.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "key1", "value1")
      iex> BfTree.Persistence.ColdTier.current_level_size(tier)
      1
  """
  @spec add(map, BfTree.key(), BfTree.value()) :: map
  def add(tier, key, value) do
    entry = {key, value}
    current = [entry | tier.current_level]

    %{tier | current_level: current}
  end

  @doc """
  Flushes the current level to disk as a new level file.

  Returns `{:ok, updated_tier}` or `{:error, reason}`.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "key1", "value1")
      iex> {:ok, tier} = BfTree.Persistence.ColdTier.flush(tier)
      iex> BfTree.Persistence.ColdTier.current_level_size(tier)
      0
  """
  @spec flush(map) :: {:ok, map} | {:error, term}
  def flush(%{current_level: []} = tier) do
    # Nothing to flush
    {:ok, tier}
  end

  def flush(tier) do
    # Sort current level and write to disk
    sorted_entries = Enum.sort(tier.current_level)
    level_number = length(tier.levels)
    level_file = Path.join(tier.dir, "level_#{level_number}.bin")

    try do
      binary = Serializer.serialize_entries(sorted_entries)
      :ok = File.write(level_file, binary)

      # Update metadata
      new_levels = tier.levels ++ [{level_number, length(sorted_entries)}]
      save_metadata(tier.metadata_file, new_levels)

      {:ok, %{tier | levels: new_levels, current_level: []}}
    rescue
      e -> {:error, {:flush_failed, e}}
    end
  end

  @doc """
  Searches for a key in the cold tier using binary search.

  Returns `{:ok, value}` if found, `:not_found` otherwise.

  Searches through all levels from newest to oldest (allows overrides).

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "key1", "value1")
      iex> {:ok, tier} = BfTree.Persistence.ColdTier.flush(tier)
      iex> {:ok, value} = BfTree.Persistence.ColdTier.search(tier, "key1")
      iex> value
      "value1"
  """
  @spec search(map, BfTree.key()) :: {:ok, BfTree.value()} | :not_found
  def search(tier, key) do
    # Search current level first (most recent)
    case Enum.find(tier.current_level, fn {k, _} -> k == key end) do
      {^key, value} ->
        {:ok, value}

      nil ->
        # Search levels from newest to oldest
        search_levels(tier, key, tier.levels |> Enum.reverse())
    end
  end

  @doc """
  Returns range of entries within bounds from cold tier.

  Returns sorted list of {key, value} pairs.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "a", 1)
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "b", 2)
      iex> tier = BfTree.Persistence.ColdTier.add(tier, "c", 3)
      iex> {:ok, tier} = BfTree.Persistence.ColdTier.flush(tier)
      iex> {:ok, results} = BfTree.Persistence.ColdTier.range(tier, "a", "b")
      iex> length(results)
      2
  """
  @spec range(map, BfTree.key(), BfTree.key()) :: {:ok, list(level_entry)} | {:error, term}
  def range(tier, min_key, max_key) do
    try do
      # Collect from current level
      current_results =
        tier.current_level
        |> Enum.filter(fn {k, _} -> k >= min_key and k <= max_key end)

      # Collect from persisted levels
      level_results =
        Enum.flat_map(tier.levels, fn {level_num, _} ->
          load_and_filter_level(tier.dir, level_num, min_key, max_key)
        end)

      # Merge and deduplicate (current level takes precedence)
      current_keys = MapSet.new(Enum.map(current_results, &elem(&1, 0)))

      all_results =
        (current_results ++
           Enum.reject(level_results, fn {k, _} -> MapSet.member?(current_keys, k) end))
        |> Enum.sort_by(&elem(&1, 0))

      {:ok, all_results}
    rescue
      e -> {:error, {:range_query_failed, e}}
    end
  end

  @doc """
  Returns count of entries in current level (staged but not flushed).

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> BfTree.Persistence.ColdTier.current_level_size(tier)
      0

      iex> tier = BfTree.Persistence.ColdTier.add(tier, "key", "value")
      iex> BfTree.Persistence.ColdTier.current_level_size(tier)
      1
  """
  @spec current_level_size(map) :: non_neg_integer
  def current_level_size(tier) do
    length(tier.current_level)
  end

  @doc """
  Returns total count of persisted levels on disk.

  ## Examples

      iex> {:ok, tier} = BfTree.Persistence.ColdTier.new("/tmp/bftree_cold")
      iex> BfTree.Persistence.ColdTier.level_count(tier)
      0
  """
  @spec level_count(map) :: non_neg_integer
  def level_count(tier) do
    length(tier.levels)
  end

  # Private helpers

  defp new_empty(dir) do
    %{
      dir: dir,
      levels: [],
      current_level: [],
      metadata_file: Path.join(dir, "metadata.bin")
    }
  end

  defp load_metadata(file) do
    case File.read(file) do
      {:ok, binary} ->
        try do
          levels = :erlang.binary_to_term(binary)
          {:ok, levels}
        rescue
          _ -> :error
        end

      {:error, :enoent} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp save_metadata(file, levels) do
    binary = :erlang.term_to_binary(levels)
    File.write(file, binary)
  end

  defp search_levels(_tier, _key, []) do
    :not_found
  end

  defp search_levels(tier, key, [{level_num, _size} | rest]) do
    case load_and_search_level(tier.dir, level_num, key) do
      {:ok, value} -> {:ok, value}
      :not_found -> search_levels(tier, key, rest)
    end
  end

  defp load_and_search_level(dir, level_num, key) do
    level_file = Path.join(dir, "level_#{level_num}.bin")

    case File.read(level_file) do
      {:ok, binary} ->
        entries = Serializer.deserialize_entries(binary)

        case Enum.find(entries, fn {k, _} -> k == key end) do
          {^key, value} -> {:ok, value}
          nil -> :not_found
        end

      {:error, _} ->
        :not_found
    end
  end

  defp load_and_filter_level(dir, level_num, min_key, max_key) do
    level_file = Path.join(dir, "level_#{level_num}.bin")

    case File.read(level_file) do
      {:ok, binary} ->
        entries = Serializer.deserialize_entries(binary)

        entries
        |> Enum.filter(fn {k, _} -> k >= min_key and k <= max_key end)

      {:error, _} ->
        []
    end
  end
end
