defmodule BfTree do
  @moduledoc """
  BFTree - A hybrid B+tree with write buffering for modern hardware.

  BFTree combines three paradigms to optimize for contemporary workloads:
  - **Hot tier**: In-memory B+tree for recent data
  - **Write buffer**: Batches insertions to reduce tree updates
  - **Cold tier**: Disk-based sorted key-value storage for historical data

  ## Features

  - Pure functional API - all operations return new tree instances
  - Hot-cold tier separation for efficient memory usage
  - Write buffering to reduce insert latency
  - Approximate indexing for disk-based cold storage
  - Generic key-value storage (comparable keys, any values)
  - File-based persistence support

  ## Basic Usage

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> {:ok, value} = BfTree.search(tree, "key1")
      iex> value
      "value1"

  ## Configuration

  Create a BFTree with custom settings:

      tree = BfTree.new(
        buffer_size: 1000,
        node_degree: 32,
        consolidate_interval: 10000
      )

  - `:buffer_size` - Maximum entries in write buffer before consolidation
  - `:node_degree` - B+tree node branching factor
  - `:consolidate_interval` - Milliseconds between automatic consolidations

  ## Persistence (Phase 2+)

  Save and load trees from disk:

      tree = BfTree.new()
      {:ok, tree} = BfTree.insert(tree, "key", "value")

      # Save tree to disk
      {:ok, path} = BfTree.Persistence.FileStore.save(tree, "/tmp/db")

      # Load tree from disk
      {:ok, restored} = BfTree.Persistence.FileStore.load(path)
  """

  alias BfTree.{Node, Buffer, Tree}

  @type key :: term
  @type value :: term
  @type config :: %{
          buffer_size: pos_integer,
          node_degree: pos_integer,
          consolidate_interval: pos_integer
        }

  @type t :: %BfTree{
          hot_tree: Node.t(),
          buffer: Buffer.t(),
          config: config,
          metadata: %{
            size: non_neg_integer,
            last_consolidation: non_neg_integer
          }
        }

  defstruct [
    :hot_tree,
    :buffer,
    :config,
    :metadata
  ]

  @default_config %{
    buffer_size: 1000,
    node_degree: 32,
    consolidate_interval: 10000
  }

  @doc """
  Creates a new empty BFTree with optional configuration.

  ## Examples

      iex> tree = BfTree.new()
      iex> BfTree.size(tree)
      0

      iex> tree = BfTree.new(buffer_size: 500)
      iex> tree.config.buffer_size
      500
  """
  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    config = merge_config(@default_config, opts)

    %BfTree{
      hot_tree: Node.new_leaf(),
      buffer: Buffer.new(),
      config: config,
      metadata: %{
        size: 0,
        last_consolidation: System.monotonic_time(:millisecond)
      }
    }
  end

  @doc """
  Inserts a key-value pair into the BFTree.

  Returns `{:ok, new_tree}` on success, `{:error, reason}` on failure.

  Keys must be comparable. If the key already exists, the value is updated.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", 100)
      iex> {:ok, value} = BfTree.search(tree, "key1")
      iex> value
      100

  """
  @spec insert(t(), key, value) :: {:ok, t()} | {:error, term}
  def insert(%BfTree{} = tree, key, value) do
    with :ok <- validate_key(key) do
      new_buffer = Buffer.put(tree.buffer, key, value)

      new_tree = %{tree | buffer: new_buffer}

      # Check if consolidation needed
      if Buffer.size(new_buffer) >= tree.config.buffer_size do
        {:ok, consolidate(new_tree)}
      else
        {:ok, new_tree}
      end
    end
  end

  @doc """
  Searches for a key in the BFTree.

  Returns `{:ok, value}` if found, `:not_found` otherwise.

  Searches both hot tier and buffer.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> BfTree.search(tree, "key1")
      {:ok, "value1"}

      iex> BfTree.search(BfTree.new(), "nonexistent")
      :not_found
  """
  @spec search(t(), key) :: {:ok, value} | :not_found
  def search(%BfTree{} = tree, key) do
    with :ok <- validate_key(key) do
      # Search buffer first (most recent writes)
      case Buffer.get(tree.buffer, key) do
        {:ok, value} -> {:ok, value}
        :not_found -> Tree.search(tree.hot_tree, key)
      end
    end
  end

  @doc """
  Searches for a range of keys in the BFTree.

  Returns `{:ok, results}` where results is a list of {key, value} tuples,
  sorted by key. Both inclusive bounds.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "a", 1)
      iex> {:ok, tree} = BfTree.insert(tree, "b", 2)
      iex> {:ok, tree} = BfTree.insert(tree, "c", 3)
      iex> {:ok, results} = BfTree.range(tree, "a", "b")
      iex> length(results)
      2
  """
  @spec range(t(), key, key) :: {:ok, list({key, value})} | {:error, term}
  def range(%BfTree{} = tree, min_key, max_key) do
    with :ok <- validate_key(min_key),
         :ok <- validate_key(max_key) do
      hot_results = Tree.range(tree.hot_tree, min_key, max_key)
      buffer_results = Buffer.range(tree.buffer, min_key, max_key)

      # Merge and deduplicate, preferring buffer (more recent)
      merged = merge_results(hot_results, buffer_results)
      {:ok, merged}
    end
  end

  @doc """
  Deletes a key from the BFTree.

  Returns `{:ok, new_tree}` on success. If key doesn't exist, still succeeds.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> {:ok, tree} = BfTree.delete(tree, "key1")
      iex> BfTree.search(tree, "key1")
      :not_found
  """
  @spec delete(t(), key) :: {:ok, t()} | {:error, term}
  def delete(%BfTree{} = tree, key) do
    with :ok <- validate_key(key) do
      # Mark deletion in buffer (can be a tombstone or removal)
      new_buffer = Buffer.delete(tree.buffer, key)
      new_tree = %{tree | buffer: new_buffer}

      # Try to delete from hot tree if present
      case Tree.delete(new_tree.hot_tree, key) do
        {:ok, new_hot_tree} ->
          {:ok, %{new_tree | hot_tree: new_hot_tree}}

        :not_found ->
          {:ok, new_tree}
      end
    end
  end

  @doc """
  Consolidates the write buffer into the hot tier.

  This is typically called automatically when buffer reaches size limit,
  but can be called manually to optimize performance.

  Returns new tree with consolidation complete.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> tree = BfTree.consolidate(tree)
      iex> tree.buffer.entries
      []
  """
  @spec consolidate(t()) :: t()
  def consolidate(%BfTree{} = tree) do
    # Move all buffer entries into hot tree
    new_hot_tree =
      Buffer.fold(tree.buffer, tree.hot_tree, fn {k, v}, acc ->
        {:ok, updated} = Tree.insert(acc, k, v)
        updated
      end)

    # Reset buffer and update metadata
    %{
      tree
      | buffer: Buffer.new(),
        hot_tree: new_hot_tree,
        metadata: %{
          tree.metadata
          | last_consolidation: System.monotonic_time(:millisecond),
            size: tree.metadata.size + Buffer.size(tree.buffer)
        }
    }
  end

  @doc """
  Returns the number of entries in the BFTree (hot tier + buffer).

  ## Examples

      iex> tree = BfTree.new()
      iex> BfTree.size(tree)
      0

      iex> {:ok, tree} = BfTree.insert(BfTree.new(), "key1", "value1")
      iex> BfTree.size(tree)
      1
  """
  @spec size(t()) :: non_neg_integer
  def size(%BfTree{} = tree) do
    # Note: This is approximate - buffer may contain updates to existing keys
    Node.size(tree.hot_tree) + Buffer.size(tree.buffer)
  end

  @doc """
  Returns all keys in the BFTree in sorted order.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "b", 1)
      iex> {:ok, tree} = BfTree.insert(tree, "a", 2)
      iex> BfTree.keys(tree)
      ["a", "b"]
  """
  @spec keys(t()) :: list(key)
  def keys(%BfTree{} = tree) do
    hot_keys = Node.keys(tree.hot_tree)
    buffer_keys = Buffer.keys(tree.buffer)

    # Merge, remove duplicates, and sort
    (hot_keys ++ buffer_keys)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc """
  Returns all values in the BFTree (corresponding to sorted keys).

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "a", 1)
      iex> {:ok, tree} = BfTree.insert(tree, "b", 2)
      iex> BfTree.values(tree)
      [1, 2]
  """
  @spec values(t()) :: list(value)
  def values(%BfTree{} = tree) do
    keys(tree)
    |> Enum.map(fn key ->
      {:ok, value} = search(tree, key)
      value
    end)
  end

  @doc """
  Returns configuration settings for the BFTree.

  ## Examples

      iex> tree = BfTree.new(buffer_size: 500)
      iex> BfTree.config(tree).buffer_size
      500
  """
  @spec config(t()) :: config
  def config(%BfTree{} = tree), do: tree.config

  # Private helpers

  defp merge_config(default, opts) do
    Enum.reduce(opts, default, fn {key, value}, acc ->
      Map.put(acc, key, value)
    end)
  end

  defp validate_key(key) when is_binary(key), do: :ok
  defp validate_key(key) when is_integer(key), do: :ok
  defp validate_key(key) when is_atom(key), do: :ok
  defp validate_key(_), do: {:error, :invalid_key_type}

  defp merge_results(hot_results, buffer_results) do
    # Convert to map, buffer takes precedence, then sort
    buffer_map = Map.new(buffer_results)

    hot_results
    |> Enum.reject(fn {k, _} -> Map.has_key?(buffer_map, k) end)
    |> Enum.concat(buffer_results)
    |> Enum.sort_by(&elem(&1, 0))
  end
end
