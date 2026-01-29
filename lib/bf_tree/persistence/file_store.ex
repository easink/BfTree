defmodule BfTree.Persistence.FileStore do
  @moduledoc """
  File-based persistence for BFTree.

  Provides high-level operations for saving and loading complete BFTree state
  from disk, including hot tier, buffer, and cold tier data.

  Directory structure:
  ```
  bftree_db/
  ├── tree.bin              (Hot tier + buffer snapshot)
  └── cold/                 (Cold tier directory)
      ├── metadata.bin
      ├── level_0.bin
      ├── level_1.bin
      └── ...
  ```
  """

  alias BfTree.Persistence.{Serializer, ColdTier}

  @doc """
  Initializes a new persistent store at the given directory.

  Creates directory structure and initializes cold tier.

  Returns `{:ok, store_path}` or `{:error, reason}`.

  ## Examples

      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> File.dir?(path)
      true
  """
  @spec init(Path.t()) :: {:ok, Path.t()} | {:error, term}
  def init(base_dir) when is_binary(base_dir) do
    cold_dir = Path.join(base_dir, "cold")

    with :ok <- File.mkdir_p(base_dir),
         {:ok, _} <- ColdTier.new(cold_dir) do
      {:ok, base_dir}
    end
  end

  @doc """
  Saves a BFTree to persistent storage.

  Saves hot tier and buffer to tree.bin, cold tier to separate directory.

  Returns `{:ok, store_path}` or `{:error, reason}`.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key", "value")
      iex> {:ok, path} = BfTree.Persistence.FileStore.save(tree, "/tmp/bftree_db")
      iex> File.exists?(Path.join(path, "tree.bin"))
      true
  """
  @spec save(BfTree.t(), Path.t()) :: {:ok, Path.t()} | {:error, term}
  def save(%BfTree{} = tree, base_dir) do
    with {:ok, base_dir} <- init(base_dir),
         tree_file = Path.join(base_dir, "tree.bin"),
         binary = Serializer.serialize_tree(tree),
         :ok <- File.write(tree_file, binary) do
      {:ok, base_dir}
    end
  end

  @doc """
  Loads a BFTree from persistent storage.

  Restores hot tier, buffer, and cold tier metadata.

  Returns `{:ok, tree}` or `{:error, reason}`.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key", "value")
      iex> {:ok, path} = BfTree.Persistence.FileStore.save(tree, "/tmp/bftree_db")
      iex> {:ok, restored} = BfTree.Persistence.FileStore.load(path)
      iex> {:ok, value} = BfTree.search(restored, "key")
      iex> value
      "value"
  """
  @spec load(Path.t()) :: {:ok, BfTree.t()} | {:error, term}
  def load(base_dir) when is_binary(base_dir) do
    tree_file = Path.join(base_dir, "tree.bin")

    case File.read(tree_file) do
      {:ok, binary} ->
        Serializer.deserialize_tree(binary)

      {:error, :enoent} ->
        {:error, :tree_file_not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Creates a checkpoint of the current tree state.

  Saves tree and optionally consolidates buffer to create a compact checkpoint.

  Returns `{:ok, checkpoint_name}` or `{:error, reason}`.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> {:ok, checkpoint} = BfTree.Persistence.FileStore.checkpoint(tree, path)
      iex> is_binary(checkpoint)
      true
  """
  @spec checkpoint(BfTree.t(), Path.t()) :: {:ok, String.t()} | {:error, term}
  def checkpoint(%BfTree{} = tree, base_dir) do
    # Generate checkpoint name with timestamp (microseconds for uniqueness)
    timestamp = DateTime.utc_now() |> DateTime.to_unix(:microsecond)
    checkpoint_name = "checkpoint_#{timestamp}"
    checkpoint_dir = Path.join(base_dir, checkpoint_name)

    with {:ok, _} <- init(checkpoint_dir),
         tree_file = Path.join(checkpoint_dir, "tree.bin"),
         binary = Serializer.serialize_tree(tree),
         :ok <- File.write(tree_file, binary) do
      {:ok, checkpoint_name}
    end
  end

  @doc """
  Loads a tree from a specific checkpoint.

  Returns `{:ok, tree}` or `{:error, reason}`.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key1", "value1")
      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> {:ok, checkpoint} = BfTree.Persistence.FileStore.checkpoint(tree, path)
      iex> {:ok, restored} = BfTree.Persistence.FileStore.load_checkpoint(path, checkpoint)
      iex> {:ok, value} = BfTree.search(restored, "key1")
      iex> value
      "value1"
  """
  @spec load_checkpoint(Path.t(), String.t()) :: {:ok, BfTree.t()} | {:error, term}
  def load_checkpoint(base_dir, checkpoint_name)
      when is_binary(base_dir) and is_binary(checkpoint_name) do
    checkpoint_dir = Path.join(base_dir, checkpoint_name)
    load(checkpoint_dir)
  end

  @doc """
  Lists all available checkpoints in a store directory.

  Returns list of checkpoint names sorted by creation time (newest first).

  ## Examples

      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> checkpoints = BfTree.Persistence.FileStore.list_checkpoints(path)
      iex> is_list(checkpoints)
      true
  """
  @spec list_checkpoints(Path.t()) :: list(String.t())
  def list_checkpoints(base_dir) when is_binary(base_dir) do
    case File.ls(base_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.starts_with?(&1, "checkpoint_"))
        |> Enum.sort(:desc)

      {:error, _} ->
        []
    end
  end

  @doc """
  Deletes a checkpoint from the store.

  Returns `:ok` or `{:error, reason}`.

  ## Examples

      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> tree = BfTree.new()
      iex> {:ok, checkpoint} = BfTree.Persistence.FileStore.checkpoint(tree, path)
      iex> :ok = BfTree.Persistence.FileStore.delete_checkpoint(path, checkpoint)
  """
  @spec delete_checkpoint(Path.t(), String.t()) :: :ok | {:error, term}
  def delete_checkpoint(base_dir, checkpoint_name)
      when is_binary(base_dir) and is_binary(checkpoint_name) do
    checkpoint_dir = Path.join(base_dir, checkpoint_name)
    {:ok, _} = File.rm_rf(checkpoint_dir)
    :ok
  end

  @doc """
  Returns information about the store.

  Returns map with store statistics.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, path} = BfTree.Persistence.FileStore.init("/tmp/bftree_db")
      iex> info = BfTree.Persistence.FileStore.info(path)
      iex> is_map(info)
      true
  """
  @spec info(Path.t()) :: map
  def info(base_dir) when is_binary(base_dir) do
    tree_file = Path.join(base_dir, "tree.bin")
    cold_dir = Path.join(base_dir, "cold")

    tree_size =
      case File.stat(tree_file) do
        {:ok, stat} -> stat.size
        {:error, _} -> 0
      end

    cold_size =
      if File.dir?(cold_dir) do
        cold_dir
        |> File.ls!()
        |> Enum.map(&Path.join(cold_dir, &1))
        |> Enum.filter(&File.regular?/1)
        |> Enum.map(&elem(File.stat!(&1), 1))
        |> Enum.sum()
      else
        0
      end

    checkpoints = list_checkpoints(base_dir)

    %{
      directory: base_dir,
      tree_size_bytes: tree_size,
      cold_size_bytes: cold_size,
      total_size_bytes: tree_size + cold_size,
      checkpoint_count: length(checkpoints),
      checkpoints: checkpoints
    }
  end
end
