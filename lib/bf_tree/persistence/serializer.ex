defmodule BfTree.Persistence.Serializer do
  @moduledoc """
  Binary serialization for BFTree structures.

  Converts trees, nodes, and buffers to/from compact binary format for storage.

  Format Overview:
  - Uses Erlang binary format for compatibility and efficiency
  - Supports versioning for forward compatibility
  - Preserves all structural information losslessly
  """

  @version 1

  @doc """
  Serializes a complete BFTree to binary format.

  Returns binary data suitable for file storage.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key", "value")
      iex> binary = BfTree.Persistence.Serializer.serialize_tree(tree)
      iex> is_binary(binary)
      true
  """
  @spec serialize_tree(BfTree.t()) :: binary
  def serialize_tree(%BfTree{} = tree) do
    data = {
      :bftree,
      @version,
      serialize_node(tree.hot_tree),
      serialize_buffer(tree.buffer),
      tree.config,
      tree.metadata
    }

    :erlang.term_to_binary(data)
  end

  @doc """
  Deserializes a BFTree from binary format.

  Returns `{:ok, tree}` on success, `{:error, reason}` on failure.

  ## Examples

      iex> tree = BfTree.new()
      iex> {:ok, tree} = BfTree.insert(tree, "key", "value")
      iex> binary = BfTree.Persistence.Serializer.serialize_tree(tree)
      iex> {:ok, restored} = BfTree.Persistence.Serializer.deserialize_tree(binary)
      iex> {:ok, value} = BfTree.search(restored, "key")
      iex> value
      "value"
  """
  @spec deserialize_tree(binary) :: {:ok, BfTree.t()} | {:error, term}
  def deserialize_tree(binary) when is_binary(binary) do
    try do
      {:bftree, version, hot_tree, buffer, config, metadata} =
        :erlang.binary_to_term(binary)

      if version == @version do
        tree = %BfTree{
          hot_tree: deserialize_node(hot_tree),
          buffer: deserialize_buffer(buffer),
          config: config,
          metadata: metadata
        }

        {:ok, tree}
      else
        {:error, {:unsupported_version, version}}
      end
    rescue
      e -> {:error, {:deserialization_failed, e}}
    end
  end

  @doc """
  Serializes a node to binary format.

  Used internally for tree serialization.
  """
  @spec serialize_node(BfTree.Node.t()) :: binary
  def serialize_node(node) do
    :erlang.term_to_binary(node)
  end

  @doc """
  Deserializes a node from binary format.

  Used internally for tree deserialization.
  """
  @spec deserialize_node(binary) :: BfTree.Node.t()
  def deserialize_node(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  end

  @doc """
  Serializes a buffer to binary format.

  Used internally for tree serialization.
  """
  @spec serialize_buffer(BfTree.Buffer.t()) :: binary
  def serialize_buffer(buffer) do
    :erlang.term_to_binary(buffer)
  end

  @doc """
  Deserializes a buffer from binary format.

  Used internally for tree deserialization.
  """
  @spec deserialize_buffer(binary) :: BfTree.Buffer.t()
  def deserialize_buffer(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  end

  @doc """
  Serializes an entry list for cold storage.

  Optimized for sequential writes. Returns list of {key, value} pairs
  serialized for efficient disk access.

  ## Examples

      iex> entries = [{"a", 1}, {"b", 2}]
      iex> binary = BfTree.Persistence.Serializer.serialize_entries(entries)
      iex> is_binary(binary)
      true
  """
  @spec serialize_entries(list({BfTree.key(), BfTree.value()})) :: binary
  def serialize_entries(entries) when is_list(entries) do
    :erlang.term_to_binary(entries)
  end

  @doc """
  Deserializes an entry list from cold storage.

  Reverses the serialization done by `serialize_entries/1`.

  ## Examples

      iex> entries = [{"a", 1}, {"b", 2}]
      iex> binary = BfTree.Persistence.Serializer.serialize_entries(entries)
      iex> restored = BfTree.Persistence.Serializer.deserialize_entries(binary)
      iex> restored
      [{"a", 1}, {"b", 2}]
  """
  @spec deserialize_entries(binary) :: list({BfTree.key(), BfTree.value()})
  def deserialize_entries(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  end
end
