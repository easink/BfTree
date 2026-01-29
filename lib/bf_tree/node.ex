defmodule BfTree.Node do
  @moduledoc """
  B+tree node representation and operations.

  Nodes can be either:
  - **Leaf nodes**: Contain {key, value} pairs (terminal data)
  - **Internal nodes**: Contain separators and child pointers

  All nodes maintain sorted key invariants for efficient searching and range queries.
  """

  @type key :: term
  @type value :: term
  @type child_ptr :: non_neg_integer | Node.t()

  @type leaf_node :: %{
          type: :leaf,
          entries: list({key, value}),
          id: pos_integer
        }

  @type internal_node :: %{
          type: :internal,
          separators: list(key),
          children: list(Node.t()),
          id: pos_integer
        }

  @type t :: leaf_node | internal_node

  # Node ID generator (simple counter in real implementation)
  @doc false
  def next_id, do: :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned()

  @doc """
  Creates a new empty leaf node.

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> node.type
      :leaf
  """
  @spec new_leaf() :: leaf_node
  def new_leaf do
    %{
      type: :leaf,
      entries: [],
      id: next_id()
    }
  end

  @doc """
  Creates a new internal node with given separators and children.

  ## Examples

      iex> child1 = BfTree.Node.new_leaf()
      iex> child2 = BfTree.Node.new_leaf()
      iex> node = BfTree.Node.new_internal(["m"], [child1, child2])
      iex> node.type
      :internal
  """
  @spec new_internal(list(key), list(t())) :: internal_node
  def new_internal(separators, children) when is_list(separators) and is_list(children) do
    %{
      type: :internal,
      separators: separators,
      children: children,
      id: next_id()
    }
  end

  @doc """
  Searches for a key in the node and its descendants.

  Returns `{:ok, value}` if found, `:not_found` otherwise.

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> {:ok, node} = BfTree.Node.insert(node, "key", "value")
      iex> BfTree.Node.search(node, "key")
      {:ok, "value"}

      iex> BfTree.Node.search(BfTree.Node.new_leaf(), "missing")
      :not_found
  """
  @spec search(t(), key) :: {:ok, value} | :not_found
  def search(%{type: :leaf, entries: entries}, key) do
    case Enum.find(entries, fn {k, _} -> k == key end) do
      {^key, value} -> {:ok, value}
      nil -> :not_found
    end
  end

  def search(%{type: :internal, separators: seps, children: children}, key) do
    child_index = find_child_index(key, seps)
    child = Enum.at(children, child_index)
    search(child, key)
  end

  @doc """
  Inserts a key-value pair into the node.

  Returns `{:ok, new_node}` or `{:ok, new_node1, new_node2}` if split occurred.

  For pure functional insert, returns either:
  - `{:ok, updated_node}` - if insert fit without splitting
  - `{:split, left_node, right_node, separator_key}` - if node split needed

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> {:ok, node} = BfTree.Node.insert(node, "a", 1)
      iex> {:ok, node} = BfTree.Node.insert(node, "b", 2)
      iex> BfTree.Node.search(node, "b")
      {:ok, 2}
  """
  @spec insert(t(), key, value) :: {:ok, t()} | {:split, t(), t(), key}
  def insert(%{type: :leaf, entries: entries} = node, key, value) do
    # Remove existing entry if key exists (update)
    new_entries = Enum.reject(entries, fn {k, _} -> k == key end)
    # Add new entry and sort
    new_entries = Enum.sort([{key, value} | new_entries])

    # Check if needs split (simple heuristic: max 32 entries per node)
    if length(new_entries) > 32 do
      split_leaf(new_entries)
    else
      {:ok, %{node | entries: new_entries}}
    end
  end

  def insert(%{type: :internal, separators: seps, children: children} = node, key, value) do
    child_index = find_child_index(key, seps)
    child = Enum.at(children, child_index)

    case insert(child, key, value) do
      {:ok, new_child} ->
        new_children = List.replace_at(children, child_index, new_child)
        {:ok, %{node | children: new_children}}

      {:split, left_child, right_child, separator} ->
        new_children = List.delete_at(children, child_index)
        new_children = List.insert_at(new_children, child_index, left_child)
        new_children = List.insert_at(new_children, child_index + 1, right_child)

        new_seps = List.insert_at(seps, child_index, separator)

        # Check if this node needs to split
        if length(new_seps) > 32 do
          split_internal(new_seps, new_children)
        else
          {:ok, %{node | separators: new_seps, children: new_children}}
        end
    end
  end

  @doc """
  Deletes a key from the node.

  Returns `{:ok, new_node}` or `:not_found` if key doesn't exist.

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> {:ok, node} = BfTree.Node.insert(node, "key", "value")
      iex> {:ok, node} = BfTree.Node.delete(node, "key")
      iex> BfTree.Node.search(node, "key")
      :not_found
  """
  @spec delete(t(), key) :: {:ok, t()} | :not_found
  def delete(%{type: :leaf, entries: entries} = node, key) do
    case Enum.find_index(entries, fn {k, _} -> k == key end) do
      nil -> :not_found
      idx -> {:ok, %{node | entries: List.delete_at(entries, idx)}}
    end
  end

  def delete(%{type: :internal, separators: seps, children: children} = node, key) do
    child_index = find_child_index(key, seps)
    child = Enum.at(children, child_index)

    case delete(child, key) do
      {:ok, new_child} ->
        new_children = List.replace_at(children, child_index, new_child)
        {:ok, %{node | children: new_children}}

      :not_found ->
        :not_found
    end
  end

  @doc """
  Returns all keys in the node and its descendants, sorted.

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> {:ok, node} = BfTree.Node.insert(node, "b", 1)
      iex> {:ok, node} = BfTree.Node.insert(node, "a", 2)
      iex> BfTree.Node.keys(node)
      ["a", "b"]
  """
  @spec keys(t()) :: list(key)
  def keys(%{type: :leaf, entries: entries}) do
    entries |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end

  def keys(%{type: :internal, children: children}) do
    children
    |> Enum.flat_map(&keys/1)
    |> Enum.sort()
  end

  @doc """
  Returns range of key-value pairs within given bounds (inclusive).

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> {:ok, node} = BfTree.Node.insert(node, "a", 1)
      iex> {:ok, node} = BfTree.Node.insert(node, "b", 2)
      iex> {:ok, node} = BfTree.Node.insert(node, "c", 3)
      iex> BfTree.Node.range(node, "a", "b")
      [{\"a\", 1}, {\"b\", 2}]
  """
  @spec range(t(), key, key) :: list({key, value})
  def range(%{type: :leaf, entries: entries}, min_key, max_key) do
    entries
    |> Enum.filter(fn {k, _} -> k >= min_key and k <= max_key end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  def range(%{type: :internal, children: children}, min_key, max_key) do
    children
    |> Enum.flat_map(&range(&1, min_key, max_key))
    |> Enum.sort_by(&elem(&1, 0))
  end

  @doc """
  Returns the number of entries in the node and descendants.

  ## Examples

      iex> node = BfTree.Node.new_leaf()
      iex> BfTree.Node.size(node)
      0

      iex> {:ok, node} = BfTree.Node.insert(BfTree.Node.new_leaf(), "key", "value")
      iex> BfTree.Node.size(node)
      1
  """
  @spec size(t()) :: non_neg_integer
  def size(%{type: :leaf, entries: entries}) do
    length(entries)
  end

  def size(%{type: :internal, children: children}) do
    Enum.sum(Enum.map(children, &size/1))
  end

  # Private helpers

  defp find_child_index(key, separators) do
    Enum.count(separators, fn sep -> key >= sep end)
  end

  defp split_leaf(entries) do
    mid = div(length(entries), 2)
    left_entries = Enum.slice(entries, 0, mid)
    right_entries = Enum.slice(entries, mid..-1//1)

    separator = hd(right_entries) |> elem(0)

    left_node = %{type: :leaf, entries: left_entries, id: next_id()}
    right_node = %{type: :leaf, entries: right_entries, id: next_id()}

    {:split, left_node, right_node, separator}
  end

  defp split_internal(separators, children) do
    mid = div(length(separators), 2)
    left_seps = Enum.slice(separators, 0, mid)
    right_seps = Enum.slice(separators, (mid + 1)..-1//1)
    separator = Enum.at(separators, mid)

    left_children = Enum.slice(children, 0, mid + 1)
    right_children = Enum.slice(children, (mid + 1)..-1//1)

    left_node = %{type: :internal, separators: left_seps, children: left_children, id: next_id()}

    right_node = %{
      type: :internal,
      separators: right_seps,
      children: right_children,
      id: next_id()
    }

    {:split, left_node, right_node, separator}
  end
end
