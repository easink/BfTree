defmodule BfTree.Tree do
  @moduledoc """
  High-level tree operations that handle node splits and consolidation.

  This module provides the main insertion, deletion, and search operations
  on the B+tree structure, abstracting away the complexity of node splitting.
  """

  alias BfTree.Node

  @type key :: term
  @type value :: term
  @type t :: Node.t()

  @doc """
  Searches for a key in the tree.

  Returns `{:ok, value}` if found, `:not_found` otherwise.

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "key", "value")
      iex> BfTree.Tree.search(tree, "key")
      {:ok, "value"}
  """
  @spec search(t(), key) :: {:ok, value} | :not_found
  def search(tree, key) do
    Node.search(tree, key)
  end

  @doc """
  Inserts a key-value pair into the tree, handling any necessary node splits.

  Returns `{:ok, new_tree}` with updated tree after insertion.

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "a", 1)
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "b", 2)
      iex> {:ok, value} = BfTree.Tree.search(tree, "b")
      iex> value
      2
  """
  @spec insert(t(), key, value) :: {:ok, t()}
  def insert(tree, key, value) do
    case Node.insert(tree, key, value) do
      {:ok, new_tree} ->
        {:ok, new_tree}

      {:split, left_node, right_node, separator} ->
        # When root splits, create new root
        new_root = Node.new_internal([separator], [left_node, right_node])
        {:ok, new_root}
    end
  end

  @doc """
  Deletes a key from the tree.

  Returns `{:ok, new_tree}` if deletion successful, `:not_found` if key doesn't exist.

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "key", "value")
      iex> {:ok, tree} = BfTree.Tree.delete(tree, "key")
      iex> BfTree.Tree.search(tree, "key")
      :not_found
  """
  @spec delete(t(), key) :: {:ok, t()} | :not_found
  def delete(tree, key) do
    case Node.delete(tree, key) do
      {:ok, new_tree} -> {:ok, new_tree}
      :not_found -> :not_found
    end
  end

  @doc """
  Returns all keys in the tree in sorted order.

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "b", 1)
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "a", 2)
      iex> BfTree.Tree.keys(tree)
      ["a", "b"]
  """
  @spec keys(t()) :: list(key)
  def keys(tree) do
    Node.keys(tree)
  end

  @doc """
  Returns all values in the tree (in sorted key order).

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "a", 1)
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "b", 2)
      iex> BfTree.Tree.values(tree)
      [1, 2]
  """
  @spec values(t()) :: list(value)
  def values(tree) do
    tree
    |> keys()
    |> Enum.map(fn key ->
      {:ok, value} = search(tree, key)
      value
    end)
  end

  @doc """
  Returns range of key-value pairs within given bounds (inclusive).

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "a", 1)
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "b", 2)
      iex> {:ok, tree} = BfTree.Tree.insert(tree, "c", 3)
      iex> BfTree.Tree.range(tree, "a", "b")
      [{"a", 1}, {"b", 2}]
  """
  @spec range(t(), key, key) :: list({key, value})
  def range(tree, min_key, max_key) do
    Node.range(tree, min_key, max_key)
  end

  @doc """
  Returns the number of entries in the tree.

  ## Examples

      iex> tree = BfTree.Node.new_leaf()
      iex> BfTree.Tree.size(tree)
      0

      iex> {:ok, tree} = BfTree.Tree.insert(BfTree.Node.new_leaf(), "key", "value")
      iex> BfTree.Tree.size(tree)
      1
  """
  @spec size(t()) :: non_neg_integer
  def size(tree) do
    Node.size(tree)
  end
end
