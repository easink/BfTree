defmodule BfTree.Buffer do
  @moduledoc """
  Write buffer for batching insertions before consolidation into the hot tier.

  The buffer accumulates write operations and maintains them as a sorted list
  of {key, value} pairs. When the buffer reaches a size threshold, it's
  consolidated into the B+tree hot tier.

  This design reduces the number of tree rebalancing operations and improves
  write throughput significantly.
  """

  @type key :: term
  @type value :: term
  @type entry :: {key, value}

  @type t :: %{
          entries: list(entry),
          tombstones: MapSet.t(key)
        }

  @doc """
  Creates a new empty write buffer.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> BfTree.Buffer.size(buffer)
      0
  """
  @spec new() :: t()
  def new do
    %{
      entries: [],
      tombstones: MapSet.new()
    }
  end

  @doc """
  Inserts or updates a key-value pair in the buffer.

  Returns a new buffer with the entry added/updated.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "key1", "value1")
      iex> BfTree.Buffer.size(buffer)
      1

      iex> buffer = BfTree.Buffer.new() |> BfTree.Buffer.put("key1", "value1") |> BfTree.Buffer.put("key1", "updated")
      iex> BfTree.Buffer.size(buffer)
      1
  """
  @spec put(t(), key, value) :: t()
  def put(buffer, key, value) do
    # Remove tombstone if present
    new_tombstones = MapSet.delete(buffer.tombstones, key)

    # Remove existing entry if key exists
    new_entries = Enum.reject(buffer.entries, fn {k, _} -> k == key end)

    # Add new entry and sort
    new_entries = Enum.sort([{key, value} | new_entries])

    %{buffer | entries: new_entries, tombstones: new_tombstones}
  end

  @doc """
  Retrieves a value from the buffer.

  Returns `{:ok, value}` if found, `:not_found` otherwise.
  Respects tombstones (deleted entries).

  ## Examples

      iex> buffer = BfTree.Buffer.new() |> BfTree.Buffer.put("key1", "value1")
      iex> BfTree.Buffer.get(buffer, "key1")
      {:ok, "value1"}

      iex> BfTree.Buffer.get(BfTree.Buffer.new(), "missing")
      :not_found
  """
  @spec get(t(), key) :: {:ok, value} | :not_found
  def get(buffer, key) do
    if MapSet.member?(buffer.tombstones, key) do
      :not_found
    else
      case Enum.find(buffer.entries, fn {k, _} -> k == key end) do
        {^key, value} -> {:ok, value}
        nil -> :not_found
      end
    end
  end

  @doc """
  Marks a key as deleted (tombstone).

  Returns a new buffer with the key marked as deleted.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "key1", "value1")
      iex> buffer = BfTree.Buffer.delete(buffer, "key1")
      iex> BfTree.Buffer.get(buffer, "key1")
      :not_found
  """
  @spec delete(t(), key) :: t()
  def delete(buffer, key) do
    new_entries = Enum.reject(buffer.entries, fn {k, _} -> k == key end)
    new_tombstones = MapSet.put(buffer.tombstones, key)

    %{buffer | entries: new_entries, tombstones: new_tombstones}
  end

  @doc """
  Returns the number of live entries in the buffer (excluding tombstones).

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> BfTree.Buffer.size(buffer)
      0

      iex> BfTree.Buffer.new() |> BfTree.Buffer.put("key1", "value1") |> BfTree.Buffer.size()
      1
  """
  @spec size(t()) :: non_neg_integer
  def size(buffer) do
    length(buffer.entries)
  end

  @doc """
  Returns all keys in the buffer, sorted.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "b", 1)
      iex> buffer = BfTree.Buffer.put(buffer, "a", 2)
      iex> BfTree.Buffer.keys(buffer)
      ["a", "b"]
  """
  @spec keys(t()) :: list(key)
  def keys(buffer) do
    buffer.entries |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end

  @doc """
  Returns range of key-value pairs within given bounds (inclusive).

  Respects tombstones by excluding deleted keys.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "a", 1)
      iex> buffer = BfTree.Buffer.put(buffer, "b", 2)
      iex> buffer = BfTree.Buffer.put(buffer, "c", 3)
      iex> BfTree.Buffer.range(buffer, "a", "b")
      [{"a", 1}, {"b", 2}]
  """
  @spec range(t(), key, key) :: list({key, value})
  def range(buffer, min_key, max_key) do
    buffer.entries
    |> Enum.filter(fn {k, _} ->
      k >= min_key and k <= max_key and not MapSet.member?(buffer.tombstones, k)
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  @doc """
  Folds over buffer entries with an accumulator function.

  Iterates through all live entries (excluding tombstones) and applies function.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "a", 1)
      iex> buffer = BfTree.Buffer.put(buffer, "b", 2)
      iex> BfTree.Buffer.fold(buffer, 0, fn {_k, v}, acc -> acc + v end)
      3
  """
  @spec fold(t(), term, (entry, term -> term)) :: term
  def fold(buffer, acc, fun) do
    Enum.reduce(buffer.entries, acc, fn entry, acc ->
      case entry do
        {key, _value} ->
          if MapSet.member?(buffer.tombstones, key) do
            acc
          else
            fun.(entry, acc)
          end

        _ ->
          acc
      end
    end)
  end

  @doc """
  Returns all live entries in the buffer as a list of {key, value} tuples.

  ## Examples

      iex> buffer = BfTree.Buffer.new()
      iex> buffer = BfTree.Buffer.put(buffer, "a", 1)
      iex> buffer = BfTree.Buffer.put(buffer, "b", 2)
      iex> BfTree.Buffer.to_list(buffer)
      [{"a", 1}, {"b", 2}]
  """
  @spec to_list(t()) :: list(entry)
  def to_list(buffer) do
    Enum.reject(buffer.entries, fn {k, _} ->
      MapSet.member?(buffer.tombstones, k)
    end)
  end
end
