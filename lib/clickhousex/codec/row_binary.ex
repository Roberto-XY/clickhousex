defmodule Clickhousex.Codec.RowBinary do
  alias Clickhousex.{Codec, Codec.Binary, Type}

  @behaviour Codec

  @impl Codec
  def response_format do
    "RowBinaryWithNamesAndTypes"
  end

  @impl Codec
  def request_format do
    "Values"
  end

  @impl Codec
  def encode(query, replacements, params) do
    params =
      Enum.map(params, fn
        %DateTime{} = dt -> DateTime.to_unix(dt)
        other -> other
      end)

    Clickhousex.Codec.Values.encode(query, replacements, params)
  end

  @impl Codec
  def decode(response) when is_binary(response) do
    {:ok, column_count, rest} = Binary.decode(response, :varint)
    decode_metadata(rest, column_count)
  end

  @spec decode_metadata(binary, integer) :: {:ok, map}
  defp decode_metadata(bytes, column_count) when is_binary(bytes) and is_integer(column_count) do
    {:ok, column_names, rest} = decode_column_names(bytes, column_count, [])
    {:ok, column_types, rest} = decode_column_types(rest, column_count, [])

    {:ok, rows} = decode_rows(rest, column_types, [])
    {:ok, %{column_names: column_names, column_types: column_types, rows: rows}}
  end

  @spec decode_column_names(binary, column_count :: integer, [String.t()]) :: {:ok, [String.t()]}
  defp decode_column_names(bytes, 0, names) when is_binary(bytes) and is_list(names) do
    {:ok, Enum.reverse(names), bytes}
  end

  defp decode_column_names(bytes, column_count, names)
       when is_binary(bytes) and is_integer(column_count) and is_list(names) do
    {:ok, column_name, rest} = Binary.decode(bytes, Type.String)
    decode_column_names(rest, column_count - 1, [column_name | names])
  end

  @spec decode_column_types(binary, column_count :: integer, [String.t()]) :: {:ok, [String.t()]}
  defp decode_column_types(bytes, 0, types) when is_binary(bytes) and is_list(types) do
    {:ok, Enum.reverse(types), bytes}
  end

  defp decode_column_types(bytes, column_count, types)
       when is_binary(bytes) and is_integer(column_count) and is_list(types) do
    {:ok, column_type, rest} = Binary.decode(bytes, Type.String)
    decode_column_types(rest, column_count - 1, [Type.parse(column_type) | types])
  end

  @spec decode_rows(binary, [Type.t()], [tuple]) :: {:ok, [tuple]}
  defp decode_rows(<<>>, _, rows) do
    {:ok, Enum.reverse(rows)}
  end

  defp decode_rows(bytes, types, rows)
       when is_binary(bytes) and is_list(types) and is_list(rows) do
    {:ok, row, rest} = decode_row(bytes, types, [])

    decode_rows(rest, types, [row | rows])
  end

  defp decode_row(bytes, [], row) when is_binary(bytes) and is_list(row) do
    row_tuple =
      row
      |> Enum.reverse()
      |> List.to_tuple()

    {:ok, row_tuple, bytes}
  end

  defp decode_row(<<1, rest::binary>>, [%_{nullable: true} | types], row) when is_list(row) do
    decode_row(rest, types, [nil | row])
  end

  defp decode_row(<<0, rest::binary>>, [%_{nullable: true} = type | types], row)
       when is_list(row) do
    decode_row(rest, [type | types], row)
  end

  defp decode_row(bytes, [%Type.Array{element_type: %element_type{}} | types], row)
       when is_binary(bytes) and is_list(row) do
    {:ok, value, rest} = Binary.decode(bytes, {:list, element_type})
    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [%Type.Tuple{element_types: element_types} | types], row)
       when is_binary(bytes) and is_list(element_types) and is_list(row) do
    IO.inspect(element_types)

    {:ok, value, rest} =
      Binary.decode_tuple(bytes, element_types)
      |> IO.inspect()

    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [%type{} | types], row) do
    {:ok, value, rest} = Binary.decode(bytes, type)
    decode_row(rest, types, [value | row])
  end
end
