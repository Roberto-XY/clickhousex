defmodule Clickhousex.Codec.Binary do
  @compile {:bin_opt_info, true}
  use Bitwise

  alias Clickhousex.Type

  # def encode(:varint, num) when num < 128, do: <<num>>
  # def encode(:varint, num), do: <<1::1, num::7, encode(:varint, num >>> 7)::binary>>

  # def encode(:string, str) when is_bitstring(str) do
  #   [encode(:varint, byte_size(str)), str]
  # end

  # def encode(:u8, i) when is_integer(i) do
  #   <<i::little-unsigned-size(8)>>
  # end

  # def encode(:u16, i) do
  #   <<i::little-unsigned-size(16)>>
  # end

  # def encode(:u32, i) do
  #   <<i::little-unsigned-size(32)>>
  # end

  # def encode(:u64, i) do
  #   <<i::little-unsigned-size(64)>>
  # end

  # def encode(:i8, i) do
  #   <<i::little-signed-size(8)>>
  # end

  # def encode(:i16, i) do
  #   <<i::little-signed-size(16)>>
  # end

  # def encode(:i32, i) do
  #   <<i::little-signed-size(32)>>
  # end

  # def encode(:i64, i) do
  #   <<i::little-signed-size(64)>>
  # end

  # def encode(:boolean, true) do
  #   encode(:u8, 1)
  # end

  # def encode(:boolean, false) do
  #   encode(:u8, 0)
  # end

  def decode(bytes, %_{nullable: true} = type) when is_binary(bytes) do
    case bytes do
      <<0, rest::binary>> -> decode(rest, %{type | nullable: false})
      <<1, rest::binary>> -> {:ok, nil, rest}
    end
  end

  # def decode(bytes, :struct, struct_module) do
  #   decode_struct(bytes, struct_module.decode_spec(), struct(struct_module))
  # end

  def decode(bytes, %Type.String{}) do
    with {:ok, byte_count, rest} <- decode_varint(bytes),
         true <- byte_size(rest) >= byte_count do
      <<decoded_str::binary-size(byte_count), rest::binary>> = rest
      {:ok, decoded_str, rest}
    end
  end

  def decode(bytes, %Type.FixedString{length: length})
      when is_binary(bytes) and is_integer(length) do
    <<decoded_str::binary-size(length), rest::binary>> = bytes
    {:ok, decoded_str, rest}
  end

  def decode(<<decoded::little-signed-size(64), rest::binary>>, %Type.Int64{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(32), rest::binary>>, %Type.Int32{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(16), rest::binary>>, %Type.Int16{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(8), rest::binary>>, %Type.Int8{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(64), rest::binary>>, %Type.UInt64{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(32), rest::binary>>, %Type.UInt32{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(16), rest::binary>>, %Type.UInt16{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(8), rest::binary>>, %Type.UInt8{}) do
    {:ok, decoded, rest}
  end

  def decode(<<days_since_epoch::little-unsigned-size(16), rest::binary>>, %Type.Date{}) do
    {:ok, date} = Date.new(1970, 01, 01)
    date = Date.add(date, days_since_epoch)

    {:ok, date, rest}
  end

  def decode(<<seconds_since_epoch::little-unsigned-size(32), rest::binary>>, %Type.DateTime{}) do
    {:ok, date_time} = NaiveDateTime.new(1970, 1, 1, 0, 0, 0)
    date_time = NaiveDateTime.add(date_time, seconds_since_epoch)

    {:ok, date_time, rest}
  end

  def decode(<<decoded::little-signed-float-size(64), rest::binary>>, %Type.Float64{}) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-float-size(32), rest::binary>>, %Type.Float32{}) do
    {:ok, decoded, rest}
  end

  def decode(binary, %Type.Tuple{element_types: element_types})
      when is_binary(binary) and is_list(element_types) do
    decode_tuple(binary, element_types)
  end

  def decode(binary, %Type.Array{element_type: element_type}) when is_binary(binary) do
    decode_list(binary, element_type)
  end

  def decode_varint(bytes, result \\ 0, shift \\ 0)

  def decode_varint(<<0::size(1), byte::size(7), rest::binary>>, result, shift) do
    {:ok, result ||| byte <<< shift, rest}
  end

  def decode_varint(<<1::1, byte::7, rest::binary>>, result, shift) do
    decode_varint(rest, result ||| byte <<< shift, shift + 7)
  end

  defp decode_list(binary, data_type) when is_binary(binary) do
    {:ok, count, rest} = decode_varint(binary)
    decode_list(rest, data_type, count, [])
  end

  defp decode_list(rest, _, 0, acc) when is_list(acc) do
    {:ok, Enum.reverse(acc), rest}
  end

  defp decode_list(binary, data_type, count, acc) do
    case decode(binary, data_type) do
      {:ok, decoded, rest} -> decode_list(rest, data_type, count - 1, [decoded | acc])
      other -> other
    end
  end

  defp decode_tuple(binary, types, acc \\ [])

  defp decode_tuple(binary, [], acc) when is_list(acc) and is_binary(binary) do
    {:ok, Enum.reverse(acc) |> List.to_tuple(), binary}
  end

  defp decode_tuple(binary, [type | types], acc) when is_binary(binary) do
    case decode(binary, type) do
      {:ok, decoded, rest} -> decode_tuple(rest, types, [decoded | acc])
      other -> other
    end
  end

  # defp decode_struct(rest, [], struct) do
  #   {:ok, struct, rest}
  # end

  # defp decode_struct(rest, [{field_name, type} | specs], struct) do
  #   case decode(rest, type) do
  #     {:ok, decoded, rest} ->
  #       decode_struct(rest, specs, Map.put(struct, field_name, decoded))

  #     {:error, _} = err ->
  #       err
  #   end
  # end
end
