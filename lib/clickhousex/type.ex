defmodule Clickhousex.Type do
  alias Clickhousex.Type

  @type t ::
          Type.UInt8.t()
          | Type.UInt16.t()
          | Type.UInt32.t()
          | Type.UInt64.t()
          | Type.Int8.t()
          | Type.Int16.t()
          | Type.Int32.t()
          | Type.Int64.t()
          | Type.Float32.t()
          | Type.Float64.t()
          | Type.String.t()
          | Type.UUID.t()
          | Type.Date.t()
          | Type.DateTime.t()
          | Type.Array.t()
          | Type.FixedString.t()
          | Type.Tuple.t()

  defmodule UInt8 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule UInt16 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule UInt32 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule UInt64 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Int8 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Int16 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Int32 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Int64 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Float32 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Float64 do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule String do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule FixedString do
    @type t :: %__MODULE__{nullable: boolean, length: non_neg_integer}

    @enforce_keys [:nullable, :length]
    defstruct [:nullable, :length]
  end

  defmodule UUID do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Date do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule DateTime do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Array do
    @type t :: %__MODULE__{element_type: Type.t()}

    @enforce_keys [:element_type]
    defstruct [:element_type]
  end

  defmodule Tuple do
    @type t :: %__MODULE__{element_types: [Type.t()]}

    @enforce_keys [:element_types]
    defstruct [:element_types]
  end

  @spec parse(binary) :: Type.t()
  def parse(binary) when is_binary(binary) do
    # with {:ok, {type, ""}} <- parse(binary, false) do
    #   {:ok, type}
    # end
    {:ok, {type, ""}} = parse(binary, false)
    type
  end

  @spec parse(binary, nullable :: boolean) :: {:ok, {Type.t(), binary}}
  defp parse(<<"Array(", tail::binary>>, false) do
    with {:ok, {type, child_tail}} <- parse(tail, false) do
      case child_tail do
        <<")", next::binary>> -> {:ok, {%Type.Array{element_type: type}, next}}
        unexpected_tail when is_binary(unexpected_tail) -> {:error, unexpected_tail}
      end
    end
  end

  defp parse(<<"Tuple", tail::binary>>, false) do
    with {:ok, {types, tail}} when is_list(types) and is_binary(tail) <- parse_list(tail) do
      {:ok, {%Type.Tuple{element_types: types}, tail}}
    end
  end

  defp parse(<<"Nullable(", tail::binary>>, nullable) when is_boolean(nullable) do
    with {:ok, {type, child_tail}} <- parse(tail, true) do
      case child_tail do
        <<")", next::binary>> -> {:ok, {type, next}}
        unexpected_tail when is_binary(unexpected_tail) -> {:error, unexpected_tail}
      end
    end
  end

  defp parse(<<"FixedString(", tail::binary>>, nullable) when is_boolean(nullable) do
    with {:ok, {number, child_tail}} when is_integer(number) <- parse_number(tail) do
      case child_tail do
        <<")", next::binary>> ->
          {:ok, {%Type.FixedString{length: number, nullable: nullable}, next}}

        unexpected_tail when is_binary(unexpected_tail) ->
          {:error, unexpected_tail}
      end
    end
  end

  defp parse(<<"UInt8", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.UInt8{nullable: nullable}, tail}}

  defp parse(<<"UInt16", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.UInt16{nullable: nullable}, tail}}

  defp parse(<<"Int16", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.Int16{nullable: nullable}, tail}}

  defp parse(<<"Int32", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.Int32{nullable: nullable}, tail}}

  defp parse(<<"Float64", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.Float64{nullable: nullable}, tail}}

  defp parse(<<"String", tail::binary>>, nullable) when is_boolean(nullable),
    do: {:ok, {%Type.String{nullable: nullable}, tail}}

  defp parse(unexpected_tail, nullable)
       when is_binary(unexpected_tail) and is_boolean(nullable),
       do: {:error, unexpected_tail}

  @spec parse_number(binary, binary) ::
          {:ok, {integer, binary}} | {:error, unexpected_tail :: binary}
  defp parse_number(binary, acc \\ "")
       when is_binary(binary) do
    case binary do
      <<")", tail::binary>> ->
        {:ok, {Elixir.String.to_integer(IO.chardata_to_string(acc)), <<")", tail::binary>>}}

      <<x::utf8, tail::binary>> ->
        parse_number(tail, [<<x::utf8>>, acc])

      unexpected_tail ->
        {:error, unexpected_tail}
    end
  end

  @spec parse_list(binary, [Type.t()]) ::
          {:ok, {[Type.t()], binary}} | {:error, unexpected_tail :: binary}
  defp parse_list(tail, acc \\ [])

  defp parse_list(<<")", tail::binary>>, acc) when is_list(acc) and length(acc) > 0 do
    {:ok, {Enum.reverse(acc), tail}}
  end

  defp parse_list(<<", ", tail::binary>>, acc) when is_list(acc) do
    with {:ok, {type, child_tail}} <- parse(tail, false) do
      parse_list(child_tail, [type | acc])
    end
  end

  defp parse_list(<<"()", tail::binary>>, []) do
    {:ok, {[], tail}}
  end

  defp parse_list(<<"(", tail::binary>>, []) do
    with {:ok, {type, child_tail}} <- parse(tail, false) do
      parse_list(child_tail, [type])
    end
  end

  defp parse_list(unexpected_tail, _) when is_binary(unexpected_tail),
    do: {:error, unexpected_tail}

  @types [
    Type.UInt8,
    Type.UInt16,
    Type.UInt32,
    Type.UInt64,
    Type.Int8,
    Type.Int16,
    Type.Int32,
    Type.Int64,
    Type.Float32,
    Type.Float64,
    Type.String,
    Type.FixedString,
    Type.UUID,
    Type.Date,
    Type.DateTime
  ]

  @doc """

  """
  def nullable?(type)

  Enum.each(@types, fn module ->
    @spec nullable?(unquote(module).t()) :: boolean
    def nullable?(%unquote(module){nullable: nullable}) when is_boolean(nullable), do: nullable
  end)

  @spec nullable?(Type.Array.t()) :: boolean
  def nullable?(%Type.Array{}), do: false

  @spec nullable?(Type.Tuple.t()) :: boolean
  def nullable?(%Type.Tuple{}), do: false

  @doc """

  """
  def to_clickhouse_string(type)

  Enum.each(@types, fn module ->
    @spec to_clickhouse_string(unquote(module).t()) :: String.t()
    def to_clickhouse_string(%unquote(module){nullable: nullable}) when is_boolean(nullable) do
      type_string = Atom.to_string(unquote(module)) |> Elixir.String.split(".") |> List.last()

      if nullable do
        "Nullable(#{type_string})"
      else
        type_string
      end
    end
  end)

  @spec to_clickhouse_string(Type.Array.t()) :: String.t()
  def to_clickhouse_string(%Type.Array{element_type: type}) do
    "Array(#{Type.to_clickhouse_string(type)})"
  end

  @spec to_clickhouse_string(Type.Tuple.t()) :: String.t()
  def to_clickhouse_string(%Type.Tuple{element_types: Type}) do
    types =
      Enum.map(Type, &Type.to_clickhouse_string/1)
      |> Enum.intersperse(",")

    "Tuple(#{types})"
  end
end
