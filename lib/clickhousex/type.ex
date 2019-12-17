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
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule UInt16 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule UInt32 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule UInt64 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Int8 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Int16 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Int32 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Int64 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Float32 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Float64 do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule String do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule FixedString do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean, length: non_neg_integer}

    @enforce_keys [:length]
    defstruct nullable: false, low_cardinality: false, length: nil
  end

  defmodule UUID do
    @type t :: %__MODULE__{nullable: boolean}

    defstruct nullable: false
  end

  defmodule Date do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule DateTime do
    @type t :: %__MODULE__{nullable: boolean, low_cardinality: boolean}

    defstruct nullable: false, low_cardinality: false
  end

  defmodule Array do
    @type t :: %__MODULE__{element_type: Type.t()}

    @enforce_keys [:element_type]
    defstruct element_type: nil
  end

  defmodule Tuple do
    @type t :: %__MODULE__{element_types: [Type.t()]}

    @enforce_keys [:element_types]
    defstruct element_types: nil
  end

  @spec parse(binary) :: Type.t()
  def parse(binary) when is_binary(binary) do
    # with {:ok, {type, ""}} <- p_parse(binary, false) do
    #   {:ok, type}
    # end
    {:ok, {type, ""}} = p_parse(binary)
    type
  end

  @spec p_parse(binary) :: {:ok, {Type.t(), binary}} | {:error, String.t()}
  defp p_parse(<<"Array(", tail::binary>>) do
    with {:ok, {type, child_tail}} <- p_parse(tail) do
      case child_tail do
        <<")", next::binary>> -> {:ok, {%Type.Array{element_type: type}, next}}
        unexpected_tail -> {:error, unexpected_tail}
      end
    end
  end

  defp p_parse(<<"Tuple", tail::binary>>) do
    with {:ok, {types, tail}} when is_list(types) and is_binary(tail) <- p_parse_list(tail) do
      {:ok, {%Type.Tuple{element_types: types}, tail}}
    end
  end

  # TODO: sinlge case
  defp p_parse(<<"Nullable(", tail::binary>>) do
    case p_parse(tail) do
      {:ok, {%_{nullable: nullable} = type, <<")", next::binary>>}}
      when is_boolean(nullable) ->
        {:ok, {%{type | nullable: true}, next}}

      {:ok, {type, <<")", next::binary>>}} ->
        {:error, "Unsupported Nullable() type. Found #{inspect(type)}"}

      {:ok, {_type, unexpected_tail}} ->
        {:error, unexpected_tail}

      {:error, _} = error ->
        error
    end
  end

  defp p_parse(<<"LowCardinality(", tail::binary>>) do
    case p_parse(tail) do
      {:ok, {%_{low_cardinality: low_cardinality} = type, <<")", next::binary>>}}
      when is_boolean(low_cardinality) ->
        {:ok, {%{type | low_cardinality: true}, next}}

      {:ok, {type, <<")", next::binary>>}} ->
        {:error, "Unsupported LowCardinality() type. Found #{inspect(type)}"}

      {:ok, {_type, unexpected_tail}} ->
        {:error, unexpected_tail}

      {:error, _} = error ->
        error
    end
  end

  defp p_parse(<<"FixedString(", tail::binary>>) do
    with {:ok, {number, child_tail}} when is_integer(number) <- p_parse_number(tail) do
      case child_tail do
        <<")", next::binary>> ->
          {:ok, {%Type.FixedString{length: number}, next}}

        unexpected_tail ->
          {:error, unexpected_tail}
      end
    end
  end

  defp p_parse(<<"UInt8", tail::binary>>),
    do: {:ok, {%Type.UInt8{}, tail}}

  defp p_parse(<<"UInt16", tail::binary>>),
    do: {:ok, {%Type.UInt16{}, tail}}

  defp p_parse(<<"Int16", tail::binary>>),
    do: {:ok, {%Type.Int16{}, tail}}

  defp p_parse(<<"Int32", tail::binary>>),
    do: {:ok, {%Type.Int32{}, tail}}

  defp p_parse(<<"Float64", tail::binary>>),
    do: {:ok, {%Type.Float64{}, tail}}

  defp p_parse(<<"String", tail::binary>>),
    do: {:ok, {%Type.String{}, tail}}

  defp p_parse(unexpected_tail), do: {:error, unexpected_tail}

  @spec p_parse_number(binary, binary) ::
          {:ok, {integer, binary}} | {:error, unexpected_tail :: binary}
  defp p_parse_number(binary, acc \\ "")
       when is_binary(binary) do
    case binary do
      <<")", tail::binary>> ->
        {:ok, {Elixir.String.to_integer(IO.chardata_to_string(acc)), <<")", tail::binary>>}}

      <<x::utf8, tail::binary>> ->
        p_parse_number(tail, [<<x::utf8>>, acc])

      unexpected_tail ->
        {:error, unexpected_tail}
    end
  end

  @spec p_parse_list(binary, [Type.t()]) ::
          {:ok, {[Type.t()], binary}} | {:error, unexpected_tail :: binary}
  defp p_parse_list(tail, acc \\ [])

  defp p_parse_list(<<")", tail::binary>>, acc) when is_list(acc) and length(acc) > 0 do
    {:ok, {Enum.reverse(acc), tail}}
  end

  defp p_parse_list(<<", ", tail::binary>>, acc) when is_list(acc) do
    with {:ok, {type, child_tail}} <- p_parse(tail) do
      p_parse_list(child_tail, [type | acc])
    end
  end

  defp p_parse_list(<<"()", tail::binary>>, []) do
    {:ok, {[], tail}}
  end

  defp p_parse_list(<<"(", tail::binary>>, []) do
    with {:ok, {type, child_tail}} <- p_parse(tail) do
      p_parse_list(child_tail, [type])
    end
  end

  defp p_parse_list(unexpected_tail, _),
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
    def nullable?(%unquote(module){nullable: nullable}), do: nullable
  end)

  @spec nullable?(Type.Array.t()) :: boolean
  def nullable?(%Type.Array{}), do: false

  @spec nullable?(Type.Tuple.t()) :: boolean
  def nullable?(%Type.Tuple{}), do: false
end
