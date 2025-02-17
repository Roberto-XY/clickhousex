defmodule Clickhousex.HTTPRequest do
  @type t :: %__MODULE__{post_data: IO.chardata(), query_string_data: IO.chardata()}

  # @enforce_keys [:post_data, :query_string_data]
  defstruct post_data: "", query_string_data: ""

  def new() do
    %__MODULE__{}
  end

  def with_post_data(%__MODULE__{} = request, post_data) do
    %{request | post_data: post_data}
  end

  def with_query_string_data(%__MODULE__{} = request, query_string_data) do
    %{request | query_string_data: query_string_data}
  end
end
