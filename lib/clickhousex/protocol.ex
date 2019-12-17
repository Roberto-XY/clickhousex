defmodule Clickhousex.Protocol do
  #   @moduledoc false
  #   use DBConnection

  #   alias Clickhousex.HTTPClient, as: Client
  #   alias Clickhousex.Error

  #   defstruct conn_opts: [], base_address: ""

  #   @type state :: %__MODULE__{
  #           conn_opts: Keyword.t(),
  #           base_address: String.t()
  #         }

  #   @type query :: Clickhousex.Query.t()
  #   @type result :: Clickhousex.Result.t()
  #   @type cursor :: any

  #   @ping_query Clickhousex.Query.new("SELECT 1") |> DBConnection.Query.parse([])
  #   @ping_params DBConnection.Query.encode(@ping_query, [], [])

  #   @doc false
  #   @impl true
  #   @spec connect(opts :: Keyword.t()) :: {:ok, state} | {:error, Exception.t()}
  #   def connect(opts) do
  #     scheme = opts[:scheme] || :https
  #     hostname = opts[:hostname] || "localhost"
  #     port = opts[:port] || 8123
  #     database = opts[:database] || "default"
  #     username = opts[:username] || nil
  #     password = opts[:password] || nil
  #     timeout = opts[:timeout] || Clickhousex.timeout()

  #     base_address = build_base_address(scheme, hostname, port)

  #     case Client.send(
  #            @ping_query,
  #            @ping_params,
  #            base_address,
  #            timeout,
  #            username,
  #            password,
  #            database
  #          ) do
  #       {:selected, _, _} ->
  #         {
  #           :ok,
  #           %__MODULE__{
  #             conn_opts: [
  #               scheme: scheme,
  #               hostname: hostname,
  #               port: port,
  #               database: database,
  #               username: username,
  #               password: password,
  #               timeout: timeout
  #             ],
  #             base_address: base_address
  #           }
  #         }

  #       resp ->
  #         resp
  #     end
  #   end

  #   @doc false
  #   @impl true
  #   @spec checkout(state) :: {:ok, state}
  #   def checkout(state) do
  #     {:ok, state}
  #   end

  #   @doc false
  #   @impl true
  #   @spec checkin(state) :: {:ok, state}
  #   def checkin(state) do
  #     {:ok, state}
  #   end

  #   @doc false
  #   @impl true
  #   @spec ping(state) :: {:ok, state} | {:disconnect, term, state}
  #   def ping(state) do
  #     case do_query(@ping_query, @ping_params, [], state) do
  #       {:ok, _, _, new_state} -> {:ok, new_state}
  #       {:error, reason, new_state} -> {:disconnect, reason, new_state}
  #     end
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_begin(opts :: Keyword.t(), state) :: no_return
  #   def handle_begin(_opts, state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_commit(opts :: Keyword.t(), state) :: no_return
  #   def handle_commit(_opts, state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_rollback(opts :: Keyword.t(), state) :: no_return
  #   def handle_rollback(_opts, state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_status(opts :: Keyword.t(), state) :: {:idle, state}
  #   def handle_status(_, state) do
  #     {:idle, state}
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_prepare(query, Keyword.t(), state) :: {:ok, query, state}
  #   def handle_prepare(query, _, state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_execute(query, list, opts :: Keyword.t(), state) ::
  #           {:ok, result, state}
  #           | {:error | :disconnect, Exception.t(), state}
  #   def handle_execute(query, params, opts, state) do
  #     do_query(query, params, opts, state)
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_close(query, Keyword.t(), state) :: {:ok, result, state}
  #   def handle_close(_query, _opts, _state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_declare(query, any, Keyword.t(), state) :: no_return
  #   def handle_declare(_query, _params, _opts, _state) do
  #     raise "Unsupported"
  #   end

  #   @impl true
  #   @spec handle_fetch(query, cursor, Keyword.t(), state) :: no_return
  #   def handle_fetch(_query, _cursor, _opts, _state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec handle_deallocate(query, cursor, Keyword.t(), state) :: no_return
  #   def handle_deallocate(_query, _cursor, _opts, _state) do
  #     raise "Unsupported"
  #   end

  #   @doc false
  #   @impl true
  #   @spec disconnect(err :: Exception.t(), state) :: :ok
  #   def disconnect(_err, _state) do
  #     # TODO: accumulate query ids in state and cancel them here
  #     :ok
  #   end

  #   @doc false
  #   @spec reconnect(new_opts :: Keyword.t(), state) :: {:ok, state}
  #   def reconnect(new_opts, state) do
  #     with :ok <- disconnect("Reconnecting", state),
  #          do: connect(new_opts)
  #   end

  #   defp do_query(query, params, _opts, state) do
  #     base_address = state.base_address
  #     username = state.conn_opts[:username]
  #     password = state.conn_opts[:password]
  #     timeout = state.conn_opts[:timeout]
  #     database = state.conn_opts[:database]

  #     res =
  #       query
  #       |> Client.send(params, base_address, timeout, username, password, database)
  #       |> handle_errors()

  #     case res do
  #       {:error, %Error{code: :connection_exception} = reason} ->
  #         {:disconnect, reason, state}

  #       {:error, reason} ->
  #         {:error, reason, state}

  #       {:selected, columns, rows} ->
  #         {
  #           :ok,
  #           query,
  #           %Clickhousex.Result{
  #             command: :selected,
  #             columns: columns,
  #             rows: rows,
  #             num_rows: Enum.count(rows)
  #           },
  #           state
  #         }

  #       {:updated, count} ->
  #         {
  #           :ok,
  #           query,
  #           %Clickhousex.Result{
  #             command: :updated,
  #             columns: ["count"],
  #             rows: [[count]],
  #             num_rows: 1
  #           },
  #           state
  #         }

  #       {command, columns, rows} ->
  #         {
  #           :ok,
  #           query,
  #           %Clickhousex.Result{
  #             command: command,
  #             columns: columns,
  #             rows: rows,
  #             num_rows: Enum.count(rows)
  #           },
  #           state
  #         }
  #     end
  #   end

  #   @doc false
  #   defp handle_errors({:error, reason}), do: {:error, Error.exception(reason)}
  #   defp handle_errors(term), do: term

  #   @doc false
  #   @spec handle_info(opts :: Keyword.t(), state) :: {:ok, result, state}
  #   def handle_info(_msg, state) do
  #     {:ok, state}
  #   end

  #   ## Private functions

  #   defp build_base_address(scheme, hostname, port) do
  #     "#{Atom.to_string(scheme)}://#{hostname}:#{port}/"
  #   end

  alias Clickhousex.Codec.Binary
  alias Clickhousex.Type

  def decode_server_msg(binary) when is_binary(binary) do
    case Binary.decode_varint(binary) do
      {:ok, 2, tail} -> decode_exception(tail) |> IO.inspect(label: "EXECPTION")
      x -> IO.inspect(x, label: "RESPONSE")
    end
  end

  def decode_exception(binary) when is_binary(binary) do
    IO.inspect(binary, label: "RAW")
    {:ok, code, tail} = Binary.decode(binary, %Type.UInt32{})
    {:ok, name, tail} = Binary.decode(tail, %Type.String{})
    {:ok, message, tail} = Binary.decode(tail, %Type.String{})
    {:ok, stack_trace, tail} = Binary.decode(tail, %Type.String{})
    {:ok, has_nested, tail} = Binary.decode(tail, %Type.UInt8{})

    nested = if has_nested do decode_exception(tail)

    {code, name, message, stack_trace, has_nested, nested, tail}
  end
end
