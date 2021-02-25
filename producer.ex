defmodule Elstatsnew.Producer do
  alias Mint.HTTP2
  use GenStage

  @behaviour Broadway.Producer

  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"

  @impl true
  def init(opts) do
    uri = URI.parse(@twitter_stream_url_v2)
    token = Keyword.fetch!(opts, :twitter_bearer_token)

    state =
      connect_to_stream(%{
        token: token,
        uri: uri
      })

    {:producer, state}
  end

  @impl true
  def handle_info({tag, _socket, _data} = message, state) when tag in [:tcp, :ssl] do
    conn = state.conn

    case HTTP2.stream(conn, message) do
      {:ok, conn, resp} ->
        process_responses(resp, %{state | conn: conn})

      {:error, conn, %error{}, _} when error in [Mint.HTTPError, Mint.TransportError] ->
        timer = schedule_connection(@reconnect_in_ms)

        {:noreply, [], %{state | conn: conn, connection_timer: timer}}

      :unknown ->
        {:stop, :stream_stopped_due_unknown_error, state}
    end
  end

  @impl true
  def handle_info(:connect_to_stream, state) do
    {:noreply, [], connect_to_stream(state)}
  end

  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  defp process_responses(responses, state) do
    ref = state.request_ref

    tweets =
      Enum.flat_map(responses, fn response ->
        case response do
          {:data, ^ref, tweet} ->
            decode_tweet(tweet)

          {:done, ^ref} ->
            []
        end
      end)

    {:noreply, tweets, state}
  end

  defp decode_tweet(tweet) do
    case Jason.decode(tweet) do
      {:ok, %{"data" => data}} ->
        meta = Map.delete(data, "text")
        text = Map.fetch!(data, "text")]

        [
          %Message{
            data: text,
            metadata: meta,
            acknowledger: {Broadway.NoopAcknowledger, nil, nil}
          }
        ]

        {:error, _} ->
          IO.puts("error decoding")

          []
    end
  end

  defp connect_to_stream(state) do
    {:ok, conn} = HTTP2.connect(:https, state.uri.host, state.uri.port)

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{state.token}"}],
        nil
      )

    %{state | request_ref: request_ref, conn: conn, connection_timer: nil}
  end

  defp schedule_connection(interval) do
    Process.send_after(self(), :connect_to_stream, interval)
  end
end
