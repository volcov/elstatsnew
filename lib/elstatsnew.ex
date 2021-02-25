defmodule Elstatsnew do
  @moduledoc """
  Elstatsnew keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """
  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [module: {Elstatsnew.Producer, opts}, concurrency: 1],
      processors: [default: [concurrency: 50]],
      batchers: [default: [batch_size: 20, batch_timeout: 2000]]
    )
  end

  @impl true
  def handle_message(_, message, _) do
    message
    |> Message.update_data(fn data -> String.upcase(data) end)
  end

  @impl true
  def handle_batch(_batch_name, messages, _, _) do
    list = Enum.map(messages, fn e -> e.data end)

    IO.inspect(list, label: "Got batch")

    messages
  end
end
