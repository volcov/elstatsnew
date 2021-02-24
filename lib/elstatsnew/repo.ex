defmodule Elstatsnew.Repo do
  use Ecto.Repo,
    otp_app: :elstatsnew,
    adapter: Ecto.Adapters.Postgres
end
