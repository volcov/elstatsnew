defmodule Elstasnew.Repo do
  use Ecto.Repo,
    otp_app: :elstasnew,
    adapter: Ecto.Adapters.Postgres
end
