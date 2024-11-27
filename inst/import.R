sc <- c(
  driver = "ODBC Driver 18 for SQL Server", server = "inbo-sql08-prd.inbo.be",
  database = "W0004_00_Waterbirds", port = 1433, uid = "W0004_Reader",
  pwd = keyring::key_get("meetnetten", username = "W0004_Reader"),
  Encrypt = "no"
)
sprintf("%s=%s;", names(sc), sc) |>
  paste(collapse = "") -> constring
flemish_channel <- odbc::dbConnect(odbc::odbc(), .connection_string = constring)
keyring::key_get("meetnetten", username = "walloon_repo") |>
  git2r::repository() -> walloon_repo
keyring::key_get("meetnetten", username = "raw_repo") |>
  git2r::repository() -> raw_repo
library(watervogelanalysis)
prepare_dataset(
  flemish_channel = flemish_channel, walloon_repo = walloon_repo,
  raw_repo = raw_repo, strict = FALSE
)
