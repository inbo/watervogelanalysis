# prepare the analysis objects and a bash script the run the analyses in docker
library(watervogelanalysis)
if (Sys.getenv("AWS_ACCESS_KEY_ID") == "") {
  Sys.setenv("AWS_ACCESS_KEY_ID" = keyring::key_get("n2kmonitoring-key"))
}
if (Sys.getenv("AWS_SECRET_ACCESS_KEY") == "") {
  Sys.setenv("AWS_SECRET_ACCESS_KEY" = keyring::key_get("n2kmonitoring-secret"))
}
if (Sys.getenv("AWS_DEFAULT_REGION") == "") {
  Sys.setenv("AWS_DEFAULT_REGION" =  keyring::key_get("n2kmonitoring-region"))
}
keyring::key_get("n2kmonitoring-bucket") |>
  aws.s3::get_bucket(prefix = "watervogels", max = 1) -> analysis_path
keyring::key_get("meetnetten", username = "watervogels_repo") |>
  git2r::repository() |>
  prepare_analysis(
    analysis_path = analysis_path, seed = 19790402, verbose = TRUE
  ) -> manifest
library(n2kanalysis)
manifest_yaml_to_bash(
  base = analysis_path, project = "watervogels", shutdown = TRUE
) |>
  sprintf(
    fmt = c(
      "", "cd ~", "export $(cat .env | xargs)",
      "aws s3 cp s3://%2$s/%1$s watervogels.sh", "chmod 711 watervogels.sh",
      ""
    ) |>
      paste(collapse = "\n"),
    keyring::key_get("n2kmonitoring-bucket")
  ) |>
  cat(sep = "\n")

manifest_yaml_to_bash(
  base = analysis_path, project = "watervogels", shutdown = TRUE,
  status = c("new", "waiting", "error"), timeout = 8
) |>
  sprintf(
    fmt = c(
      "", "cd ~", "export $(cat .env | xargs)",
      "aws s3 cp s3://%2$s/%1$s watervogels.sh", "chmod 711 watervogels.sh", ""
    ) |>
      paste(collapse = "\n"),
    keyring::key_get("n2kmonitoring-bucket")
  ) |>
  cat(sep = "\n")

manifest_yaml_to_bash(
  base = analysis_path, project = "watervogels", shutdown = TRUE,
  status = "error", timeout = 8
) |>
  sprintf(
    fmt = c(
      "", "cd ~", "export $(cat .env | xargs)",
      "aws s3 cp s3://%2$s/%1$s watervogels.sh", "chmod 711 watervogels.sh",
      ""
    ) |>
      paste(collapse = "\n"),
    keyring::key_get("n2kmonitoring-bucket")
  ) |>
  cat(sep = "\n")


# run the analyses in R
library(n2kanalysis)
Sys.setenv("AWS_ACCESS_KEY_ID" = keyring::key_get("n2kmonitoring-key"))
Sys.setenv("AWS_SECRET_ACCESS_KEY" = keyring::key_get("n2kmonitoring-secret"))
Sys.setenv("AWS_DEFAULT_REGION" =  keyring::key_get("n2kmonitoring-region"))
keyring::key_get("n2kmonitoring-bucket") |>
  aws.s3::get_bucket(prefix = "watervogels", max = 1) -> base
aws_objects <- keyring::key_get("n2kmonitoring-bucket") |>
  aws.s3::get_bucket_df(prefix = "watervogels/manifest", max = Inf)
as.POSIXct(aws_objects$LastModified, format = "%Y-%m-%dT%H:%M:%S.0") |>
  which.max() -> most_recent
aws_objects[most_recent, "Key"] |>
  basename() |>
  fit_model(
    base = base, project = "watervogels", status = c("new", "waiting", "error")
  )

# extract the results
library(watervogelanalysis)
if (Sys.getenv("AWS_ACCESS_KEY_ID") == "") {
  Sys.setenv("AWS_ACCESS_KEY_ID" = keyring::key_get("n2kmonitoring-key"))
}
if (Sys.getenv("AWS_SECRET_ACCESS_KEY") == "") {
  Sys.setenv("AWS_SECRET_ACCESS_KEY" = keyring::key_get("n2kmonitoring-secret"))
}
if (Sys.getenv("AWS_DEFAULT_REGION") == "") {
  Sys.setenv("AWS_DEFAULT_REGION" =  keyring::key_get("n2kmonitoring-region"))
}
keyring::key_get("n2kmonitoring-bucket") |>
  aws.s3::get_bucket(prefix = "watervogels/manifest", max = 1) -> base
keyring::key_get("meetnetten", username = "watervogels_repo") |>
  git2r::repository() -> raw_data
keyring::key_get("meetnetten", username = "watervogels_result") |>
  git2r::repository() -> root
project <- "watervogels"
read_manifest(base = base, project = project) |>
  slot("Fingerprint") |>
  extract_results(
    base = base, project = project, raw_data = raw_data, root = root
  )
