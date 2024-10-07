Sys.getenv("N2KBUCKET") |>
  aws.s3::get_bucket(prefix = "watervogels", max = 1) |>
  watervogelanalysis::prepare_analysis(
    raw_repo = git2r::repository(fs::path("~", "n2k", "watervogels")),
    seed = 19790402, verbose = TRUE, knot_interval = 10
  ) |>
  sprintf(
    fmt = c(
      "", "cd ~", "export $(cat .env | xargs)",
      "aws s3 cp s3://%2$s/%1$s watervogels.sh", "chmod 711 watervogels.sh",
      ""
    ) |>
      paste(collapse = "\n"),
    Sys.getenv("N2KBUCKET")
  ) |>
  cat(sep = "\n")
