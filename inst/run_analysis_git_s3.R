library(watervogelanalysis)
prepare_analysis(
  analysis_path = aws.s3::get_bucket(
    bucket = Sys.getenv("N2KBUCKET"), prefix = "watervogels", max = 1
  ),
  raw_repo = git2r::repository(fs::path("~", "n2k", "watervogels")),
  seed = 19790402, verbose = TRUE, knot_interval = 10
) -> script
