library(watervogelanalysis)
prepare_analysis(
  analysis_path = aws.s3::get_bucket("n2kmonitoring", prefix = "watervogels",
                                     max = 1),
  raw_repo = git2r::repository(fs::path("~", "n2k", "watervogels")),
  verbose = TRUE
)
