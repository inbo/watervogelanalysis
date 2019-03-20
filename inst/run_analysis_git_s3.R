library(watervogelanalysis)
manifest <- prepare_analysis(
  analysis_path = aws.s3::get_bucket("n2kmonitoring"),
  raw_repo = git2r::repository("~/n2k/watervogels"),
  verbose = TRUE
)
