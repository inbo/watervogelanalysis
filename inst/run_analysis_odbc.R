library(watervogelanalysis)
result_channel <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)
raw_repo <- git2r::repository("~/n2k/watervogels")

prepare_dataset(
  scheme_id = scheme_id(result_channel = result_channel),
  raw_repo = raw_repo,
  result_channel = result_channel,
  flemish_channel = connect_flemish_source(result_channel = result_channel),
  walloon_repo = git2r::repository("~/n2k/waterbirds_wallonia"),
  develop = as.logical(Sys.getenv("N2KRESULT_DEVELOP", unset = FALSE))
)

prepare_analysis(
  analysis_path = aws.s3::get_bucket("n2kmonitoring"),
  raw_repo = raw_repo
)
