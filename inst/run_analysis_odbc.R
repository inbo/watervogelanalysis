library(watervogelanalysis)
result.channel <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)
raw.connection = n2khelper::git_connection(
  repo.path = "~/n2k/ssh/rawdata", #nolint
  local.path = "watervogel",
  key = "~/.ssh/id_rsa_n2k", #nolint
  commit.user = "watervogelanalysis",
  commit.email = "bmk@inbo.be"
)

prepare_dataset(
  scheme.id = scheme_id(result.channel = result.channel),
  raw.connection = raw.connection,
  result.channel = result.channel,
  attribute.connection = connect_attribute(result.channel = result.channel),
  walloon.connection = connect_walloon_source(result.channel = result.channel),
  flemish.channel = connect_flemish_source(result.channel = result.channel),
  nbn.channel = watervogelanalysis::connect_nbn(result.channel = result.channel),
  develop = as.logical(Sys.getenv("N2KRESULT_DEVELOP", unset = FALSE))
)

prepare_analysis(
  analysis.path = aws.s3::get_bucket("n2kmonitoring"),
  raw.connection = raw.connection
)
