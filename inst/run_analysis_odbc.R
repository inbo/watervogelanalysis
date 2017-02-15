library(watervogelanalysis)
result.channel <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)
raw.connection <- connect_raw(
  result.channel = result.channel
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

library(aws.s3)
prepare_analysis(
  analysis.path = get_bucket("n2kmonitoring"),
  raw.connection = raw.connection
)
