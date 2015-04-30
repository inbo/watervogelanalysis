library(watervogelanalysis)
result.channel <- n2khelper::connect_result()
scheme.id <- scheme_id(result.channel = result.channel)
raw.connection <- connect_raw(
  result.channel = result.channel,
  username = username,
  password = password
)

prepare_dataset(
  scheme.id = scheme.id,
  raw.connection = raw.connection,
  result.channel = result.channel,
  attribute.connection = connect_attribute(
    result.channel = result.channel,
    username = username,
    password = password
  ),
  walloon.connection = connect_walloon_source(
    result.channel = result.channel,
    username = username,
    password = password
  ),
  flemish.channel = connect_flemish_source(result.channel = result.channel)
)
prepare_analysis(
  analysis.path = "~/analysis", 
  raw.connection = raw.connection, 
  scheme.id = scheme.id
)
