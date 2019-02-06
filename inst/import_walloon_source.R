library(watervogelanalysis)
result.channel <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)
duplicates <- import_walloon_source_data(
  location_file = "rStation.csv",
  visit_file = "rVisits.csv",
  data_file = "rData.csv",
  path = "~/n2k/waterbirds_wallonia",
  walloon_repo = git2r::repository("~/n2k/waterbirds_wallonia"),
  nbn_channel = connect_nbn(result.channel = result.channel)
)
duplicates
