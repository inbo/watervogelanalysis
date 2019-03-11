library(watervogelanalysis)
result_channel <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)
duplicates <- import_walloon_source_data(
  location_file = "locations_waterbirds_winter_count_aves.txt",
  visit_file = "visits_winter_waterbirds_count_walbru_aves.txt",
  species_file = "species_winter_waterbirds_count_walbru_aves.txt",
  data_file = "data_winter_waterbirds_count_walbru_aves.txt",
  path = "~/Downloads",
  walloon_repo = git2r::repository("~/n2k/waterbirds_wallonia"),
  strict = FALSE
)
duplicates
