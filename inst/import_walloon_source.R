watervogelanalysis::import_walloon_source_data(
  species_file = "winter_waterbirds_species_wallonia_brussels.csv",
  data_file = "winter_waterbirds_counts_wallonia_brussels.csv",
  location_file = "winter_waterbirds_sites_wallonia_brussels.csv",
  path = keyring::key_get("meetnetten", username = "walloon_download"),
  walloon_repo = keyring::key_get("meetnetten", username = "walloon_repo") |>
    git2rdata::repository()
)
