library(digest)
library(git2rdata)
library(lubridate)
library(tidyverse)
source_folder <- keyring::key_get("meetnetten", username = "walloon_download")
keyring::key_get("meetnetten", username = "walloon_repo") |>
  repository() -> target

# import species
file.path(source_folder, "winter_waterbirds_species_wallonia_brussels.csv") |>
  read.csv2(fileEncoding = "Latin1") |>
  select("euring", scientific = "taxprio", "french_name") |>
  filter(.data$scientific != "no_species") -> species
species |>
  count(.data$scientific) |>
  filter(.data$n > 1) -> duplicate_species
stopifnot(nrow(duplicate_species) == 0)
data.frame(
  euring = c(
    1869L, 1619L, 1580L, 1630L, 1560L, 1574L, 5610L, 1680L, 1663L, 1690L, 1110L,
    4970L, 5120L, 5100L, 4690L, 4700L, 1340L, 1540L, 1190L, 4500L, 6000L, 5750L,
    5340L, 5320L, 2150L, 2130L, 2250L, 5170L, 1440L, 4860L, 100L, 4560L, 5450L,
    5480L, 5460L
  ),
  scientific = c(
    "Anas platyrhynchos forma domestica", "Anser anser forma domesticus",
    "Anser brachyrhynchus", "Anser caerulescens",
    "Anser cygnoides forma domestica", "Anser fabalis rossicus",
    "Arenaria interpres", "Branta bernicla",
    "Branta hutchinsii", "Branta ruficollis", "Bubulcus ibis",
    "Calidris alba", "Calidris alpina", "Calidris maritima",
    "Charadrius dubius", "Charadrius hiaticula", "Ciconia ciconia",
    "Cygnus cygnus", "Egretta garzetta", "Haematopus ostralegus",
    "Larus marinus", "Larus melanocephalus", "Limosa lapponica",
    "Limosa limosa", "Melanitta fusca", "Melanitta nigra", "Oxyura jamaicensis",
    "Philomachus pugnax", "Platalea leucorodia", "Pluvialis squatarola",
    "Podiceps grisegena", "Recurvirostra avosetta", "Tringa erythropus",
    "Tringa nebularia", "Tringa totanus"
  )
) -> extra
species |>
  filter(is.na(.data$euring)) |>
  select(-"euring") |>
  left_join(extra, by = "scientific") |>
  bind_rows(
    species |>
      filter(!is.na(.data$euring))
  ) -> species
write_vc(
  species, file = "species", root = target, sorting = c("euring", "scientific")
)
update_metadata(
  "species", root = target, name = "species",
  title = "Species list of the Wallonia waterbirds dataset",
  field_description = c(
    euring = "Euring code https://euring.org/data-and-codes/euring-codes",
    scientific = "Scientific name", french_name = "French name"
  )
)

# import sites
file.path(source_folder, "winter_waterbirds_sites_wallonia_brussels.csv") |>
  read.csv2(fileEncoding = "Latin1") |>
  transmute(
    id = .data$code_site, name = .data$nom_site,
    natura2000 = as.logical(.data$natura2000)
  ) |>
  arrange(.data$id) -> all_sites
all_sites |>
  slice_head(n = 1, by = "id") -> sites
write_vc(sites, file = "location", root = target, sorting = "id")
update_metadata(
  "location", root = target, name = "sites",
  title = "Sites list of the Wallonia waterbirds dataset",
  field_description = c(
    id = "Internal code of the site",
    name = "name of the site",
    natura2000 = "Indicates if the site is a Natura 2000 site"
  )
)
all_sites |>
  anti_join(sites, by = "name") |>
  semi_join(x = all_sites, by = "id") |>
  write_vc(
    "problems/duplicate_site", root = target, optimize = FALSE,
    sorting = c("id", "name")
  )

# import visits
file.path(source_folder, "winter_waterbirds_counts_wallonia_brussels.csv") |>
  read.csv2(fileEncoding = "Latin1") |>
  select(
    site = "code_site", scientific = "taxprio", "euring", "n", "date",
    visit_id = "visite"
  ) |>
  mutate(date = as.Date(.data$date)) -> observations
observations |>
  anti_join(sites, by = c("site" = "id")) -> unknown_sites
stopifnot(nrow(unknown_sites) == 0)
observations |>
  anti_join(species, by = "scientific") |>
  filter(.data$scientific != "no_species") -> unknown_species
stopifnot(nrow(unknown_species) == 0)
observations |>
  distinct(.data$scientific, .data$euring) |>
  count(.data$scientific) |>
  filter(.data$n > 1) -> duplicate_species
stopifnot(nrow(duplicate_species) == 0)
observations |>
  distinct(.data$visit_id, .data$date, .data$site) -> visits
visits |>
  count(.data$site, .data$date) |>
  filter(.data$n > 1) |>
  semi_join(x = observations, by = c("site", "date")) |>
  write_vc(
    "problems/duplicate_visit", root = target, optimize = FALSE,
    sorting = c("site", "date", "scientific", "visit_id", "n"), strict = FALSE
  )
visits |>
  distinct(.data$site, .data$date) |>
  mutate(
    start = format(.data$date, "%Y-%m-01") |>
      as.Date(),
    end = .data$start + months(1) - days(1),
    midpoint = difftime(.data$end, .data$start, units = "days") / 2 +
      .data$start,
    delta = difftime(.data$date, .data$midpoint, units = "days") |>
      as.integer() |>
      abs()
  ) |>
  slice_min(.data$delta, n = 1, with_ties = FALSE, by = "site") |>
  transmute(
    hash = map2_chr(.data$site, .data$date, ~sha1(c(site = .x, date = .y))) |>
      str_trunc(width = 7, ellipsis = ""),
    site = factor(.data$site, levels = sites$id), .data$date
  ) -> relevant_visits
visits |>
  anti_join(relevant_visits, by = c("site", "date")) |>
  write_vc(
    "problems/unused_visit", root = target, optimize = FALSE,
    sorting = c("site", "date", "visit_id"), strict = FALSE
  )
write_vc(relevant_visits, file = "visit", root = target, sorting = "hash")
update_metadata(
  "visit", root = target, name = "visits",
  title = "Visits list of the Wallonia waterbirds dataset",
  field_description = c(
    hash = "Unique identifier of the visit", site = "Internal code of the site",
    date = "Date of the visit"
  )
)

# store relevant observations
observations |>
  filter(.data$scientific != "no_species", .data$n > 0) |>
  inner_join(relevant_visits, by = c("site", "date")) |>
  group_by(
    visit = factor(.data$hash, levels = relevant_visits$hash),
    species = factor(.data$scientific, levels = species$scientific)
  ) |>
  summarise(n = sum(.data$n), .groups = "drop") |>
  write_vc(file = "data", root = target, sorting = c("visit", "species"))
update_metadata(
  "data", root = target, name = "Observations",
  title = "Observations of the Wallonia waterbirds dataset",
  field_description = c(
    visit = "Unique identifier of the visit",
    species = "Scientific name of the species", n = "Observed number of species"
  )
)
commit(
  repo = target, message = "Import Wallonia waterbirds dataset", session = TRUE,
  all = TRUE
)
