#' Add the raw data from Wallonia to the git repository
#'
#' This functions reads the files and performs some basic checks on them.
#' See the details section for the required format of the files.
#' The median date is used in case of multiple dates per visit id.
#' The maximum is used in case of multiple observations per visit id.
#' @param location_file file with details on the location
#' @param data_file file with observed species at each visit
#' @param species_file file with all species
#' @param path directory were the above files are stored
#' @inheritParams git2rdata::write_vc
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that is.string is.dir noNA
#' @importFrom digest sha1
#' @importFrom dplyr anti_join arrange bind_rows count distinct filter group_by
#' inner_join left_join mutate select semi_join slice_head slice_min transmute
#' @importFrom git2rdata commit update_metadata write_vc
#' @importFrom lubridate days
#' @importFrom purrr map2_chr
#' @importFrom rlang .data
#' @importFrom utils file_test
import_walloon_source_data <- function(
  location_file, species_file, data_file, path = ".", walloon_repo,
  strict = TRUE
) {
  assert_that(
    is.string(location_file), is.string(species_file), is.string(data_file),
    noNA(location_file), noNA(species_file), noNA(data_file), is.dir(path)
  )
  location_file <- file.path(path, location_file)
  species_file <- file.path(path, species_file)
  data_file <- file.path(path, data_file)
  assert_that(
    file_test("-f", location_file), file_test("-f", species_file),
    file_test("-f", data_file)
  )

  # import species
  read.csv2(species_file, fileEncoding = "Latin1") |>
    select("euring", scientific = "taxprio", "french_name") |>
    filter(.data$scientific != "no_species") -> species
  species |>
    count(.data$scientific) |>
    filter(.data$n > 1) -> duplicate_species
  stopifnot(nrow(duplicate_species) == 0)
  data.frame(
    euring = c(
      1869L, 1619L, 1580L, 1630L, 1560L, 1574L, 5610L, 1680L, 1663L, 1690L,
      1110L, 4970L, 5120L, 5100L, 4690L, 4700L, 1340L, 1540L, 1190L, 4500L,
      6000L, 5750L, 5340L, 5320L, 2150L, 2130L, 2250L, 5170L, 1440L, 4860L,
      100L, 4560L, 5450L, 5480L, 5460L
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
      "Limosa limosa", "Melanitta fusca", "Melanitta nigra",
      "Oxyura jamaicensis", "Philomachus pugnax", "Platalea leucorodia",
      "Pluvialis squatarola", "Podiceps grisegena", "Recurvirostra avosetta",
      "Tringa erythropus", "Tringa nebularia", "Tringa totanus"
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
    species, file = "species", root = walloon_repo,
    sorting = c("euring", "scientific"), strict = strict
  )
  update_metadata(
    "species", root = walloon_repo, name = "species",
    title = "Species list of the Wallonia waterbirds dataset",
    field_description = c(
      euring = "Euring code https://euring.org/data-and-codes/euring-codes",
      scientific = "Scientific name", french_name = "French name"
    )
  )

  # import sites
  file.path(path, location_file) |>
    read.csv2(fileEncoding = "Latin1") |>
    transmute(
      id = .data$code_site, name = .data$nom_site,
      natura2000 = as.logical(.data$natura2000)
    ) |>
    arrange(.data$id) -> all_sites
  all_sites |>
    slice_head(n = 1, by = "id") -> sites
  write_vc(sites, file = "location", root = walloon_repo, sorting = "id")
  update_metadata(
    "location", root = walloon_repo, name = "sites",
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
      "problems/duplicate_site", root = walloon_repo, optimize = FALSE,
      sorting = c("id", "name"), strict = strict
    )

  # import visits
  file.path(path, data_file) |>
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
      "problems/duplicate_visit", root = walloon_repo, optimize = FALSE,
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
        substr(start = 1, stop = 7),
      site = factor(.data$site, levels = sites$id), .data$date
    ) -> relevant_visits
  visits |>
    anti_join(relevant_visits, by = c("site", "date")) |>
    write_vc(
      "problems/unused_visit", root = walloon_repo, optimize = FALSE,
      sorting = c("site", "date", "visit_id"), strict = strict
    )
  write_vc(
    relevant_visits, file = "visit", root = walloon_repo, sorting = "hash",
    strict = strict
  )
  update_metadata(
    "visit", root = walloon_repo, name = "visits",
    title = "Visits list of the Wallonia waterbirds dataset",
    field_description = c(
      hash = "Unique identifier of the visit",
      site = "Internal code of the site", date = "Date of the visit"
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
    write_vc(
      file = "data", root = walloon_repo, sorting = c("visit", "species"),
      strict = strict
    )
  update_metadata(
    "data", root = walloon_repo, name = "Observations",
    title = "Observations of the Wallonia waterbirds dataset",
    field_description = c(
      visit = "Unique identifier of the visit",
      species = "Scientific name of the species",
      n = "Observed number of species"
    )
  )
  commit(
    repo = walloon_repo, message = "Import Wallonia waterbirds dataset",
    session = TRUE, all = TRUE
  )
  return(invisible(NULL))
}
