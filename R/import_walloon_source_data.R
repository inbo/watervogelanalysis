#' Add the raw data from Wallonia to the git repository
#'
#' This functions reads the files and performs some basic checks on them. See the details section for the required format of the files. The median date is used in case of multiple dates per visit id. The maximum is used in case of multiple observations per visit id.
#'
#' All files must textfile with ';' separated fields and '.' to indicate decimal points
#'
#' \code{location.file} must have the following fields:
#' \itemize{
#'   \item{\code{CODE_ULT} The id of the location}
#'   \item{\code{Nom_site} The name of the location}
#'   \item{\code{ZPS} 1 if the location is a Special Procection Area, otherwise 0}
#'   \item{\code{Area} The surface area of the location in square m}
#'   \item{\code{Xlamb} The X coordinate of the location, Belgium Lambert 72 system}
#'   \item{\code{Ylamb} The Y coordinate of the location, Belgium Lambert 72 system}
#' }
#'
#' \code{visit.file} must have the following fields:
#' \itemize{
#'   \item{\code{ID_visit} The is of the visit to the location}
#'   \item{\code{CODE_ULT} The id of the location}
#'   \item{\code{DATE} The date of the visit to the location}
#' }
#'
#' \code{visit_file} must have the following fields:
#' \itemize{
#'   \item{\code{ID_visit} The is of the visit to the location}
#'   \item{\code{TAXPRIO} The scientific name of the species. Only exactly matches from \code{\link{read_specieslist}} with \code{limit = FALSE} are retained. Non-matching values are displayed in a warning.}
#'   \item{\code{SommeDeN} The observed number of animals}
#' }
#' @param location_file file with details on the location
#' @param visit_file file with details on the visits to each location
#' @param data_file file with observed species at each visit
#' @param path directory were the above files are stored
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that
#' @importFrom utils file_test read.csv
#' @importFrom dplyr select distinct %>% inner_join
#' @importFrom rlang !! .data
#' @importFrom git2rdata write_vc auto_commit
#' @importFrom digest digest
#' @importFrom stats median aggregate
import_walloon_source_data <- function(
  location_file, visit_file, data_file, path = ".", walloon_repo, nbn_channel
){
  assert_that(
    is.string(location_file),
    is.string(visit_file),
    is.string(data_file),
    is.string(path),
    file_test("-d", path)
  )
  location_file <- file.path(path, location_file)
  visit_file <- file.path(path, visit_file)
  data_file <- file.path(path, data_file)
  assert_that(
    file_test("-f", location_file),
    file_test("-f", visit_file),
    file_test("-f", data_file)
  )

  location <- read.csv(location_file, stringsAsFactors = FALSE)
  old_names <- c(
    LocationID = "CODE_ULT", LocationName = "Nom_site", SPA = "ZPS",
    Area = "Area", XBelgiumLambert72 = "Xlamb", YBelgiumLambert72 = "Ylamb"
  )
  assert_that(all(old_names %in% colnames(location)))
  location <- select(location, !!old_names)
  if (anyDuplicated(location$LocationID)) {
    warning("duplicate id in ", location.file)
  }
  write_vc(
    location, file = "location.txt", sorting = "LocationID", stage = TRUE,
    root = walloon_repo
  )

  old_names <- c(
    ObservationID = "ID_visit", LocationID = "CODE_ULT", Date = "DATE"
  )
  visit <- read.csv(visit_file, stringsAsFactors = FALSE)
  assert_that(all(old_names %in% colnames(visit)))
  visit <- select(visit, !!old_names)
  visit$Date <- as.Date(visit$Date, format = "%d/%m/%Y")

  if (anyDuplicated(visit$ObservationID)) {
    warning("duplicate id in ", visit_file)
    duplicate_id <- names(which(table(visit$ObservationID) > 1))
    visit_duplicate <- visit[visit$ObservationID %in% duplicate_id, ]
    visit <- aggregate(
      visit[, "Date", drop = FALSE],
      visit[, c("ObservationID", "LocationID")],
      FUN = median
    )
  } else {
    visit_duplicate <- FALSE
  }
  if (!all(visit$LocationID %in% location$LocationID)) {
    stop(visit_file, " contains id which is not in ", location_file)
  }
  write_vc(
    visit, file = "visit.txt", sorting = "ObservationID", stage = TRUE,
    root = walloon_repo
  )

  old_names <- c(
    ObservationID = "ID_visit", Species = "TAXPRIO", Count = "SommeDeN"
  )
  data <- read.csv(data_file, stringsAsFactors = FALSE)
  assert_that(all(old_names %in% colnames(data)))
  data <- select(data, !!old_names)
  if (!all(data$ObservationID %in% visit$ObservationID)) {
    stop(data_file, " contains id which is not in ", visit_file)
  }
  if (any(table(data$ObservationID, data$Species) > 1)) {
    warning(
"Species with multiple counts per visit. Only the highest values is retained"
    )
    n_obs <- aggregate(
      data[, "Count"],
      data[, c("ObservationID", "Species")],
      FUN = length
    )
    data_duplicate <- merge(
      n_obs[n_obs$x > 1, c("ObservationID", "Species")],
      data
    )
    data <- aggregate(
      data[, "Count", drop = FALSE],
      data[, c("ObservationID", "Species")],
      FUN = max
    )
  } else {
    data_duplicate <- NA
  }
  data %>%
    distinct(ScientificName = .data$Species) %>%
    get_nbn_key_multi(orders = "la", nbn_channel) -> species
  if (anyNA(species$NBNKey)) {
    species_nomatch <- species$ScientificName[is.na(species$NBNKey)]
    warning(
      "Unmatched species will be ignored:\n",
      paste(species_nomatch, collapse = "\n")
    )
    species <- species[!is.na(species$NBNKey), ]
  } else {
    species_nomatch <- NA
  }
  write_vc(
    species, file = "species.txt", sorting = "NBNKey", stage = TRUE,
    root = walloon_repo
  )

  data %>%
    inner_join(species, by = c("Species" = "ScientificName")) %>%
    select("ObservationID", "NBNKey", "Count") %>%
    write_vc(
      file = "data.txt", sorting = c("ObservationID", "NBNKey"), stage = TRUE,
      root = walloon_repo
    )

  auto_commit(package = "watervogelanalysis", repo = walloon_repo)
  return(list(
    DuplicateVisit = visit_duplicate,
    DuplicateData = data_duplicate,
    UnmatchedSpecies = species_nomatch
  ))
}
