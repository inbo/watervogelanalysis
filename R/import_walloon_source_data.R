#' Add the raw data from Wallonia to the git repository
#'
#' This functions reads the files and performs some basic checks on them.
#' See the details section for the required format of the files.
#' The median date is used in case of multiple dates per visit id.
#' The maximum is used in case of multiple observations per visit id.
#' @param location_file file with details on the location
#' @param visit_file file with details on the visits to each location
#' @param data_file file with observed species at each visit
#' @param species_file file with all species
#' @param path directory were the above files are stored
#' @inheritParams git2rdata::write_vc
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that is.string is.dir noNA
#' @importFrom utils file_test read.table
#' @importFrom dplyr %>% add_count anti_join arrange count filter group_by
#' inner_join mutate_at select semi_join summarise ungroup
#' @importFrom rlang !! .data
#' @importFrom git2rdata write_vc commit
#' @importFrom stats median
#' @importFrom purrr map_chr map2_chr
#' @importFrom digest sha1
import_walloon_source_data <- function(
  location_file, visit_file, species_file, data_file, path = ".", walloon_repo,
  strict = TRUE
) {
  assert_that(is.string(location_file), is.string(visit_file),
              is.string(species_file), is.string(data_file), is.dir(path))
  location_file <- file.path(path, location_file)
  visit_file <- file.path(path, visit_file)
  species_file <- file.path(path, species_file)
  data_file <- file.path(path, data_file)
  assert_that(file_test("-f", location_file), file_test("-f", visit_file),
              file_test("-f", data_file))

  location <- read.table(location_file, header = TRUE, sep = "\t",
                         stringsAsFactors = FALSE, fileEncoding = "Latin1")
  old_names <- c(
    LocationID = "CODE", LocationName = "NOM", SPA = "N2000",
    Latitude = "X", Longitude = "Y", Start = "StartDate", End = "EndDate"
  )
  assert_that(all(old_names %in% colnames(location)))
  select(location, !!old_names) %>%
    mutate_at(c("Start", "End"), as.Date) -> location
  if (anyDuplicated(location$LocationID)) {
    warning("duplicate id in ", location_file)
  }

  visit <- read.table(visit_file, header = TRUE, sep = "\t",
                      stringsAsFactors = FALSE, fileEncoding = "Latin1")
  old_names <- c(
    ObservationID = "id_visit", LocationID = "CODE_ULT", Date = "date"
  )
  assert_that(all(old_names %in% colnames(visit)))
  select(visit, !!old_names) %>%
    mutate_at("Date", as.Date) -> visit
  if (anyDuplicated(visit$ObservationID)) {
    warning("duplicate id in ", visit_file)
    visit %>%
      add_count(.data$ObservationID) %>%
      arrange(.data$ObservationID, .data$Date) %>%
      select("ObservationID", "LocationID", "Date") -> visit_duplicate
    visit %>%
      group_by(.data$ObservationID, .data$LocationID) %>%
      summarise(Date = median(.data$Date)) %>%
      ungroup() -> visit
  } else {
    visit_duplicate <- FALSE
  }
  visit %>%
    anti_join(location, by = "LocationID") %>%
    nrow() -> test
  if (test > 0) {
    stop(visit_file, " contains id which is not in ", location_file)
  }
  location %>%
    anti_join(visit, by = "LocationID") %>%
    nrow() -> test
  if (test > 0) {
    warning(location_file, " contains id which is not in ", visit_file)
    location %>%
      semi_join(visit, by = "LocationID") -> location
  }

  species <- read.table(species_file, header = TRUE, sep = "\t",
                        stringsAsFactors = FALSE, fileEncoding = "Latin1")
  old_names <- c(ScientificName = "species", euringcode = "code_euring")
  assert_that(all(old_names %in% colnames(species)))
  species <- select(species, !!old_names)
  if (anyNA(species$euringcode)) {
    species %>%
      filter(is.na(.data$euringcode)) -> species_nomatch
    warning(
      "Species without euringcode will be ignored:\n",
      paste(species_nomatch$ScientificName, collapse = "\n")
    )
    species %>%
      filter(!is.na(.data$euringcode)) -> species
  } else {
    species_nomatch <- NA
  }
  if (anyDuplicated(species$euringcode)) {
    stop("duplicated euringcodes")
  }

  data <- read.table(data_file, header = TRUE, sep = "\t",
                     stringsAsFactors = FALSE, fileEncoding = "Latin1")
  old_names <- c(
    ObservationID = "id_visit", Species = "species", Count = "n"
  )
  assert_that(all(old_names %in% colnames(data)))
  data <- select(data, !!old_names)
  if (!all(data$ObservationID %in% visit$ObservationID)) {
    stop(data_file, " contains id which is not in ", visit_file)
  }
  if (anyDuplicated(select(data, "ObservationID", "Species"))) {
    warning(
"Species with multiple counts per visit. Only the highest values is retained"
    )
    data %>%
      count(.data$ObservationID, .data$Species) %>%
      filter(.data$n > 1) -> data_duplicate
    data %>%
      group_by(.data$ObservationID, .data$Species) %>%
      summarise(Count = max(.data$Count)) %>%
      ungroup() -> data
  } else {
    data_duplicate <- NA
  }

  if (any(is.na(location$LocationName))) {
    warning("Locations without location names are ignored")
    location <- filter(location, !is.na(.data$LocationName))
  }
  assert_that(noNA(location$LocationID), noNA(location$SPA))
  visit %>%
    filter(
      as.integer(format(.data$Date, "%m")) %in% c(11, 12, 1, 2),
      as.Date("1991-10-01") <= .data$Date
    ) %>%
    mutate(
      OriginalObservationID = .data$ObservationID,
      ObservationID = map_chr(.data$OriginalObservationID, sha1)
    ) -> visit
  data %>%
    semi_join(visit, by = c("ObservationID" = "OriginalObservationID")) %>%
    inner_join(species, by = c("Species" = "ScientificName")) %>%
    select(OriginalObservationID = "ObservationID", "euringcode", "Count") %>%
    mutate(ObservationID = map2_chr(.data$OriginalObservationID,
                                    .data$euringcode, ~sha1(list(.x, .y)))
    ) %>%
    write_vc(file = "data", sorting = c("OriginalObservationID", "euringcode"),
             stage = TRUE, root = walloon_repo, strict = strict)
  write_vc(species, file = "species", sorting = "euringcode", stage = TRUE,
           root = walloon_repo, strict = strict)
  write_vc(location, file = "location", sorting = "LocationID", stage = TRUE,
           root = walloon_repo, strict = strict)
  write_vc(visit, file = "visit", sorting = "ObservationID", stage = TRUE,
           root = walloon_repo, strict = strict)
  tryCatch(
    commit(repo = walloon_repo, session = TRUE,
            message = "scripted commit from watervogelanalysis"),
    error = function(e) {
      NULL
    }
  )

  return(list(DuplicateVisit = visit_duplicate, DuplicateData = data_duplicate,
    UnmatchedSpecies = species_nomatch))
}
