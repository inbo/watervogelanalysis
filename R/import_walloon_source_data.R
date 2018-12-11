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
#' \code{visit.file} must have the following fields:
#' \itemize{
#'   \item{\code{ID_visit} The is of the visit to the location}
#'   \item{\code{TAXPRIO} The scientific name of the species. Only exactly matches from \code{\link{read_specieslist}} with \code{limit = FALSE} are retained. Non-matching values are displayed in a warning.}
#'   \item{\code{SommeDeN} The observed number of animals}
#' }
#' @param location.file file with details on the location
#' @param visit.file file with details on the visits to each location
#' @param data.file file with observed species at each visit
#' @param path directory were the above files are stored
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper check_path git_connect check_dataframe_variable get_nbn_key_multi
#' @importFrom git2rdata write_vc auto_commit
#' @importFrom digest digest
#' @importFrom stats median aggregate
#' @importFrom utils read.csv2
import_walloon_source_data <- function(
  location.file, visit.file, data.file, path = ".", walloon.connection
){
  path <- check_path(path, type = "directory")
  location.file <- check_path(
    paste(path, location.file, sep = "/"),
    type = "file"
  )
  visit.file <- check_path(
    paste(path, visit.file, sep = "/"),
    type = "file"
  )
  data.file <- check_path(
    paste(path, data.file, sep = "/"),
    type = "file"
  )

  old.names <- c("CODE_ULT", "Nom_site", "ZPS", "Area", "Xlamb", "Ylamb")
  new.names <- c(
    "LocationID", "LocationName", "SPA", "Area",
    "XBelgiumLambert72", "YBelgiumLambert72"
  )
  location <- read.csv2(
    location.file,
    dec = "."
  )
  check_dataframe_variable(
    df = location,
    variable = old.names,
    name = location.file
  )
  location <- location[, old.names]
  colnames(location) <- new.names
  if (anyDuplicated(location$LocationID)) {
    warning("duplicate id in ", location.file)
  }
  write_vc(
    location,
    file = "location.txt",
    sorting = "LocationID",
    stage = TRUE,
    root = walloon.connection
  )

  old.names <- c("ID_visit", "CODE_ULT", "DATE")
  new.names <- c("OriginalObservationID", "LocationID", "Date")
  visit <- read.csv2(
    visit.file,
    dec = "."
  )
  check_dataframe_variable(df = visit, variable = old.names, name = visit.file)
  visit <- visit[, old.names]
  colnames(visit) <- new.names
  visit$Date <- as.Date(visit$Date, format = "%d/%m/%Y")
  if (anyDuplicated(visit$OriginalObservationID)) {
    warning("duplicate id in ", visit.file)
    duplicate.id <- names(which(table(visit$OriginalObservationID) > 1))
    visit.duplicate <- visit[visit$OriginalObservationID %in% duplicate.id, ]
    visit <- aggregate(
      visit[, "Date", drop = FALSE],
      visit[, c("OriginalObservationID", "LocationID")],
      FUN = median
    )
  } else {
    visit.duplicate <- FALSE
  }
  if (!all(visit$LocationID %in% location$LocationID)) {
    stop(visit.file, " contains id which is not in ", location.file)
  }

  visit$ObservationID <- apply(
    visit[, c("OriginalObservationID", "LocationID")],
    1,
    digest,
    algo = "sha1"
  )
  write_vc(
    visit[, c("ObservationID", new.names)],
    file = "visit.txt",
    sorting = "ObservationID",
    stage = TRUE,
    root = walloon.connection
  )

  old.names <- c("ID_visit", "TAXPRIO", "SommeDeN")
  new.names <- c("OriginalObservationID", "Species", "Count")
  data <- read.csv2(
    data.file,
    dec = "."
  )
  check_dataframe_variable(df = data, variable = old.names, name = data.file)
  data <- data[, old.names]
  colnames(data) <- new.names
  if (!all(data$OriginalObservationID %in% visit$OriginalObservationID)) {
    stop(data.file, " contains id which is not in ", visit.file)
  }
  if (any(table(data$OriginalObservationID, data$Species) > 1)) {
    warning(
"Species with multiple counts per visit. Only the highest values is retained"
    )
    n.obs <- aggregate(
      data[, "Count"],
      data[, c("OriginalObservationID", "Species")],
      FUN = length
    )
    data.duplicate <- merge(
      n.obs[n.obs$x > 1, c("OriginalObservationID", "Species")],
      data
    )
    data <- aggregate(
      data[, "Count", drop = FALSE],
      data[, c("OriginalObservationID", "Species")],
      FUN = max
    )
  } else {
    data.duplicate <- NA
  }
  species <- data.frame(
    ScientificName = unique(data$Species),
    stringsAsFactors = FALSE
  )
  species <- get_nbn_key_multi(species, orders = "la")
  if (anyNA(species$NBNKey)) {
    species.nomatch <- species$ScientificName[is.na(species$NBNKey)]
    warning(
      "Unmatched species will be ignored:\n",
      paste(species.nomatch, collapse = "\n")
    )
    species <- species[!is.na(species$NBNKey), ]
  } else {
    species.nomatch <- NA
  }
  write_vc(
    species,
    file = "species.txt",
    sorting = "NBNKey",
    stage = TRUE,
    root = walloon.connection
  )


  data <- merge(data, species, by.x = "Species", by.y = "ScientificName")
  data$Species <- NULL

  write_vc(
    data[, c("OriginalObservationID", "NBNKey", "Count")],
    file = "data.txt",
    sorting = c("OriginalObservationID", "NBNKey"),
    stage = TRUE,
    root = walloon.connection
  )

  auto_commit(
    package = environmentName(parent.env(environment())),
    repo = walloon.connection
  )
  return(list(
    DuplicateVisit = visit.duplicate,
    DuplicateData = data.duplicate,
    UnmatchedSpecies = species.nomatch
  ))
}
