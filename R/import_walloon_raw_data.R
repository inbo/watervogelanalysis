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
#' @inheritParams n2khelper::auto_commit
#' @export
#' @importFrom n2khelper check_dataframe_variable write_delim_git auto_commit
import_walloon_raw_data <- function(
  location.file, visit.file, data.file, path = ".", username, password
){
  path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  
  old.names <- c("CODE_ULT", "Nom_site", "ZPS", "Area", "Xlamb", "Ylamb")
  new.names <- c(
    "LocationID", "LocationName", "SPA", "Area", "XBelgiumLambert72", "YBelgiumLambert72"
  )
  location <- read.csv2(
    paste(path, location.file, sep = "/"), 
    dec = "."
  )
  if(!check_dataframe_variable(df = location, variable = old.names, name = "location")){
    stop("Some required variables are missing in ", location.file)
  }
  location <- location[, old.names]
  colnames(location) <- new.names
  if(any(duplicated(location$LocationID))){
    warning("duplicate id in ", location.file)
  }
  location <- location[order(location$LocationID), ]
  write_delim_git(location, file = "location.txt", path = "watervogel/wallonia")
  
  old.names <- c("ID_visit", "CODE_ULT", "DATE")
  new.names <- c("ObservationID", "LocationID", "Date")
  visit <- read.csv2(
    paste(path, visit.file, sep = "/"), 
    dec = "."
  )
  if(!check_dataframe_variable(df = visit, variable = old.names, name = "visit")){
    stop("Some required variables are missing in ", visit.file)
  }
  visit <- visit[, old.names]
  colnames(visit) <- new.names
  if(any(duplicated(visit$ObservationID))){
    warning("duplicate id in ", visit.file)
    visit <- aggregate(
      visit[, "Date", drop = FALSE],
      visit[, c("ObservationID", "LocationID")],
      FUN = median
    )
  }
  
  if(!all(visit$LocationID %in% location$LocationID)){
    stop(visit.file, " contains id which is not in ", location.file)
  }
  visit <- visit[order(visit$ObservationID), ]
  write_delim_git(visit, file = "visit.txt", path = "watervogel/wallonia")
  
  old.names <- c("ID_visit", "TAXPRIO", "SommeDeN")
  new.names <- c("ObservationID", "Species", "Count")
  data <- read.csv2(
    paste(path, data.file, sep = "/"), 
    dec = "."
  )
  if(!check_dataframe_variable(df = data, variable = old.names, name = "data")){
    stop("Some required variables are missing in ", data.file)
  }
  data <- data[, old.names]
  colnames(data) <- new.names
  if(!all(data$ObservationID %in% visit$ObservationID)){
    stop(data.file, " contains id which is not in ", visit.file)
  }
  species <- read_specieslist(limit = FALSE)$species
  detected.species <- unique(data$Species) 
  unknown.species <- detected.species[!detected.species %in% species$Species]
  if(length(unknown.species) > 0){
    warning("Species in ", data.file, " but not present in read_specieslist(limit = FALSE):\n", paste(sort(unknown.species), collapse = "\n"))
  }
  data <- merge(data, species[, c("SpeciesID", "Species")])
  data <- aggregate(
    data[, "Count", drop = FALSE],
    data[, c("ObservationID", "SpeciesID")],
    FUN = max
  )

  data <- data[order(data$ObservationID, data$SpeciesID), ]
  write_delim_git(data, file = "data.txt", path = "watervogel/wallonia")
  
  auto_commit(
    package = environmentName(parent.env(environment())), 
    username = username, password = password
  )
}
