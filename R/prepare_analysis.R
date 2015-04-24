#' Prepare all datasets and a to do list of models
#' @export
#' @importFrom n2khelper check_single_character read_delim_git list_files_git check_single_strictly_positive_integer
#' @importFrom lubridate ymd year round_date
#' @inheritParams prepare_analysis_dataset
#' @inheritParams prepare_dataset
#' @inheritParams n2khelper::odbc_connect
prepare_analysis <- function(output.path = ".", raw.connection){
  path <- check_single_character(output.path)
  
  # remove existing rda files
  if(file_test("-d", output.path)){
    existing <- list.files(output.path, pattern = ".rda", full.names = TRUE)
    success <- file.remove(existing)
    if(length(success) > 0 & !all(success)){
      stop("unable to remove some files:\n", paste(existing[!success], collapse = "\n"))
    }
  }
  
  location <- read_delim_git(file = "location.txt", connection = raw.connection)
  location.group <- read_delim_git(file = "locationgroup.txt", connection = raw.connection)
  location.group.location <- read_delim_git(
    file = "locationgrouplocation.txt", connection = raw.connection
  )
  
  location$StartDate <- ymd(location$StartDate)
  location$EndDate <- ymd(location$EndDate)
  location$StartYear <- year(round_date(location$StartDate, unit = "year"))
  location$EndYear <- year(round_date(location$EndDate, unit = "year"))
  
  to.impute <- unique(location.group[, c("Impute", "SubsetMonths")])
  rm(location.group)
  location.group.location <- merge(
    location.group.location, 
    to.impute, 
    by.x = "LocationGroupID", 
    by.y = "Impute"
  )
  location <- merge(
    location.group.location,
    location[, c("ID", "StartYear", "EndYear")],
    by.x = "LocationID",
    by.y = "ID"
  )
  rm(location.group.location)

  rawdata.files <- list_files_git(connection = raw.connection, pattern = "^[0-9.*]\\.txt$")
  output <- lapply(
    rawdata.files, 
    prepare_analysis_dataset, 
    path = path, location = location
  )
  output <- do.call(rbind, output)
  filename <- normalizePath(
    paste0(output.path, "/to_do.rda"), winslash = "/", mustWork = FALSE
  )
  save(output, file = filename)
  return(output)
}
