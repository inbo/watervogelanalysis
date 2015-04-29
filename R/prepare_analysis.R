#' Prepare all datasets and a to do list of models
#' @export
#' @importFrom n2khelper check_single_character read_delim_git list_files_git git_sha
#' @importFrom lubridate ymd year round_date
#' @inheritParams prepare_analysis_dataset
#' @inheritParams prepare_dataset
prepare_analysis <- function(analysis.path = ".", raw.connection, scheme.id){
  path <- check_path(paste0(analysis.path, "/"), type = "directory", error = FALSE)
  if(is.logical(path)){
    dir.create(path = analysis.path, recursive = TRUE)
    path <- check_path(paste0(analysis.path, "/"), type = "directory")
  }
  analysis.path <- path
  
  
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

  rawdata.files <- list_files_git(connection = raw.connection, pattern = "^[0-9]*\\.txt$")
  analysis <- do.call(rbind, lapply(
    rawdata.files, 
    prepare_analysis_dataset, 
    analysis.path = analysis.path, 
    location = location,
    scheme.id = scheme.id,
    raw.connection = raw.connection
  ))
  sha.rawdata <- git_sha(
    file = c(
      rawdata.files, 
      "location.txt",  "locationgroup.txt", "locationgrouplocation.txt"
    ), 
    connection = raw.connection
  )
  dataset <- merge(
    analysis[, c("FileName", "PathName", "Fingerprint")], 
    sha.rawdata,
    by.x = c("FileName", "PathName"),
    by.y = c("File", "Path")
  )
  
  to.do.extra <- analysis[
    !is.na(analysis$Covariate), 
    c("SchemeID", "SpeciesGroupID", "LocationGroupID", "ModelType", "Covariate", "AnalysisDate", "NObs", "NLocation", "Fingerprint")
  ]
  to.do.extra$Status <- "new"
  to.do.file <- check_path(paste0(analysis.path, "todo.rda"), type = "file", error = FALSE)
  if(is.logical(to.do.file)){
    to.do <- to.do.extra
  } else {
    load(to.do.file)
    to.do <- rbind(to.do, to.do.extra)
  }
  save(to.do, file = paste0(analysis.path, "todo.rda"))
  
  return(
    list(
      Analysis = analysis,
      AnalysisDataset = dataset
    )
  )
}
