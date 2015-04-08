#' Prepare the raw datasets and save them to the git repository
#' 
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme.id the id of the scheme
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams n2khelper::auto_commit
#' @inheritParams n2khelper::odbc_connect
#' @export
#' @importFrom n2khelper check_single_logical connect_result odbc_get_id write_delim_git auto_commit get_scheme_id get_datasource_id
#' @importFrom n2kanalysis select_factor_count_strictly_positive select_factor_threshold select_observed_range
#' @importFrom RODBC odbcClose
#' @importFrom plyr d_ply
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(username, password, scheme.id = get_scheme_id("Watervogels"), verbose = TRUE, develop = TRUE){
  verbose <- check_single_logical(verbose)
  develop <- check_single_logical(develop)
  
  #read and save locations to database
  import.date <- Sys.time()
  location <- read_location(develop = develop)
  
  channel <- connect_result(develop = develop)
  database.id <- odbc_get_id(
    data = location[, c("ExternalCode", "DatasourceID", "Description")],
    id.field = "ID", merge.field = c("ExternalCode", "DatasourceID"), table = "Location",
    channel = channel, create = TRUE
  )
  location <- merge(database.id, location)
  location$Description <- NULL
  location$ExternalCode <- NULL
  
  # define and save location groups
  location.group <- data.frame(
    Description = c("Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\\u0137", "Vogelrichtlijn Belgi\\u0137"),
    Flanders = c(TRUE, FALSE, TRUE, TRUE, FALSE, TRUE),
    Wallonia = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SPA = c(FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
    Impute = c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE),
    SchemeID = scheme.id
  )
  database.id <- odbc_get_id(
    data = location.group[, c("SchemeID", "Description")],
    id.field = "ID", merge.field = c("SchemeID", "Description"), table = "LocationGroup",
    channel = channel, create = TRUE
  )
  location.group <- merge(database.id, location.group)
  
  location.group$SchemeID <- NULL
  location.group$Description <- NULL
  
  # get the locations per location group
  flanders <- get_datasource_id(data.source.name = "Raw data watervogels Flanders")
  location.group.location <- do.call(rbind, lapply(
    seq_along(location.group$ID),
    function(i){
      if(location.group$SPA[i]){
        selection <- location$SPA == 1
      } else {
        selection <- rep(TRUE, nrow(location))
      }
      if(location.group$Flanders[i]){
        if(!location.group$Wallonia[i]){
          selection <- selection & location$DatasourceID == flanders
        }
      } else {
        selection <- selection & location$DatasourceID != flanders
      }
      data.frame(
        LocationGroupID = location.group$ID[i],
        LocationID = location$ID[selection]
      )
    }
  ))
  database.id <- odbc_get_id(
    data = location.group.location[, c("LocationGroupID", "LocationID")],
    id.field = "ID", merge.field = c("LocationGroupID", "LocationID"), table = "LocationGroupLocation",
    channel = channel, create = TRUE
  )
  
  location <- location[order(location$ID), c("ID", "StartDate", "EndDate")]
  location.group <- location.group[order(location.group$ID), c("ID", "Impute")]
  location.group.location <- location.group.location[
    order(
      location.group.location$LocationGroupID,
      location.group.location$LocationID
    ), 
    c("LocationGroupID", "LocationID")
  ]
  
  location.sha <- write_delim_git(
    x = location, file = "location.txt", path = "watervogel"
  )
  location.group.sha <- write_delim_git(
    x = location.group, file = "locationgroup.txt", path = "watervogel"
  )
  location.group.location.sha <- write_delim_git(
    x = location.group.location, file = "locationgrouplocation.txt", path = "watervogel"
  )
  
  dataset <- data.frame(
    FileName = c("location.txt", "locationgroup.txt", "locationgrouplocation.txt"),
    PathName = "watervogel",
    Fingerprint = c(location.sha, location.group.sha, location.group.location.sha),
    ImportDate = import.date,
    Obsolete = FALSE
  )
  database.id <- odbc_get_id(
    data = dataset,
    id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"), table = "Dataset",
    channel = channel, create = TRUE
  )
  
  #read and save species
  species.list <- read_specieslist(develop = develop)
  write_delim_git(x = species.list$species, file = "species.txt", path = "watervogel")
  
  # read and save observations
  if(verbose){
    progress <- "time"
  } else {
    progress <- "none"
  }

  d_ply(species.list$species.constraint, "SpeciesID", .progress = progress, function(x){
    observation.flemish <- read_observation(
      species.id = x$SpeciesID[1], 
      first.winter = x$Firstyear[1], 
      species.covered = x$SpeciesCovered,
      develop = develop
    )
    observation.walloon <- read_observation_wallonia(
      species.id = x$SpeciesID[1], 
      first.winter = x$Firstyear[1]
    )
    
    observation <- select_relevant(observation.flemish)
    if(!is.null(observation)){
      write_delim_git(x = observation, file = paste0(x$SpeciesID[1], "_VL.txt"), path = "watervogel")
    }
    
    observation <- select_relevant(observation.walloon)
    if(!is.null(observation)){
      write_delim_git(x = observation, file = paste0(x$SpeciesID[1], "_WA.txt"), path = "watervogel")
    }
    
    selection <- format(observation.flemish$Date, "%m") %in% c("11", "12", "01", "02")
    if(is.null(observation.walloon)){
      observation <- observation.flemish[selection, ]
    } else {
      observation <- rbind(
        observation.flemish[selection, ],
        observation.walloon
      )
    }
    observation <- select_relevant(observation)
    if(!is.null(observation)){
      observation <- observation[order(observation$ObservationID), ]
      write_delim_git(x = observation, file = paste0(x$SpeciesID[1], "_BE.txt"), path = "watervogel")
    }
  })
  
  auto_commit(
    package = environmentName(parent.env(environment())),
    username = username,
    password = password
  )

  odbcClose(channel)
  
  return(invisible(NULL))
}
