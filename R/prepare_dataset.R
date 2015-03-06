#' Prepare the raw datasets and save them to the git repository
#' 
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams n2khelper::auto_commit
#' @export
#' @importFrom n2khelper write_delim_git check_single_logical auto_commit
#' @importFrom n2kanalysis select_factor_count_strictly_positive select_factor_threshold select_observed_range
#' @importFrom plyr d_ply
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(username, password, verbose = TRUE){
  verbose <- check_single_logical(verbose)
  
  #read and save locations
  location <- read_location()
  write_delim_git(x = location, file = "location.txt", path = "watervogel")
  
  #read and save species
  species.list <- read_specieslist()
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
      species.covered = x$SpeciesCovered
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
  
  return(invisible(NULL))
}
