#' Prepare the raw datasets and save them to the git repository
#' @param verbose Display a progress bar when TRUE (default)
#' @export
#' @importFrom n2khelper write_delim_git check_single_logical auto_commit
#' @importFrom n2kanalysis select_factor_count_strictly_positive select_factor_threshold select_observed_range
#' @importFrom plyr d_ply
#' @importFrom lubridate round_date
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(verbose = TRUE){
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
    observation <- read_observation(
      species.id = x$SpeciesID[1], 
      first.winter = x$Firstyear[1], 
      species.covered = x$SpeciesCovered
    )

    # select locations with at least 4 prescences
    observation <- select_factor_count_strictly_positive(
      observation = observation, 
      variable = "LocationID",
      threshold = 4
    )
    
    # select months with have on average at least 5% of the top month
    observation$Month <- factor(format(observation$Date, format = "%m"))
    observation <- select_factor_threshold(
      observation = observation,
      variable = "Month",
      threshold = 0.05
    )
    observation$Month <- NULL

    # select locations with prescences in at least 3 years
    observation$Winter <- round_date(observation$Date, unit = "year")
    observation <- select_factor_count_strictly_positive(
      observation = observation, 
      variable = c("LocationID", "Winter"),
      threshold = 3,
      dimension = 1
    )
    
    # remove time periodes without prescences
    observation <- select_observed_range(
      observation = observation,
      variable = "Winter"
    )
    observation$Winter <- NULL
    
    write_delim_git(x = observation, file = paste0(x$SpeciesID[1], "_VL.txt"), path = "watervogel")
  })
  
  auto_commit(package = environmentName(parent.env(environment())))
  
  return(invisible(NULL))
}
