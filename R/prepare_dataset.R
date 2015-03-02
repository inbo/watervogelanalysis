#' Prepare the raw datasets and save them to the git repository
#' @param verbose Display a progress bar when TRUE (default)
#' @export
#' @importFrom n2khelper write_delim_git check_single_logical auto_commit
#' @importFrom plyr d_ply
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
    write_delim_git(x = observation, file = paste0(x$SpeciesID[1], "_VL.txt"), path = "watervogel")
  })
  
  auto_commit(package = environmentName(parent.env(environment())))
  
  return(invisible(NULL))
}
