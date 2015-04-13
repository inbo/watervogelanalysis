#' Prepare the raw datasets and save them to the git repository
#' 
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme.id the id of the scheme
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams n2khelper::auto_commit
#' @inheritParams n2khelper::odbc_connect
#' @export
#' @importFrom n2khelper check_single_logical check_single_strictly_positive_integer remove_files_git auto_commit odbc_get_id
#' @importFrom RODBC odbcClose
#' @importFrom plyr d_ply
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(
  username, 
  password, 
  verbose = TRUE, 
  develop = TRUE, 
  scheme.id = odbc_get_id(
    table = "Scheme", variable = "Description", value = "Watervogels", develop = develop
  )
){
  verbose <- check_single_logical(verbose)
  scheme.id <- check_single_strictly_positive_integer(scheme.id)

  path <- "watervogel"
  pattern <- "\\.txt$"
  success <- remove_files_git(path = path, pattern = pattern)
  if(length(success) > 0 && !all(success)){
    stop("Error cleaning existing files in the git repository. Path: '", path, "', pattern: '", pattern, "'")
  }
  
  #read and save locations to database
  location <- prepare_dataset_location(develop = develop)
  
  #read and save species
  species.constraint <- prepare_dataset_species(develop = develop)
  
  # read and save observations
  if(verbose){
    progress <- "time"
  } else {
    progress <- "none"
  }
#   this.constraint <- subset(species.constraint, ExternalCode == 70)
  junk <- d_ply(
    .data = species.constraint, 
    .variables = "ExternalCode", 
    .progress = progress, 
    .fun = prepare_dataset_observation,
    location = location,
    scheme.id = scheme.id,
    develop = develop
  )

  auto_commit(
    package = environmentName(parent.env(environment())),
    username = username,
    password = password
  )

  return(invisible(NULL))
}
