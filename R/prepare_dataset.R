#' Prepare the raw datasets and save them to the git repository
#' 
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme.id the id of the scheme
#' @param raw.connection a git-connection object to write the output to
#' @param walloon.connection a git-connection object to the Walloon source data
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams connect_flemish_source
#' @inheritParams read_specieslist
#' @export
#' @importFrom n2khelper check_single_logical check_single_strictly_positive_integer remove_files_git write_delim_git auto_commit odbc_get_id
#' @importFrom RODBC odbcClose
#' @importFrom plyr d_ply
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(
  scheme.id,
  raw.connection,
  result.channel,
  attribute.connection,
  walloon.connection,
  flemish.channel,
  verbose = TRUE
){
  verbose <- check_single_logical(verbose)
  scheme.id <- check_single_strictly_positive_integer(scheme.id)

  success <- remove_files_git(connection = raw.connection, pattern = "\\.txt$")
  write_delim_git(
    x = data.frame(SchemeID = scheme.id),
    file = "scheme.txt",
    connection = raw.connection
  )
  
  if(verbose){
    message("Reading and saving locations")
  }
  location <- prepare_dataset_location(  
    result.channel = result.channel, 
    flemish.channel = flemish.channel, 
    walloon.connection = walloon.connection,
    raw.connection = raw.connection
  )
  
  if(verbose){
    message("Reading and saving species")
  }
  species.constraint <- prepare_dataset_species(
    raw.connection = raw.connection,
    flemish.channel = flemish.channel, 
    walloon.connection = walloon.connection,
    result.channel = result.channel,
    attribute.connection = attribute.connection
  )
  species.constraint$ExternalCode <- levels(species.constraint$ExternalCode)[species.constraint$ExternalCode]
  
  # read and save observations
  if(verbose){
    message("Reading and saving observations")
    progress <- "time"
  } else {
    progress <- "none"
  }
#   this.constraint <- subset(species.constraint, SpeciesGroupID == 1)
  junk <- d_ply(
    .data = species.constraint, 
    .variables = "SpeciesGroupID", 
    .progress = progress, 
    .fun = prepare_dataset_observation,
    location = location,
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection,
    raw.connection = raw.connection
  )

  auto_commit(
    package = environmentName(parent.env(environment())),
    connection = raw.connection
  )

  return(invisible(NULL))
}
