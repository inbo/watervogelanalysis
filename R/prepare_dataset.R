#' Prepare the raw datasets and save them to the git repository
#'
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme.id the id of the scheme
#' @param raw.connection a git-connection object to write the output to
#' @param walloon.connection a git-connection object to the Walloon source data
#' @param nbn.channel an RODBC connection to the NBN database
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams connect_flemish_source
#' @inheritParams read_specieslist
#' @export
#' @importFrom n2khelper remove_files_git write_delim_git auto_commit odbc_get_id
#' @importFrom RODBC odbcClose
#' @importFrom plyr d_ply
#' @importFrom assertthat assert_that is.string is.flag noNA
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
  nbn.channel,
  verbose = TRUE
){
  assert_that(is.flag(verbose))
  assert_that(noNA(verbose))
  assert_that(is.string(scheme.id))

  remove_files_git(connection = raw.connection, pattern = "\\.txt$")

  if (verbose) {
    message("Reading and saving locations")
  }
  location <- prepare_dataset_location(
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection,
    raw.connection = raw.connection,
    scheme.id = scheme.id
  )
  dataset <- location$Dataset
  location <- location$Location

  if (verbose) {
    message("Reading and saving species")
  }
  species.constraint <- prepare_dataset_species(
    raw.connection = raw.connection,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection,
    result.channel = result.channel,
    attribute.connection = attribute.connection,
    nbn.channel = nbn.channel,
    scheme.id = scheme.id
  )
  latest.year <- as.integer(format(Sys.time(), "%Y"))
  if (Sys.time() < as.POSIXct(format(Sys.time(), "%Y-05-15"))) {
    latest.year <- latest.year - 1
  }
  species.constraint$Lastyear <- latest.year

  metadata <- species.constraint %>%
    select_(
      ~SpeciesGroupID,
      FirstImportedYear = ~Firstyear,
      LastImportedYear = ~Lastyear
    ) %>%
    distinct_() %>%
    mutate_(
      Duration = ~LastImportedYear - FirstImportedYear + 1,
      SchemeID = ~scheme.id
    ) %>%
    arrange_(~SpeciesGroupID)

  metadata.sha <- write_delim_git(
    x = metadata,
    file = "metadata.txt",
    connection = raw.connection
  )

  # read and save observations
  if (verbose) {
    message("Reading and saving observations")
    progress <- "time"
  } else {
    progress <- "none"
  }
  junk <- d_ply(
    .data = species.constraint,
    .variables = "SpeciesGroupID",
    .progress = progress,
    .fun = prepare_dataset_observation,
    location = location,
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection,
    raw.connection = raw.connection,
    scheme.id = scheme.id
  )

  auto_commit(
    package = environmentName(parent.env(environment())),
    connection = raw.connection
  )

  return(invisible(NULL))
}
