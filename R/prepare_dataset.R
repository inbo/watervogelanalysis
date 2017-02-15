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
#' @inheritParams datasource_id_result
#' @export
#' @importFrom n2khelper remove_files_git write_delim_git auto_commit odbc_get_id
#' @importFrom RODBC odbcClose
#' @importFrom dplyr %>% bind_rows select_ distinct_ mutate_ arrange_ group_by_ do_
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
  develop = FALSE,
  verbose = TRUE
){
  assert_that(is.flag(verbose))
  assert_that(noNA(verbose))
  assert_that(is.flag(develop))
  assert_that(noNA(develop))
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
  location_group_id <- location$LocationGroup %>%
    filter_(~description == "Belgi\\u0137") %>%
    '[['("fingerprint")
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
      ~SpeciesID,
      FirstImportedYear = ~Firstyear,
      LastImportedYear = ~Lastyear
    ) %>%
    distinct_() %>%
    mutate_(
      Duration = ~LastImportedYear - FirstImportedYear + 1,
      SchemeID = ~scheme.id,
      ResultDatasourceID = ~datasource_id_result(
        result.channel = result.channel,
        develop = develop
      )
    ) %>%
    arrange_(~SpeciesID)

  metadata.sha <- write_delim_git(
    x = metadata,
    file = "metadata.txt",
    connection = raw.connection
  )

  dataset <- data.frame(
    filename = c("metadata.txt", "speciesgroupspecies.txt"),
    fingerprint = c(
      metadata.sha,
      attr(species.constraint, "speciesgroupspecies.hash")
    ),
    import_date = dataset$import_date[1],
    datasource = dataset$datasource[1],
    stringsAsFactors = FALSE
  ) %>%
    bind_rows(dataset)

  # read and save observations
  if (verbose) {
    message("Reading and saving observations")
  }
  species.constraint %>%
    group_by_(~SpeciesGroupID) %>%
    do_(
      stored = ~prepare_dataset_observation(
        .,
        location = location,
        location_group_id = location_group_id,
        flemish.channel = flemish.channel,
        walloon.connection = walloon.connection,
        result.channel = result.channel,
        raw.connection = raw.connection,
        scheme.id = scheme.id,
        dataset = dataset
      )
    )

  auto_commit(
    package = "watervogels",
    connection = raw.connection
  )

  return(invisible(NULL))
}
