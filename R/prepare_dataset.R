#' Prepare the raw datasets and save them to the git repository
#'
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme_id the id of the scheme
#' @param raw_repo a git_repository object to write the output to
#' @param flemish_channel a DBI connection to the Flemish database
#' @param walloon_repo a git_repository object to the Walloon source data
#' @param verbose Display a progress bar when TRUE (default)
#' @param first_year first winter to import. defaults to 1992
#' @param latest_year latest winter to import. Winter 2019 is defined as 2018-10-01 until 2019-03-31. Defaults the winter prior to last firsth of July. 2019-06-30 becomes 2018, 2019-07-01 becomes 2019
#' @inheritParams connect_flemish_source
#' @inheritParams datasource_id_result
#' @export
#' @importFrom git2rdata write_vc commit rm_data prune_meta
#' @importFrom dplyr %>% bind_rows distinct mutate arrange group_by do filter
#' @importFrom rlang .data
#' @importFrom assertthat assert_that is.string is.flag noNA is.count
#' @importFrom tidyr unnest
#' @importFrom tibble tibble
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(
  scheme_id, raw_repo, result_channel, walloon_repo, flemish_channel,
  develop = FALSE, verbose = TRUE, first_year = 1992,
  latest_year = as.integer(format(Sys.time(), "%Y"))
) {
  assert_that(is.flag(verbose), noNA(verbose), is.flag(develop), noNA(develop),
              is.string(scheme_id), is.count(first_year), is.count(latest_year),
              first_year <= latest_year)
  latest_year <- min(latest_year, as.integer(format(Sys.time(), "%Y")))
  latest_date <- as.POSIXct(paste0(latest_year, "-07-01"))
  if (latest_date > Sys.time()) {
    latest_year <- latest_year - 1
    latest_date <- as.POSIXct(paste0(latest_year, "-07-01"))
  }
  first_date <- as.POSIXct(paste0(first_year - 1, "-10-01"))

  rm_data(root = raw_repo, path = ".", stage = TRUE)

  if (verbose) {
    message("Reading and saving locations")
  }
  location <- prepare_dataset_location(
    result_channel = result_channel, flemish_channel = flemish_channel,
    walloon_repo = walloon_repo, raw_repo = raw_repo, scheme_id = scheme_id,
    first_date = first_date, latest_date = latest_date)
  dataset <- location$Dataset
  location$LocationGroup %>%
    filter(.data$description == "Belgi\u00EB") %>%
    dplyr::pull("fingerprint") -> location_group_id
  location <- location$Location

  if (verbose) {
    message("Reading and saving species")
  }
  species <- prepare_dataset_species(
    raw_repo = raw_repo, flemish_channel = flemish_channel,
    walloon_repo = walloon_repo, result_channel = result_channel,
    scheme_id = scheme_id, first_date = first_date, latest_date = latest_date)

  species %>%
    distinct(.data$species_id) %>%
    mutate(
      first_imported_year = first_year, last_imported_year = latest_year,
      scheme_id = scheme_id,
      results_datasource_id =
        datasource_id_result(result_channel = result_channel, develop = develop)
    ) %>%
    arrange(.data$species_id) -> metadata
  metadata_sha <- write_vc(x = metadata, file = "metadata", root = raw_repo,
                           sorting = "species_id", stage = TRUE)

  tibble(
    filename = c(metadata_sha, attr(species, "speciesgroupspecies_hash")),
    fingerprint = c(names(metadata_sha),
                    names(attr(species, "speciesgroupspecies_hash"))),
    import_date = dataset$import_date[1],
    datasource = dataset$datasource[1]
  ) %>%
    bind_rows(dataset) -> dataset

  # read and save observations
  if (verbose) {
    message("Reading and saving observations")
  }
  species %>%
    group_by(.data$species_group_id) %>%
    do(
      import_analysis = prepare_dataset_observation(
        .data, location = location, location_group_id = location_group_id,
        flemish_channel = flemish_channel, walloon_repo = walloon_repo,
        result_channel = result_channel, raw_repo = raw_repo, dataset = dataset
      )
    ) %>%
    unnest(.data$import_analysis) %>%
    write_vc(file = "import", sorting = "species_group_id", stage = TRUE,
             root = raw_repo)
  prune_meta(root = raw_repo, path = ".", stage = TRUE)

  commit(repo = raw_repo, session = TRUE,
         message = "scripted commit from watervogelanalysis")

  return(invisible(NULL))
}
