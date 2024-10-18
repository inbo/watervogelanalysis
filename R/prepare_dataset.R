#' Prepare the raw datasets and save them to the git repository
#'
#' The raw data is written to the git repository.
#' All changes are always staged and committed.
#' The commit is pushed when both username and password are provided.
#' @param raw_repo a git_repository object to write the output to
#' @param flemish_channel a DBI connection to the Flemish database
#' @param walloon_repo a git_repository object to the Walloon source data
#' @param verbose Display a progress bar when TRUE (default)
#' @param first_year first winter to import. defaults to 1992
#' @param latest_year latest winter to import.
#' Winter 2019 is defined as 2018-10-01 until 2019-03-31.
#' Defaults the winter prior to last firth of July.
#' 2019-06-30 becomes 2018, 2019-07-01 becomes 2019.
#' @export
#' @importFrom assertthat assert_that is.string is.flag noNA is.count
#' @importFrom dplyr arrange filter mutate pull
#' @importFrom git2rdata commit prune_meta rm_data write_vc
#' @importFrom n2kanalysis display
#' @importFrom purrr walk
#' @importFrom rlang .data
#' @importFrom tidyr nest
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(
  raw_repo, walloon_repo, flemish_channel, verbose = TRUE, first_year = 1992,
  latest_year = as.integer(format(Sys.time(), "%Y"))
) {
  assert_that(
    is.flag(verbose), noNA(verbose), is.count(first_year),
    is.count(latest_year), first_year <= latest_year
  )
  latest_year <- min(latest_year, as.integer(format(Sys.time(), "%Y")))
  latest_date <- as.POSIXct(paste0(latest_year, "-07-01"))
  if (latest_date > Sys.time()) {
    latest_year <- latest_year - 1
    latest_date <- as.POSIXct(paste0(latest_year, "-07-01"))
  }
  first_date <- as.POSIXct(paste0(first_year - 1, "-10-01"))

  rm_data(root = raw_repo, path = "location", stage = TRUE)
  rm_data(root = raw_repo, path = "observation", stage = TRUE)
  rm_data(root = raw_repo, path = "species", stage = TRUE)

  display(verbose, "Reading and saving locations")
  location <- prepare_dataset_location(
    flemish_channel = flemish_channel, walloon_repo = walloon_repo,
    raw_repo = raw_repo, first_date = first_date, latest_date = latest_date
  )

  display(verbose, "Reading and saving species")
  species <- prepare_dataset_species(
    flemish_channel = flemish_channel, walloon_repo = walloon_repo,
    raw_repo = raw_repo, first_date = first_date, latest_date = latest_date
  )

  # read and save observations
  display(verbose, "Reading and saving observations")
  species |>
    mutate(id = .data$euring) |>
    arrange(.data$id) |>
    nest(.by = "id") |>
    pull(.data$data) |>
    walk(
      ~ prepare_dataset_observation(
        this_species = .x, location = location, walloon_repo = walloon_repo,
        flemish_channel = flemish_channel, raw_repo = raw_repo,
        latest_year = latest_year, verbose = verbose
      ), location = location, walloon_repo = walloon_repo, raw_repo = raw_repo,
      flemish_channel = flemish_channel, latest_year = latest_year,
      verbose = verbose
    )
  prune_meta(root = raw_repo, path = ".", stage = TRUE)

  commit(
    repo = raw_repo, session = TRUE,
    message = "scripted commit from watervogelanalysis"
  )

  return(invisible(NULL))
}
