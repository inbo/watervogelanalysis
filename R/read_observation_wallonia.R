#' Read the Walloon observations from a species
#'
#' All available imported data is used.
#' @inheritParams read_observation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom git2rdata read_vc
#' @importFrom lubridate round_date year
#' @importFrom assertthat assert_that is.count
#' @importFrom dplyr %>% filter mutate left_join transmute
#' @importFrom rlang .data
read_observation_wallonia <- function(
  species_id, first_year, latest_year, walloon_repo
) {
  assert_that(is.count(species_id), is.count(first_year), is.count(latest_year))
  first_year <- as.integer(first_year)
  latest_year <- as.integer(latest_year)

  read_vc(file = "species", root = walloon_repo) |>
    filter(.data$euring == species_id) |>
    semi_join(
      x = read_vc(file = "data", root = walloon_repo),
      by = c("species" = "scientific")
    ) |>
    select(-"species", count = "n") -> data

  if (nrow(data) == 0) {
    return(NULL)
  }

  read_vc(file = "visit", root = walloon_repo) |>
    mutate(
      month = as.integer(format(.data$date, "%m")),
      year = as.integer(format(.data$date, "%Y")) +
        as.integer(.data$month >= 7)
    ) |>
    filter(first_year <= .data$year, .data$year <= latest_year) |>
    left_join(data, by = c("hash" = "visit")) |>
    transmute(
      observation_id = .data$hash, external_code = .data$site, .data$year,
      .data$month, count = replace_na(.data$count, 0L), complete = 1L
    )
}
