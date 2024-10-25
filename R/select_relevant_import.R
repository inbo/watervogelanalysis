#' Select the relevant observations when importing the raw data
#'
#' Relevant observations have
#' - At least 4 presences per location
#' - Only locations where the species was present during at least 3 winters
#' - No winters without any presences at the beginning or end of the dataset
#' @inheritParams n2kanalysis::select_factor_threshold
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr count distinct filter semi_join summarise
#' @importFrom rlang .data
#' @export
select_relevant_import <- function(observation) {
  if (is.null(observation)) {
    return(NULL)
  }
  assert_that(
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "location")
  )

  # select locations with at least 4 occurrences
  observation |>
    filter(.data$count > 0) |>
    count(.data$location) |>
    filter(.data$n >= 4) |>
    semi_join(x = observation, by = "location") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # select locations with occurrences in at least 3 years
  observation |>
    filter(.data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 3) |>
    semi_join(x = observation, by = "location") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # remove time periods without occurrences at the start or end
  observation |>
    filter(.data$count > 0) |>
    summarise(start = min(.data$year), end = max(.data$year)) -> w_range
  observation |>
    filter(w_range$start <= .data$year, .data$year <= w_range$end)
}
