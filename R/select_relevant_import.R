#' Select the relevant observations when importing the raw data
#'
#' Relevant observations have
#' - At least 4 presences per location
#' - Only locations where the species was present during at least 3 winters
#' - No winters without any presences at the beginning or end of the dataset
#' @inheritParams n2kanalysis::select_factor_threshold
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% filter count semi_join distinct summarise
#' @importFrom rlang .data
#' @export
select_relevant_import <- function(observation) {
  if (is.null(observation)) {
    return(NULL)
  }
  assert_that(inherits(observation, "data.frame"),
              has_name(observation, "Count"), has_name(observation, "Year"),
              has_name(observation, "LocationID"))

  # select locations with at least 4 occurrences
  observation %>%
    filter(.data$Count > 0) %>%
    count(.data$LocationID) %>%
    filter(.data$n >= 4) %>%
    semi_join(x = observation, by = "LocationID") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # select locations with occurrences in at least 3 years
  observation %>%
    filter(.data$Count > 0) %>%
    distinct(.data$LocationID, .data$Year) %>%
    count(.data$LocationID) %>%
    filter(.data$n >= 3) %>%
    semi_join(x = observation, by = "LocationID") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # remove time periodes without occurrences at the start or end
  observation %>%
    filter(.data$Count > 0) %>%
    summarise(start = min(.data$Year), end = max(.data$Year)) -> w_range
  observation %>%
    filter(w_range$start <= .data$Year, .data$Year <= w_range$end)
}
