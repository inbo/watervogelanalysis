#' Select the relevant observations when making the analysis dataset
#'
#' Relevant observations have
#' \itemize{
#'  \item
#'  \item At least 4 presences per location
#'  \item Only locations where the species was present during at least 3 winters
#'  \item No winters without any presences at the beginning or end of the dataset
#' }
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom n2kanalysis select_factor_threshold select_factor_count_strictly_positive select_observed_range
#' @importFrom dplyr distinct filter add_count semi_join
#' @importFrom rlang .data
select_relevant_analysis <- function(observation){
  if (is.null(observation)) {
    return(NULL)
  }
  assert_that(
    inherits(observation, "data.frame"), has_name(observation, "Count"),
    has_name(observation, "Month"), has_name(observation, "LocationID"),
    has_name(observation, "Year"))

  # select locations with at least 4 prescences
  observation %>%
    filter(.data$Count > 0) %>%
    add_count(.data$LocationID) %>%
    filter(.data$n >= 4) %>%
    semi_join(x = observation, by = "LocationID") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # select months with have on average at least 5% of the top month
  observation <- select_factor_threshold(observation = observation,
                                         variable = "Month", threshold = 0.05)
  if (nrow(observation) == 0) {
    return(observation)
  }
  observation$Month <- factor(observation$Month)

  # select locations with at least 4 prescences
  observation %>%
    filter(.data$Count > 0) %>%
    add_count(.data$LocationID) %>%
    filter(.data$n >= 4) %>%
    semi_join(x = observation, by = "LocationID") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # select locations with prescences in at least 3 years
  observation %>%
    filter(.data$Count > 0) %>%
    distinct(.data$Year, .data$LocationID) %>%
    add_count(.data$LocationID) %>%
    filter(.data$n >= 3) %>%
    semi_join(x = observation, by = "LocationID") -> observation
  if (nrow(observation) == 0) {
    return(observation)
  }

  # remove time periodes without prescences at the start or end
  observation %>%
    filter(.data$Count > 0) %>%
    summarise(start = min(.data$Year), end = max(.data$Year)) -> obs_range
  observation %>%
    filter(obs_range$start <= .data$Year, .data$Year <= obs_range$end) ->
    observation
  if (nrow(observation) == 0) {
    return(observation)
  }
  observation %>%
    mutate(Missing = is.na(.data$Count), Month = factor(.data$Month),
           fYear = factor(.data$Year), cYear = .data$Year - max(.data$Year),
           fYearMonth = interaction(.data$fYear, .data$Month, drop = TRUE),
           fYearLocation = interaction(.data$fYear, factor(.data$LocationID),
                                      drop = TRUE)) %>%
    select(-"LocationGroupID")
}
