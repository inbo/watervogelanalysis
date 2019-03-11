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
select_relevant_analysis <- function(observation){
  if (is.null(observation)) {
    return(NULL)
  }
  assert_that(inherits(observation, "data.frame"))
  assert_that(has_name(observation, "Count"))
  assert_that(has_name(observation, "fMonth"))
  assert_that(has_name(observation, "LocationID"))
  assert_that(has_name(observation, "Year"))

  # select months with have on average at least 5% of the top month
  observation <- select_factor_threshold(
    observation = observation,
    variable = "fMonth",
    threshold = 0.05
  )
  if (nrow(observation) == 0) {
    return(observation)
  }
  observation$fMonth <- factor(observation$fMonth)

  # select locations with at least 4 prescences
  observation <- select_factor_count_strictly_positive( #nolint
    observation = observation,
    variable = "LocationID",
    threshold = 5
  )
  if (nrow(observation) == 0) {
    return(observation)
  }

  # select locations with prescences in at least 3 years
  observation <- select_factor_count_strictly_positive( #nolint
    observation = observation,
    variable = c("LocationID", "Year"),
    threshold = 3,
    dimension = 1
  )
  if (nrow(observation) == 0) {
    return(observation)
  }

  # remove time periodes without prescences at the start or end
  select_observed_range(
    observation = observation,
    variable = "Year"
  )
}
