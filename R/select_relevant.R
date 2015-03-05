#' Select the relevant observation
#' 
#' Relevant observations have
#' \itemize{
#'  \item At least 4 presences per location
#'  \item Only months who's average is at least 5% of the month with the highst abundance
#'  \item Only locations where the species was present during at least 3 winters
#'  \item No winters without any presences at the beginning or end of the dataset
#' }
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom lubridate round_date
select_relevant <- function(observation){
  # select locations with at least 4 prescences
  observation <- select_factor_count_strictly_positive(
    observation = observation, 
    variable = "LocationID",
    threshold = 4
  )
  
  # select months with have on average at least 5% of the top month
  observation$Month <- factor(format(observation$Date, format = "%m"))
  observation <- select_factor_threshold(
    observation = observation,
    variable = "Month",
    threshold = 0.05
  )
  observation$Month <- NULL
  
  # select locations with prescences in at least 3 years
  observation$Winter <- round_date(observation$Date, unit = "year")
  observation <- select_factor_count_strictly_positive(
    observation = observation, 
    variable = c("LocationID", "Winter"),
    threshold = 3,
    dimension = 1
  )
  
  # remove time periodes without prescences
  observation <- select_observed_range(
    observation = observation,
    variable = "Winter"
  )
  observation$Winter <- NULL
  
  return(observation)
}