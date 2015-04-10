#' Select the relevant observations when importing the raw data
#' 
#' Relevant observations have
#' \itemize{
#'  \item At least 4 presences per location
#'  \item Only locations where the species was present during at least 3 winters
#'  \item No winters without any presences at the beginning or end of the dataset
#' }
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom n2khelper check_dataframe_variable
#' @importFrom n2kanalysis select_factor_count_strictly_positive select_observed_range
#' @importFrom lubridate round_date
select_relevant_import <- function(observation){
  if(is.null(observation)){
    return(NULL)
  }
  check_dataframe_variable(
    df = observation, 
    variable = c("Count", "Date", "LocationID"), 
    name = "observation"
  )
  
  # select locations with at least 4 prescences
  observation <- select_factor_count_strictly_positive(
    observation = observation, 
    variable = "LocationID",
    threshold = 4
  )
  if(nrow(observation) == 0){
    return(NULL)
  }
  
  # select locations with prescences in at least 3 years
  observation$Winter <- round_date(observation$Date, unit = "year")
  observation <- select_factor_count_strictly_positive(
    observation = observation, 
    variable = c("LocationID", "Winter"),
    threshold = 3,
    dimension = 1
  )
  if(nrow(observation) == 0){
    return(NULL)
  }
  
  # remove time periodes without prescences
  observation <- select_observed_range(
    observation = observation,
    variable = "Winter"
  )
  observation$Winter <- NULL
  if(nrow(observation) == 0){
    return(NULL)
  }
  
  return(observation)
}
