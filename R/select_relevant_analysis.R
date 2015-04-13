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
#' @importFrom n2khelper check_dataframe_variable
#' @importFrom n2kanalysis select_factor_threshold select_factor_count_strictly_positive select_observed_range
select_relevant_analysis <- function(observation){
  if(is.null(observation)){
    return(NULL)
  }
  check_dataframe_variable(
    df = observation, 
    variable = c("Count", "fMonth", "LocationID", "Year"), 
    name = "observation"
  )
  
  # select months with have on average at least 5% of the top month
  observation <- select_factor_threshold(
    observation = observation,
    variable = "fMonth",
    threshold = 0.05
  )
  if(nrow(observation) == 0){
    return(NULL)
  }
  observation$fMonth <- factor(observation$fMonth)
  
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
  observation <- select_factor_count_strictly_positive(
    observation = observation, 
    variable = c("LocationID", "Year"),
    threshold = 3,
    dimension = 1
  )
  if(nrow(observation) == 0){
    return(NULL)
  }
  
  # remove time periodes without prescences
  observation <- select_observed_range(
    observation = observation,
    variable = "Year"
  )
  if(nrow(observation) == 0){
    return(NULL)
  }
  
  return(observation)
}