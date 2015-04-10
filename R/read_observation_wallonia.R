#' Read the Wallon observations from a species
#' 
#' All available imported data is used. The only constraint is that the observation are not older than \code{first.winter} and originate form november up to februari.
#' @inheritParams read_observation
#' @export
#' @importFrom n2khelper check_single_strictly_positive_integer read_delim_git
#' @importFrom lubridate round_date year
read_observation_wallonia <- function(species.id, first.winter){
  species.id <- check_single_strictly_positive_integer(species.id)
  first.winter <- check_single_strictly_positive_integer(first.winter)
  
  data <- read_delim_git(file = "data.txt", path = "watervogel/wallonia")
  data <- data[!is.na(data$SpeciesID) & data$SpeciesID == species.id, ]
  
  if(nrow(data) == 0){
    return(NULL)
  }
  
  visit <- read_delim_git(file = "visit.txt", path = "watervogel/wallonia")
  visit$Date <- as.Date(visit$Date)
  #limit to november to februari
  visit <- visit[format(visit$Date, "%m") %in% c("11", "12", "01", "02"), ]
  
  visit.winter <- year(round_date(visit$Date, unit = "year"))
  visit <- visit[visit.winter >= first.winter, ]
  
  observation <- merge(visit, data, all.x = TRUE)
  observation$SpeciesID <- NULL
  observation$Complete <- 1
  observation$Count[is.na(observation$Count)] <- 0
  
  observation <- observation[order(observation$ObservationID), ]
  return(observation)
}
