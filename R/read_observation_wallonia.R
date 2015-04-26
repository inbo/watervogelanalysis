#' Read the Wallon observations from a species
#' 
#' All available imported data is used. The only constraint is that the observation are not older than \code{first.winter} and originate form november up to februari.
#' @inheritParams read_observation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper check_single_strictly_positive_integer read_delim_git
#' @importFrom lubridate round_date year
read_observation_wallonia <- function(species.id, first.winter, walloon.connection){
  species.id <- check_single_character(species.id, name = "species.id")
  first.winter <- check_single_strictly_positive_integer(first.winter, name = "first.winter")
  
  data <- read_delim_git(file = "data.txt", connection = walloon.connection)
  data <- data[!is.na(data$NBNID) & data$NBNID == species.id, ]
  
  if(nrow(data) == 0){
    return(NULL)
  }
  
  visit <- read_delim_git(file = "visit.txt", connection = walloon.connection)
  visit$Date <- as.Date(visit$Date)
  #limit to november to februari
  visit <- visit[format(visit$Date, "%m") %in% c("11", "12", "01", "02"), ]
  
  visit.winter <- year(round_date(visit$Date, unit = "year"))
  visit <- visit[visit.winter >= first.winter, ]
  
  observation <- merge(visit, data, all.x = TRUE)
  observation$OriginalObservationID <- NULL
  observation$NBNID <- NULL
  observation$Complete <- 1
  observation$Count[is.na(observation$Count)] <- 0
  
  observation <- observation[order(observation$ObservationID), ]
  return(observation)
}
