#' Remove duplicate observations
#'
#' Duplicate observations are combinations of LocationID, Year and fMonth for which multiple rows exist.
#' @return a list with components \code{Observation} and \code{Duplicate}. \code{Observation} holds the cleaned observations. \code{Duplicate} is a data.frame with the ObservationID and DatasourceID of duplicated records.
#' @param observation a data.frame with observations
#' @export
#' @importFrom n2khelper check_dataframe_variable
#' @importFrom plyr ddply
#' @importFrom stats aggregate
remove_duplicate_observation <- function(observation){
  check_dataframe_variable(
    df = observation,
    variable = c(
      "ObservationID", "DatasourceID", "LocationID", "Year", "fMonth",
      "Complete", "Count"
    ),
    name = "observation"
  )

  n.duplicate <- anyDuplicated(observation[, c("LocationID", "Year", "fMonth")])
  if (n.duplicate == 0) {
    return(list(
      Observation = observation,
      Duplicate = data.frame(
        ObservationID = character(0),
        DatasourceID = integer(0)
      )
    ))
  }

  # locate the replicated observations
  combination <- interaction(
    observation$LocationID,
    observation$Year,
    observation$fMonth,
    drop = TRUE
  )
  n.replicate <- table(combination)
  duplicate <- combination %in% names(which(n.replicate > 1))
  observation.ok <- observation[!duplicate, ]
  observation.toclean <- observation[duplicate, ]
  duplicate.id <- observation.toclean[, c("ObservationID", "DatasourceID")]

  # remove uncomplete replicate if complete replicate exist
  max.complete <- aggregate(
    observation.toclean[, "Complete", drop = FALSE],
    observation.toclean[, c("LocationID", "Year", "fMonth")],
    FUN = max
  )
  observation.toclean <- merge(
    observation.toclean,
    max.complete
  )

  # keep the replicate with largest count
  observation.clean <- ddply(
    observation.toclean,
    c("LocationID", "Year", "fMonth"),
    function(x){
      x[which.max(x$Count), ]
    }
  )
  return(list(
    Observation = rbind(observation.ok, observation.clean),
    Duplicate = duplicate.id
  ))
}
