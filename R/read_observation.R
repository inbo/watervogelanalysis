#' Read the Flemish observations from a species
#' 
#' The selection uses several constraints. The user defined constraints are \code{first.winter} and \code{species.covered}. Following contraints are imposed at the same time as the user defined contraints:
#'\itemize{
#'  \item The observation is valided
#'  \item The observation is a midmonthly observation
#'  \item The observation is not 'unobserved'
#'  \item The location is 'active' and the observation date is within the 'active' period of the location
#'  \item The observation is between october and march
#'}
#' @param species.id The id of the species
#' @param first.winter The oldest winter from which the observation are relevant. Observation prior to \code{first.winter} are ignored
#' @param species.covered A vector with codes of relevant methodes
#' @inheritParams n2khelper::odbc_connect
#' @return A \code{data.frame} with observations. \code{Complete= 1} indicates that the entire location was surveyed. 
#' @export
#' @importFrom n2khelper check_single_strictly_positive_integer
#' @importFrom RODBC sqlQuery odbcClose
#' @examples
#' \dontrun{
#' observation <- read_observation(
#'   species.id = 4860, 
#'   first.winter = 1992, 
#'   species.covered = c("w", "weg", "wegm", "wem", "st")
#' )
#' head(observation)
#' }
read_observation <- function(species.id, first.winter, species.covered, develop = TRUE){
  species.id <- check_single_strictly_positive_integer(species.id)
  first.winter <- check_single_strictly_positive_integer(first.winter)
  channel <- odbc_connect(data.source.name = "Raw data watervogels Flanders", develop = develop)
  species.covered.sql <- paste0(
    "SoortenTellingCode IN (",
    paste0(
      paste0("'", species.covered, "'"),
      collapse = ", "
    ),
    ")"
  )
  sql <- paste0("
    SELECT
      ObservationID,
      LocationID,
      Date,
      Complete,
      Count
    FROM
        (
          SELECT 
            WaarnemingID AS SpeciesObservationID, 
            Aantal AS Count
          FROM 
            tblWaarnemingSoort 
          WHERE 
            EuringNummer = ", species.id, "
        ) AS counts
      RIGHT JOIN
        (
          SELECT
            ID AS ObservationID,
            TelDatum AS Date,
            CASE 
              WHEN UPPER(TelvolledigheidCode) = 'O' 
              THEN 0
              ELSE 1
            END AS Complete,
            location.LocationID
          FROM
              tblWaarneming
            INNER JOIN
              (
                SELECT
                  Code AS LocationID,
                  BeginDatum AS StartDate,
                  EindDatum AS EndDate
                FROM
                  tblGebied
                WHERE
                  Actief = 1
              ) AS location
            ON
              tblWaarneming.GebiedCode = location.LocationID
          WHERE
            MidmaandelijkseTelling = 1 AND
            Gevalideerd = 1 AND 
            ( -- use all observations except those which are not observed
              NOT TelvolledigheidCode = 'N' OR
              TelvolledigheidCode IS NULL
            ) AND
            ( -- only observations from october until march
              datepart(m, TelDatum) <= 3 OR
              datepart(m, TelDatum) >= 10
            ) AND ",
            species.covered.sql, " AND
            TelDatum >= '", first.winter - 1, "/10/01' AND
            (
              TelDatum >= StartDate OR
              StartDate IS NULL
            ) AND (
              TelDatum <= EndDate OR
              EndDate IS NULL
            )
        ) AS observation
      ON
        counts.SpeciesObservationID = observation.ObservationID
  "
  )
  observation <- sqlQuery(channel = channel, query = sql, stringsAsFactors = FALSE)
  odbcClose(channel)
  
  observation$Count[is.na(observation$Count)] <- 0
  
  observation <- observation[order(observation$ObservationID), ]
  return(observation)
}
