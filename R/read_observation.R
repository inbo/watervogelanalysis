#' Read the Flemish observations from a species
#'
#' The selection uses several constraints. The user defined constraints are \code{first.winter} and \code{species.covered}. Following contraints are imposed at the same time as the user defined contraints:
#'\itemize{
#'  \item The observation is validated
#'  \item The observation is a midmonthly observation
#'  \item The observation is not 'unobserved'
#'  \item The location is 'active' and the observation date is within the 'active' period of the location
#'  \item The observation is between october and march
#'}
#' @param species_id The id of the species
#' @inheritParams prepare_dataset
#' @return A \code{data.frame} with observations. \code{Complete= 1} indicates that the entire location was surveyed.
#' @export
#' @importFrom assertthat assert_that is.count
#' @importFrom DBI dbQuoteString dbQuoteLiteral dbGetQuery
#' @importFrom dplyr %>% group_by arrange slice ungroup desc
#' @importFrom rlang .data
read_observation <- function(species_id, first_year, latest_year, flemish_channel) {
  assert_that(is.count(species_id), is.count(first_year), is.count(latest_year))
  species_id <- as.integer(species_id)
  latest_year <- as.integer(latest_year)

  sprintf("
    SELECT
      f.OccurrenceKey AS ObservationID,
      YEAR(f.SampleDate) + IIF(MONTH(f.SampleDate) >= 7, 1, 0) AS Year,
      MONTH(f.SampleDate) AS Month,
      l.LocationWVCode AS external_code,
      f.TaxonCount AS Count,
      'FactAnalyseSetOccurrence' AS TableName,
      CASE WHEN s.CoverageCode = 'V' THEN 1
           ELSE 0 END AS Complete
    FROM FactAnalyseSetOccurrence AS f
    INNER JOIN DimLocationWV AS l ON l.locationwvkey = f.locationwvkey
    INNER JOIN DimAnalyseSet AS a ON  a.analysesetkey = f.analysesetkey
    INNER JOIN DimSample AS s ON f.samplekey = s.samplekey
    WHERE
      %s <= f.SampleDate AND f.SampleDate <= %s AND f.TaxonWVKey = %s AND
      analysesetcode LIKE 'MIDMA%%' AND s.CoverageCode IN ('V', 'O')",
    sprintf("%i-10-01", first_year - 1) %>%
      dbQuoteString(conn = flemish_channel),
    sprintf("%i-06-30", latest_year) %>%
      dbQuoteString(conn = flemish_channel),
    dbQuoteLiteral(flemish_channel, species_id)
  ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    group_by(.data$Year, .data$Month, .data$external_code) %>%
    arrange(desc(.data$Count)) %>%
    slice(1) %>%
    ungroup()
}
