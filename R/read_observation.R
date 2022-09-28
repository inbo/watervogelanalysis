#' Read the Flemish observations from a species
#'
#' The selection uses several constraints.
#' The user defined constraints are `first.winter` and  `species.covered`.
#' Following constraints are imposed at the same time as the user defined
#' constraints:
#'  - The observation is validated.
#'  - The observation is a mid monthly observation.
#'  - The observation is not 'unobserved'.
#'  - The location is 'active' and the observation date is within the 'active'
#'    period of the location.
#'  - The observation is between October and March.
#'
#' @param species_id The id of the species
#' @inheritParams prepare_dataset
#' @return A `data.frame` with observations.
#' The function only returns observations based on parent locations.
#' Following rules apply for each combination of parent location, year and
#' month.
#'
#' 1. If the parent location has an observation, then return this observation.
#' 1. If some child location have observation, then return the sum of counts.
#' 1. If neither of the parent or child location has observations,
#'    then the counts are missing.
#'
#' `Complete = 1` indicates that the entire location was surveyed.
#' In case of child locations, all child locations were surveyed in full.
#' `Complete = 0` indicates the location was only partially surveyed.
#' @export
#' @importFrom assertthat assert_that is.count
#' @importFrom DBI dbQuoteString dbQuoteLiteral dbGetQuery
#' @importFrom dplyr %>% group_by arrange slice ungroup desc add_count
#' @importFrom rlang .data
read_observation <- function(
  species_id, first_year, latest_year, flemish_channel) {
  assert_that(is.count(species_id), is.count(first_year), is.count(latest_year))
  species_id <- as.integer(species_id)
  latest_year <- as.integer(latest_year)

  sprintf("
    SELECT
      f.OccurrenceKey AS ObservationID,
      d.YearNumber + IIF(d.MonthNumber >= 7, 1, 0) AS Year,
      d.MonthNumber AS Month,
      l.LocationWVCode AS external_code,
      l.LocationWVKey,
      f.TaxonCount AS Count,
      'FactAnalyseSetOccurrence' AS TableName,
      CASE WHEN s.CoverageCode = 'V' THEN 1
           ELSE 0 END AS Complete
    FROM FactAnalyseSetOccurrence AS f
    INNER JOIN DimLocationWV AS l ON l.locationwvkey = f.locationwvkey
    INNER JOIN DimAnalyseSet AS a ON  a.analysesetkey = f.analysesetkey
    INNER JOIN DimSample AS s ON f.samplekey = s.samplekey
    INNER JOIN DimDate AS d ON f.SampleDatekey = d.Datekey
    WHERE
      %s <= f.SampleDate AND f.SampleDate <= %s AND f.TaxonWVKey = %s AND
      a.AnalysesetCode LIKE 'MIDMA%%' AND s.CoverageCode IN ('V', 'O')",
    sprintf("%i-10-01", first_year - 1) %>%
      dbQuoteString(conn = flemish_channel),
    sprintf("%i-06-30", latest_year) %>%
      dbQuoteString(conn = flemish_channel),
    dbQuoteLiteral(flemish_channel, species_id)
  ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    group_by(.data$Year, .data$Month, .data$external_code) %>%
    arrange(desc(.data$Complete), desc(.data$Count), .data$ObservationID) %>%
    slice(1) %>%
    ungroup() -> raw_observation
  "SELECT
    p.LocationWVKey, p.ParentLocationWVKey, l.LocationWVCode AS extrenal_code
  FROM DimLocationWVParent AS p
  INNER JOIN DimLocationWV AS l ON p.ParentLocationWVKey = l.LocationWVKey
  WHERE ParentLocationWVKey IS NOT NULL" %>%
    dbGetQuery(conn = flemish_channel) -> parents
  parents %>%
    add_count(.data$ParentLocationWVKey, name = "target") %>%
    inner_join(raw_observation, by = "LocationWVKey") %>%
    add_count(.data$external_code, .data$Year, .data$Month,
              name = "current") %>%
    group_by(.data$external_code, .data$Year, .data$Month, .data$TableName) %>%
    summarise(
      Count = sum(.data$Count),
      Complete = min(.data$Complete, .data$current == .data$target),
      ObservationID = min(.data$ObservationID)
    ) %>%
    ungroup() %>%
    anti_join(raw_observation, by = c("external_code", "Year", "Month")) %>%
    bind_rows(
      raw_observation %>%
        anti_join(parents, by = "LocationWVKey") %>%
        select(-"LocationWVKey")
    )
}
