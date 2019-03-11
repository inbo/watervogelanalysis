#' read the dataset of locations from the database
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom assertthat assert_that
#' @importFrom dplyr %>% mutate filter semi_join transmute bind_rows
#' @importFrom rlang .data
#' @importFrom git2rdata read_vc
#' @importFrom DBI dbGetQuery dbQuoteString
read_location <- function(
  result_channel, flemish_channel, walloon_repo, latest_date
){
  assert_that(inherits(latest_date, "POSIXct"), length(latest_date) == 1)
  # read Flemish data from the database
  datasource_id <- datasource_id_flanders(result_channel = result_channel)

  format(latest_date, "%Y-%m-%d") %>%
    dbQuoteString(conn = flemish_channel) %>%
    sprintf(
      fmt = "WITH cte_survey AS (
        SELECT LocationWVKey
        FROM FactAnalyseSetOccurrence
        WHERE SampleDate <= %s
        GROUP BY LocationWVKey
      ),
      cte AS (
        SELECT
          l.LocationWVCode AS external_code,
          l.locationWVNaam AS description,
          l.StartDate,
          l.EndDate,
          CASE
            WHEN f.LocationGroupTypeCode = 'EVRL'
            THEN 1 ELSE 0 END AS SPA
        FROM cte_survey AS c
        INNER JOIN DimLocationWV AS l ON c.LocationWVKey = l.LocationWVKey
        LEFT JOIN FactLocationGROUP AS f ON l.LocationWVCode = f.LocationWVCode
        WHERE l.LocationWVCode <> '-'
      )

      SELECT external_code, description, StartDate, EndDate, MAX(SPA) AS SPA
      FROM cte
      GROUP BY external_code, description, StartDate, EndDate"
    ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    mutate(
      datasource = datasource_id,
      SPA = pmax(0, .data$SPA, na.rm = TRUE),
      Region = "Flanders"
    ) -> location
  future <- !is.na(location$EndDate) & location$EndDate > latest_date
  location$EndDate[future] <- NA

  # Read Walloon data from the git repository
  read_vc(file = "visit", root = walloon_repo) %>%
    filter(.data$Date <= latest_date) %>%
    semi_join(
      x = read_vc(file = "location", root = walloon_repo),
      by = "LocationID"
    ) %>%
    transmute(
      external_code = .data$LocationID,
      description = .data$LocationName,
      .data$SPA,
      datasource = datasource_id_wallonia(result_channel = result_channel),
      Region = "Wallonia"
    ) %>%
    bind_rows(location)
}
