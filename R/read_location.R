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
  result_channel, flemish_channel, walloon_repo, first_date, latest_date
){
  assert_that(inherits(first_date, "POSIXct"), length(first_date) == 1,
              inherits(latest_date, "POSIXct"), length(latest_date) == 1)
  # read Flemish data from the database
  datasource_id <- datasource_id_flanders(result_channel = result_channel)

  sprintf(
    "WITH cte_spa AS (
      SELECT
        LocationWVKey,
          MAX(
          CASE
            WHEN LocationGroupTypeCode = 'EVRL'
            THEN 1 ELSE 0 END
          ) AS SPA
      FROM FactLocationGroup
      GROUP BY LocationWVKey
    ),
    cte_parent AS (
      SELECT DISTINCT
          CAST(
            COALESCE(ParentLocationWVKey, LocationWVKey) AS VARCHAR
          ) AS parent
      FROM DimLocationWVParent
    ),
    cte_survey AS (
      SELECT DISTINCT LocationWVKey
      FROM FactAnalyseSetOccurrence
      WHERE %s <= SampleDate AND SampleDate <= %s
    )

    SELECT
        lp.LocationWVCode AS external_code,
        lp.locationWVNaam AS description,
        lp.StartDate,
        lp.EndDate,
        cs.SPA
      FROM DimLocationWV AS lp
      INNER JOIN cte_survey AS cv ON cv.LocationWVKey = lp.LocationWVKey
      INNER JOIN cte_parent AS cp ON cp.parent = lp.LocationWVKey
      LEFT JOIN cte_spa AS cs ON cs.LocationWVKey = lp.LocationWVKey
    ",
    format(first_date, "%Y-%m-%d") %>%
      dbQuoteString(conn = flemish_channel),
    format(latest_date, "%Y-%m-%d") %>%
      dbQuoteString(conn = flemish_channel)
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
    filter(first_date <= .data$Date, .data$Date <= latest_date) %>%
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
