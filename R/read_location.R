#' read the dataset of locations from the database
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom assertthat assert_that noNA
#' @importFrom DBI dbGetQuery dbQuoteString
#' @importFrom git2rdata read_vc
#' @importFrom dplyr bind_rows filter mutate semi_join transmute
#' @importFrom rlang .data
read_location <- function(
  flemish_channel, walloon_repo, first_date, latest_date
) {
  assert_that(
    inherits(first_date, "POSIXct"), length(first_date) == 1, noNA(first_date),
    inherits(latest_date, "POSIXct"), length(latest_date) == 1,
    noNA(latest_date)
  )

  sprintf(
    "WITH cte_spa AS (
      SELECT
        LocationWVKey,
          MAX(
          CASE
            WHEN LocationGroupTypeCode = 'EVRL' THEN 1
            WHEN LocationGroupTypeCode = 'EHRL' THEN 1
            ELSE 0 END
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
        lp.StartDate AS start_date,
        lp.EndDate AS end_date,
        cs.SPA AS natura2000,
        'Flanders' AS region
      FROM DimLocationWV AS lp
      INNER JOIN cte_survey AS cv ON cv.LocationWVKey = lp.LocationWVKey
      INNER JOIN cte_parent AS cp ON cp.parent = lp.LocationWVKey
      LEFT JOIN cte_spa AS cs ON cs.LocationWVKey = lp.LocationWVKey
    ",
    format(first_date, "%Y-%m-%d") |>
      dbQuoteString(conn = flemish_channel),
    format(latest_date, "%Y-%m-%d") |>
      dbQuoteString(conn = flemish_channel)
  ) |>
    dbGetQuery(conn = flemish_channel) -> location
  future <- !is.na(location$end_date) & location$end_date > latest_date
  location$end_date[future] <- NA

  # Read Walloon data from the git repository
  read_vc(file = "visit", root = walloon_repo) |>
    filter(first_date <= .data$date, .data$date <= latest_date) |>
    semi_join(
      x = read_vc(file = "location", root = walloon_repo),
      by = c("id" = "site")
    ) |>
    transmute(
      external_code = .data$id, description = .data$name,
      .data$natura2000, region = "Wallonia"
    ) |>
    bind_rows(location) |>
    transmute(
      id = substring(.data$region, 1, 1) |>
        paste(.data$external_code, sep = "_"),
      region = factor(.data$region, levels = c("Flanders", "Wallonia")),
      .data$external_code, .data$start_date, .data$end_date,
      natura2000 = pmax(0, .data$natura2000, na.rm = TRUE), .data$description
    )
}
