#' read the dataset of locations from the database
#' @inheritParams read_specieslist
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_id odbc_connect git_connect
#' @importFrom git2rdata read_vc
#' @importFrom DBI dbGetQuery
#' @examples
#' \dontrun{
#' result.channel <- n2khelper::connect_result()
#' flemish.channel <- connect_flemish_source(result.channel = result.channel)
#' walloon.connection <- connect_walloon_source(
#'   result.channel = result.channel,
#'   username = "Someone",
#'   password = "xxxx",
#'   commit.user = "Someone",
#'   commit.email = "some\\u0040one.com"
#' )
#' location <- read_location(
#'   result.channel = result.channel,
#'   flemish.channel = flemish.channel,
#'   walloon.connection = walloon.connection
#' )
#' head(location)
#' }
read_location <- function(
  result_channel, flemish_channel, walloon_repo, import_date
){
  assert_that(
    inherits(import_date, "POSIXct"),
    length(import_date) == 1
  )
  # read Flemish data from the database
  datasource_id <- datasource_id_flanders(result.channel = result_channel)

  format(import_date, "%Y-%m-%d") %>%
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
  future <- !is.na(location$EndDate) & location$EndDate > import_date
  location$EndDate[future] <- NA

  # Read Walloon data from the git repository
  read_vc(file = "visit.txt", root = walloon_repo) %>%
    filter(.data$Date <= import_date) %>%
    semi_join(
      x = read_vc(file = "location.txt", root = walloon_repo),
      by = "LocationID"
    ) %>%
    transmute(
      external_code = .data$LocationID,
      description = .data$LocationName,
      .data$SPA,
      datasource = datasource_id_wallonia(result.channel = result_channel),
      Region = "Wallonia"
    ) %>%
    bind_rows(location)

  return(location)
}
