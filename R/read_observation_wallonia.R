#' Read the Wallon observations from a species
#'
#' All available imported data is used.
#' @inheritParams read_observation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom git2rdata read_vc
#' @importFrom lubridate round_date year
#' @importFrom assertthat assert_that is.count
#' @importFrom dplyr %>% filter mutate left_join transmute
#' @importFrom rlang .data
read_observation_wallonia <- function(species_id, first_year, latest_year, walloon_repo) {
  assert_that(is.count(species_id), is.count(first_year), is.count(latest_year))
  first_year <- as.integer(first_year)
  latest_year <- as.integer(latest_year)

  read_vc(file = "data", root = walloon_repo) %>%
    filter(.data$euringcode == species_id) %>%
    mutate(Count = as.integer(.data$Count)) -> data

  if (nrow(data) == 0) {
    return(NULL)
  }

  read_vc(file = "visit", root = walloon_repo) %>%
    mutate(
      Month = as.integer(format(.data$Date, "%m")),
      Year = as.integer(format(.data$Date, "%Y")) +
        as.integer(.data$Month >= 7)
    ) %>%
    filter(first_year <= .data$Year, .data$Year <= latest_year) %>%
    left_join(
      data,
      by = "OriginalObservationID"
    ) %>%
    transmute(
      ObservationID = ifelse(is.na(.data$Count), .data$ObservationID.x,
                             .data$ObservationID.y),
      TableName = ifelse(is.na(.data$Count), "visit", "data"),
      external_code = .data$LocationID, .data$Year, .data$Month,
      Count = pmax(0L, .data$Count, na.rm = TRUE),
      Complete = 1L
    )
}
