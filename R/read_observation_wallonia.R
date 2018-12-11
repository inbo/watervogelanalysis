#' Read the Wallon observations from a species
#'
#' All available imported data is used. The only constraint is that the observation are not older than \code{first.winter} and originate form november up to februari.
#' @inheritParams read_observation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom git2rdata read_vc
#' @importFrom lubridate round_date year
#' @importFrom assertthat assert_that is.string is.count
#' @importFrom dplyr %>% filter_ mutate_ full_join transmute_ arrange_
read_observation_wallonia <- function(
  species.id,
  first.winter,
  last.winter,
  walloon.connection
){
  assert_that(is.string(species.id))
  assert_that(is.count(first.winter))
  assert_that(is.count(last.winter))
  first.winter <- as.integer(first.winter)
  last.winter <- as.integer(last.winter)

  data <- read_vc(file = "data.txt", root = walloon.connection) %>%
    filter_(~NBNKey == species.id)

  if (nrow(data) == 0) {
    return(NULL)
  }

  observation <- read_vc(file = "visit.txt", root = walloon.connection) %>%
    mutate_(
      Date = ~as.Date(Date) %>%
        as.POSIXct(),
      Month = ~format(Date, "%m"),
      Winter = ~round_date(Date, unit = "year") %>%
        year()
    ) %>%
    filter_(
      ~Month %in% c("11", "12", "01", "02"),
      ~Winter >= first.winter,
      ~Winter <= last.winter
    ) %>%
    full_join(
      data,
      by = "OriginalObservationID"
    ) %>%
    transmute_(
      ~ObservationID,
      ~LocationID,
      ~Date,
      Count = ~ifelse(is.na(Count), 0, Count),
      Complete = ~1
    ) %>%
    arrange_(~ObservationID)

  return(observation)
}
