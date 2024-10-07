#' Select the relevant observations when making the analysis dataset
#'
#' Relevant observations have
#' - At least 4 presences per location
#' - Only locations where the species was present during at least 3 winters
#' - No winters without any presences at the beginning or end of the dataset
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr anti_join bind_rows count distinct filter mutate select
#' semi_join slice_max summarise transmute
#' @importFrom purrr map
#' @importFrom rlang .data
#' @importFrom stats glm coef poisson
#' @importFrom tidyr nest pivot_longer unnest
#' @importFrom tidyselect everything
select_relevant_analysis <- function(observation) {
  if (is.null(observation)) {
    return(list(observation = data.frame(), rare_observation = NULL))
  }
  assert_that(
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "month"), has_name(observation, "location"),
    has_name(observation, "year"), has_name(observation, "observation_id"),
    has_name(observation, "datafield_id")
  )

  relevant <- select_relevant_period(observation = observation)
  if (nrow(relevant$observation) == 0) {
    return(relevant)
  }

  # require the species to be present during 5 different winters at a location
  relevant$observation |>
    filter(!is.na(.data$count), .data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 5) -> to_keep
  relevant$observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(relevant$rare_observation) -> rare_observation
  relevant$observation |>
    semi_join(to_keep, by = "location") -> observation

  # count the number of winters during which a species present for every
  # location and month.
  # count per location the number of months which have a least three winters of
  # presences.
  # keep only locations that have at least two months meeting this criterion
  observation |>
    filter(.data$count > 0) |>
    count(.data$location, .data$month) |>
    filter(.data$n >= 3) |>
    count(.data$location) |>
    filter(.data$n >= 2) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }

  # select months with a median of at least 1 locations per winter
  observation |>
    filter(.data$count > 0) |>
    count(.data$month, .data$year) |>
    complete(.data$month, .data$year, fill = list(n = 0)) |>
    group_by(.data$month) |>
    summarise(median = median(.data$n)) |>
    filter(.data$median >= 1) -> observed_months
  if (nrow(observed_months) == 0) {
    observation |>
      filter(.data$count > 0) |>
      bind_rows(rare_observation) -> rare_observation
    return(
      list(observation = observation[0, ], rare_observation = rare_observation)
    )
  }
  observation |>
    semi_join(observed_months, by = "month") |>
    mutate(month = factor(.data$month)) -> observation
  rare_observation |>
    semi_join(observed_months, by = "month") |>
    mutate(
      month = factor(.data$month, levels = levels(observation$month))
    ) -> rare_observation

  # select months with have on average at least 5% of the top month
  if (length(unique(observation$month)) > 1) {
    observation |>
      filter(!is.na(count)) |>
      glm(formula = count ~ 0 + month, family = poisson) |>
      coef() -> month_coef
    data.frame(
      month = gsub("month", "", names(month_coef)), estimate = month_coef
    ) |>
      mutate(estimate = .data$estimate - max(.data$estimate)) |>
      filter(.data$estimate >= log(0.05)) -> to_keep
    observation |>
      semi_join(to_keep, by = "month") |>
      mutate(month = factor(.data$month)) -> observation
    rare_observation |>
      semi_join(to_keep, by = "month") |>
      mutate(
        month = factor(.data$month, levels = levels(observation$month))
      ) -> rare_observation
  }

  observation |>
    filter(.data$count > 0) |>
    nest(.by = "location") |>
    transmute(
      .data$location,
      range = map(.data$data, ~mutate(.x, year = factor(year))) |>
        map(glm, formula = count ~ 0 + year, family = poisson) |>
        map(coef) |>
        map(t) |>
        map(data.frame) |>
        map(
          pivot_longer, cols = everything(), names_to = "year",
          values_to = "estimate"
        )
    ) |>
    unnest("range") |>
    filter(grepl("year", .data$year)) |>
    group_by(.data$location) |>
    slice_max(.data$estimate, n = 5, with_ties = TRUE) |>
    summarise(
      delta = range(.data$estimate) |>
        diff() |>
        exp()
    ) |>
    filter(1 / .data$delta > 0.1) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation

  relevant <- select_relevant_period(
    observation = observation, rare_observation = rare_observation
  )
  if (nrow(relevant$observation) == 0) {
    return(relevant)
  }

  relevant$observation |>
    distinct(.data$location) |>
    nrow() -> n_location
  if (n_location < 6) {
    relevant$observation |>
      filter(.data$count > 0) |>
      bind_rows(relevant$rare_observation) -> relevant$rare_observation
    relevant$observation <- relevant$observation[0, ]
  }

  return(relevant)
}

#' @importFrom dplyr bind_rows filter summarise
#' @importFrom rlang .data
select_relevant_period <- function(observation, rare_observation = NULL) {
  # remove winters with less than 5 observations at the start or end of the data
  # the dataset should be a least 5 years long
  observation |>
    filter(.data$count > 0) |>
    count(.data$year) |>
    filter(.data$n >= 5) -> relevant
  if (nrow(relevant) < 5) {
    observation |>
      filter(.data$count > 0) |>
      bind_rows(rare_observation) -> rare_observation
    return(
      list(observation = observation[0, ], rare_observation = rare_observation)
    )
  }
  relevant |>
    summarise(start = min(.data$year), end = max(.data$year)) -> ranges
  if (ranges$end - ranges$start < 4) {
    observation |>
      filter(.data$count > 0) |>
      bind_rows(rare_observation) -> rare_observation
    return(
      list(observation = observation[0, ], rare_observation = rare_observation)
    )
  }
  observation |>
    filter(ranges$start <= .data$year, .data$year <= ranges$end) -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}
