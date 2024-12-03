#' Select the relevant observations when making the analysis dataset
#'
#' Relevant observations have
#' - At least 4 presences per location
#' - Only locations where the species was present during at least 3 winters
#' - No winters without any presences at the beginning or end of the dataset
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr bind_rows distinct filter inner_join pull
#' @importFrom rlang .data
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

  relevant <- select_relevant_period(
    observation = observation, n_winters = 5, n_observations = 5
  )
  relevant <- select_min_winter_location(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_winters = 5
  )
  relevant <- select_month_location(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_locations = 1
  )
  relevant <- select_relevant_month(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, threshold = 0.05
  )
  relevant <- select_multi_month(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_winters = 3, n_months = 2
  )
  relevant <- select_min_winter_location(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_winters = 5
  )
  relevant <- select_top_ratio(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_rank = 5, max_ratio = 10
  )
  relevant <- select_min_season(
    observation = relevant$observation, n_winters = 2, fraction = 0.5,
    rare_observation = relevant$rare_observation
  )
  relevant <- select_location_winter(
    observation = relevant$observation, min_winters = 3, min_months = 2,
    rare_observation = relevant$rare_observation
  )
  relevant <- select_relevant_period(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation
  )
  relevant <- select_multi_month(
    observation = relevant$observation,
    rare_observation = relevant$rare_observation, n_winters = 3, n_months = 2
  )

  # relevant observations need at least 6 locations
  relevant$observation |>
    distinct(.data$location) |>
    nrow() -> n_location
  if (n_location < 6) {
    relevant$observation |>
      filter(.data$count > 0) |>
      bind_rows(relevant$rare_observation) -> relevant$rare_observation
    relevant$observation <- relevant$observation[0, ]
    return(relevant)
  }

  # don't impute when nearest observation at the location is more than 5 years
  # away
  relevant$observation |>
    filter(.data$minimum > 0) |>
    distinct(.data$location, observed = .data$year) |>
    inner_join(
      relevant$observation |>
        filter(is.na(.data$count)),
      by = "location", relationship = "many-to-many"
    ) |>
    filter(abs(.data$observed - .data$year) <= 5) |>
    distinct(.data$observation_id) |>
    pull("observation_id") -> to_impute
  relevant$observation |>
    filter(
      !is.na(.data$count) | .data$observation_id %in% to_impute
    ) -> relevant$observation

  return(relevant)
}

#' Remove winters with few observations at the start or end of the dataset
#' @param observation A data frame with the observations
#' @param rare_observation A data frame with the rare observations
#' @param n_observations The minimum number of observations per year.
#' Defaults to 5.
#' @param n_winters The minimum number of winters with observations.
#' Defaults to 5.
#' @importFrom assertthat assert_that has_name is.count
#' @importFrom dplyr bind_rows filter summarise
#' @importFrom rlang .data
#' @noRd
select_relevant_period <- function(
  observation, rare_observation = NULL, n_observations = 5, n_winters = 5
) {
  assert_that(
    is.count(n_observations), is.count(n_winters),
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(.data$count > 0) |>
    count(.data$year) |>
    filter(.data$n >= n_observations) -> relevant
  if (nrow(relevant) < n_winters) {
    observation |>
      filter(.data$count > 0) |>
      bind_rows(rare_observation) -> rare_observation
    return(
      list(observation = observation[0, ], rare_observation = rare_observation)
    )
  }
  relevant |>
    summarise(start = min(.data$year), end = max(.data$year)) -> ranges
  if (ranges$end - ranges$start < n_winters - 1) {
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


#' Keep locations depending on the number of winters with observations
#' @inheritParams select_relevant_period
#' @noRd
#' @importFrom assertthat assert_that has_name is.count
#' @importFrom dplyr anti_join bind_rows count distinct filter semi_join
#' @importFrom rlang .data
select_min_winter_location <- function(
  observation, rare_observation = NULL, n_winters = 5
) {
  assert_that(
    is.count(n_winters),
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "location"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(!is.na(.data$count), .data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= n_winters) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}

#' Select months based on the median number of locations over the winters
#' @inheritParams select_relevant_period
#' @param n_locations Keep only months where the median number of locations
#' with observations is at least this number.
#' Defaults to 1.
#' @noRd
#' @importFrom assertthat assert_that has_name is.number noNA
#' @importFrom dplyr bind_rows count filter group_by mutate semi_join summarise
#' @importFrom rlang .data
#' @importFrom tidyr complete
select_month_location <- function(
    observation, rare_observation = NULL, n_locations = 1
) {
  assert_that(
    is.number(n_locations), noNA(n_locations), n_locations > 0,
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "month"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(.data$count > 0) |>
    count(.data$month, .data$year) |>
    complete(.data$month, .data$year, fill = list(n = 0)) |>
    group_by(.data$month) |>
    summarise(median = median(.data$n)) |>
    filter(.data$median >= n_locations) -> observed_months
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
  return(list(observation = observation, rare_observation = rare_observation))
}


#' Select months with a sufficient number of counts
#' @inheritParams select_relevant_period
#' @param threshold The minimum average number of counts for a relevant month.
#' Defaults to 5% of the month with the largest average.
#' @noRd
#' @importFrom assertthat assert_that has_name is.number noNA
#' @importFrom dplyr filter mutate semi_join
#' @importFrom rlang .data
#' @importFrom stats glm coef
select_relevant_month <- function(
  observation, rare_observation, threshold = 0.05
) {
  assert_that(
    is.number(threshold), noNA(threshold), 0 < threshold, threshold <= 1,
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "month"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0 || length(unique(observation$month)) <= 1) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
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
  return(list(observation = observation, rare_observation = rare_observation))
}

#' Select locations with multiple months
#'
#' Do nothing when the data contains only a single month.
#'
#' Otherwise, start by removing all locations only observed during a single
#' month.
#' Next, count the number of winters during which a species present for every
#' location and month.
#' count per location the number of months which have a least `n_winters`
#' winters of presences.
#' Keep only locations that have at least `n_months` months meeting this
#' criterion.
#' @inheritParams select_relevant_period
#' @param n_winters Minimal number of winters.
#' Defaults to 3.
#' @param n_months Minimal number of months.
#' Defaults to 2.
#' @noRd
#' @importFrom assertthat assert_that has_name is.number noNA
#' @importFrom dplyr anti_join bind_rows count distinct filter semi_join
#' @importFrom rlang .data
select_multi_month <- function(
  observation, rare_observation = NULL, n_winters = 3, n_months = 2
) {
  assert_that(
    is.count(n_winters), is.count(n_months),
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "month"), has_name(observation, "location"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0 || length(unique(observation$month)) <= 1) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(!is.na(.data$count)) |>
    distinct(.data$month, .data$location) |>
    count(.data$location) |>
    filter(.data$n > 1) |>
    semi_join(x = observation, by = "location") -> observation
  observation |>
    filter(.data$count > 0) |>
    count(.data$location, .data$month) |>
    filter(.data$n >= n_winters) |>
    count(.data$location) |>
    filter(.data$n >= n_months) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}

#' Keep locations based on the ratio between the high ranking winters
#' @inheritParams select_relevant_period
#' @param n_rank Which top winter to compare to the top winter.
#' Defaults to 5.
#' @param max_ratio The maximum ratio between the highest and lowest
#' estimates.
#' Defaults to 10.
#' Calculate for every location and winter the ratio between the highest and
#' `n_rank` highest estimate.
#' Keep only locations where this ratio is less than `max_ratio`.
#' @noRd
#' @importFrom assertthat assert_that has_name is.number noNA
#' @importFrom dplyr anti_join bind_rows filter group_by semi_join slice_max
#' summarise transmute
#' @importFrom purrr map
#' @importFrom rlang .data
#' @importFrom stats glm coef poisson
#' @importFrom tidyr nest pivot_longer unnest
#' @importFrom tidyselect everything
select_top_ratio <- function(
  observation, rare_observation = NULL, n_rank = 5, max_ratio = 10
) {
  assert_that(
    is.count(n_rank), n_rank > 1, is.number(max_ratio), noNA(max_ratio),
    max_ratio > 1,
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "location"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
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
    slice_max(.data$estimate, n = n_rank, with_ties = TRUE) |>
    summarise(
      delta = range(.data$estimate) |>
        diff() |>
        exp()
    ) |>
    filter(.data$delta < max_ratio) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}

#' Remove rare observations based on the number of descend surveyed winters
#' @inheritParams select_relevant_period
#' @param fraction The minimum fraction of the winter with observations.
#' Defaults to 0.5.
#' @param n_winters The minimum number of sufficiently monitored winters.
#' Defaults to 2.
#' Calculate for every location and winter the fraction of the winter with
#' observations.
#' Keep only winters where the fraction is at least `fraction`.
#' Count the number of these winters per location.
#' Keep only locations with at least `n_winters` winters.
#' @noRd
#' @importFrom assertthat assert_that has_name is.number noNA
#' @importFrom dplyr anti_join bind_rows count filter group_by summarise
#' @importFrom rlang .data
select_min_season <- function(
  observation, rare_observation = NULL, fraction = 0.5, n_winters = 2
) {
  assert_that(
    is.number(fraction), noNA(fraction), 0 < fraction, fraction <= 1,
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "location"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(!is.na(.data$count)) |>
    group_by(.data$location, .data$year) |>
    summarise(season_duration = mean(.data$count > 0), .groups = "drop") |>
    filter(.data$season_duration >= fraction) |>
    count(.data$location) |>
    filter(.data$n >= n_winters) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}

#' Select locations based on the number of winters with observations
#' @inheritParams select_relevant_period
#' @param min_months The minimum number of months with observations.
#' Defaults to 2.
#' @param min_winters The minimum number of winters with observations.
#' Defaults to 2.
#' @noRd
#' @importFrom assertthat assert_that has_name is.count
#' @importFrom dplyr anti_join bind_rows count filter semi_join
#' @importFrom rlang .data
select_location_winter <- function(
  observation, rare_observation = NULL, min_months = 2, min_winters = 2
) {
  assert_that(
    is.count(min_winters), is.count(min_months),
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "year"), has_name(observation, "location"),
    is.null(rare_observation) || inherits(rare_observation, "data.frame")
  )
  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }
  observation |>
    filter(.data$count > 0) |>
    count(.data$location, .data$year) |>
    filter(.data$n >= min_months) |>
    count(.data$location) |>
    filter(.data$n >= min_winters) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation
  return(list(observation = observation, rare_observation = rare_observation))
}
