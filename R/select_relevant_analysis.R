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
#' @importFrom stats glm coef
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

  # require the species to be present during 5 different winters at a location
  observation |>
    filter(!is.na(.data$count), .data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 5) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(.data$count > 0) -> rare_observation
  observation |>
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

  # select months with have on average at least 5% of the top month
  if (length(unique(observation$month)) > 1) {
    observation |>
      filter(!is.na(count)) |>
      glm(formula = count ~ 0 + month, family = poisson) |>
      coef() -> month_coef
    names(month_coef) <- gsub("month", "", names(month_coef))
    to_keep <- exp(month_coef - max(month_coef)) >= 0.05
    rare_observation <- rare_observation[
      rare_observation$month %in% names(to_keep),
    ]
    observation <- observation[observation$month %in% names(to_keep), ]
  }

  observation |>
    filter(.data$count > 0) |>
    nest(.by = "location") |>
    transmute(
      .data$location,
      range = map(.data$data, ~mutate(.x, year = factor(year))) |>
        map(glm, formula = count ~ 0 + year, family = poisson) |>
        map(coefficients) |>
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

  # remove winters without data at the start or end of the data
  observation |>
    filter(.data$count > 0) |>
    summarise(start = min(.data$year), end = max(.data$year)) -> ranges
  observation |>
    filter(ranges$start <= .data$year, .data$year <= ranges$end) -> observation

  if (nrow(observation) == 0) {
    return(list(observation = observation, rare_observation = rare_observation))
  }

  observation |>
    distinct(.data$location) |>
    nrow() -> n_location
  if (n_location < 6) {
    observation |>
      filter(.data$count > 0) |>
      bind_rows(rare_observation) -> rare_observation
    observation <- observation[0, ]
  }

  return(list(observation = observation, rare_observation = rare_observation))
}
