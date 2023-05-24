#' Select the relevant observations when making the analysis dataset
#'
#' Relevant observations have
#' - At least 4 presences per location
#' - Only locations where the species was present during at least 3 winters
#' - No winters without any presences at the beginning or end of the dataset
#' @inheritParams n2kanalysis::select_factor_threshold
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr anti_join arrange bind_rows count distinct filter group_by lag lead left_join mutate n select semi_join summarise transmute
#' @importFrom rlang .data
#' @importFrom stats glm coef
select_relevant_analysis <- function(observation) {
  if (is.null(observation)) {
    return(list(observation = data.frame(), rare_observation = NULL))
  }
  assert_that(
    inherits(observation, "data.frame"), has_name(observation, "count"),
    has_name(observation, "month"), has_name(observation, "location"),
    has_name(observation, "year")
  )

  # require the species to be present during 5 different years at a location
  observation |>
  filter(!is.na(.data$count), .data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 5) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") -> rare_observation
  # calculate the first, second, penultimate and last year with occurrence for
  # every location
  # keep all counts from the second to the penultimate year
  # keep the first year only if it one year before the second year
  # keep the last year only if it one year later than the penultimate year
  observation |>
    filter(!is.na(.data$count), .data$count > 0) |>
    distinct(.data$location, .data$year) |>
    group_by(.data$location) |>
    filter(n() >= 5) |>
    arrange(.data$year) |>
    summarise(
      first = min(.data$year),
      second  = min(lead(.data$year, n = 1), na.rm = TRUE),
      penultimate = max(lag(.data$year, n = 1), na.rm = TRUE),
      last = max(.data$year)
    ) |>
    transmute(
      .data$location,
      start = ifelse(
        .data$first == .data$second - 1, .data$first, .data$second
      ),
      end = ifelse(
        .data$last == .data$penultimate + 1, .data$last, .data$penultimate
      )
    ) |>
    left_join(x = observation, by = "location") |>
    mutate(
      count = ifelse(
        is.na(.data$start), NA,
        ifelse(
          .data$start <= .data$year & .data$year <= .data$end, .data$count, NA
        )
      )
    ) |>
    select(-"start", -"end") -> observation
  # select locations with observations from at least 10 different years
  observation |>
    filter(!is.na(.data$count)) |>
    distinct(.data$year, .data$location) |>
    count(.data$location) |>
    filter(.data$n >= 10) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(!is.na(.data$minimum), .data$minimum > 0) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation

  # select locations with presences in at least 5 years
  observation |>
    filter(.data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 5) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(!is.na(.data$minimum), .data$minimum > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation

  # keep only months with at least 5 observations
  observation |>
    filter(!is.na(.data$count), .data$count > 0) |>
    count(.data$month) |>
    filter(.data$n >= 5) -> to_keep
  observation |>
    anti_join(to_keep, by = "month") |>
    filter(!is.na(.data$minimum), .data$minimum > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "month") -> observation

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

  # select locations with observations from at least 10 different years
  observation |>
    filter(!is.na(.data$count)) |>
    distinct(.data$year, .data$location) |>
    count(.data$location) |>
    filter(.data$n >= 10) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(!is.na(.data$minimum), .data$minimum > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation

  # select locations with presences in at least 5 years
  observation |>
    filter(.data$count > 0) |>
    distinct(.data$location, .data$year) |>
    count(.data$location) |>
    filter(.data$n >= 5) -> to_keep
  observation |>
    anti_join(to_keep, by = "location") |>
    filter(!is.na(.data$minimum), .data$minimum > 0) |>
    bind_rows(rare_observation) -> rare_observation
  observation |>
    semi_join(to_keep, by = "location") -> observation

  observation |>
    distinct(.data$location) |>
    nrow() -> n_location
  if (n_location < 6) {
    rare_observation |>
      bind_rows(observation) -> rare_observation
    observation <- observation[0, ]
  }

  return(list(observation = observation, rare_observation = rare_observation))
}
