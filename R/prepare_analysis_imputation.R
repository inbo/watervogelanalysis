#' Create the analysis dataset based on the available raw data
#'
#' The analysis dataset is saved to a `.rda` file with the SHA-1 as name.
#' @param speciesgroupspecies a data.frame with the current species group and
#' the associates species.
#' @param location a data.frame with the locations and location groups
#' @param analysis_path Path to store the analysis files.
#' Must be either an existing local file path or an object of the `s3_bucket`
#' class.
#' @param seed the seed for the random number generator.
#' Defaults to 19790402 which refers to "Council Directive 79/409/EEC of 2 April
#' 1979 on the conservation of wild birds".
#' @inheritParams prepare_dataset
#' @return A data.frame with the species id number of rows in the analysis
#' analysis dataset or NULL if not enough data.
#' @importFrom assertthat assert_that has_name noNA is.flag is.count
#' @importFrom dplyr count inner_join filter mutate select row_number transmute
#' @importFrom git2rdata read_vc recent_commit write_vc
#' @importFrom methods slot
#' @importFrom purrr map map2 map2_dfr map_dfr map_lgl
#' @importFrom rlang .data
#' @importFrom tidyr complete nest pivot_wider
#' @export
prepare_analysis_imputation <- function(
  speciesgroupspecies, location, analysis_path, raw_repo, seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
  assert_that(
    inherits(location, "data.frame"), has_name(location, "location"),
    has_name(location, "locationgroup"), has_name(location, "subset_months"),
    has_name(location, "start_year"), has_name(location, "end_year"),
    noNA(location$location), noNA(location$locationgroup),
    noNA(location$subset_month), inherits(speciesgroupspecies, "data.frame"),
    has_name(speciesgroupspecies, "speciesgroup"),
    has_name(speciesgroupspecies, "species"), noNA(speciesgroupspecies),
    anyDuplicated(speciesgroupspecies$speciesgroup) == 0,
    is.flag(verbose), noNA(verbose)
  )
  sprintf("%s ", unique(speciesgroupspecies[["speciesgroup"]])) |>
    display(verbose = verbose, linefeed = FALSE)

  stopifnot(
    "Speciesgroups with multiple species not yet handled" =
      nrow(speciesgroupspecies) == 1
  )

  sprintf("observation/%06i", speciesgroupspecies$species) |>
    verify_vc(
      root = raw_repo,
      variables = c(
        "year", "month", "location", "count", "complete", "observation_id",
        "datafield"
      )
    ) -> rawdata
  assert_that(
    rawdata |>
      filter(!is.na(.data$count)) |>
      noNA()
  )
  stopifnot(
"Each combination of location, year and month must have exactly one
observation" = anyDuplicated(rawdata[, c("location", "year", "month")]) == 0
  )
  sprintf("observation/%06i", speciesgroupspecies$species) |>
    recent_commit(root = raw_repo, data = TRUE) |>
    select(analysis_date = "when") |>
    bind_cols(
      rawdata |>
        summarise(
          first_imported_year = min(.data$year),
          last_imported_year = max(.data$year)
        ),
      speciesgroupspecies
    ) -> metadata
  rawdata |>
    complete(.data$year, .data$month, .data$location) |>
    inner_join(location, by = "location", relationship = "many-to-many") |>
    filter(
      !.data$subset_months |
        .data$month %in% c("November", "December", "January", "February"),
      is.na(.data$start_year) | .data$start_year <= .data$year,
      is.na(.data$end_year) | .data$year <= .data$end_year
    ) |>
    transmute(
      observation_id = ifelse(
        is.na(.data$observation_id), -row_number(), .data$observation_id
      ),
      datafield_id = replace_na(.data$datafield, -1), .data$locationgroup,
      .data$location, .data$year, .data$month, minimum = pmax(0, .data$count),
      count = ifelse(.data$complete, .data$count, NA)
    ) |>
    arrange(.data$location, .data$year, .data$month) |>
    nest(.by = "locationgroup") |>
    mutate(
      relevant = map(.data$data, select_relevant_analysis),
      rare_observation = map(.data$relevant, "rare_observation"),
      relevant = map(.data$relevant, "observation"),
    ) |>
    transmute(
      model = pmap(
        list(
          location_group = .data$locationgroup, relevant = .data$relevant,
          extra = .data$rare_observation
        ),
        prepare_imputation_model, seed = seed, metadata = metadata
      )
    ) |>
    unnest("model") |>
    mutate(
      filename = map_chr(
        .data$model, store_model, base = analysis_path, project = "watervogels",
        overwrite = FALSE
      )
    ) -> selected
  names(selected$model) <- selected$filename

  map_dfr(selected$model, slot, "AnalysisMetadata") |>
    filter(.data$status != "insufficient_data") -> relevant
  if (nrow(relevant) == 0) {
    return(data.frame())
  }

  relevant |>
    transmute(
      impute = .data$location_group_id, .data$species_group_id,
      type = ifelse(grepl(" binomial:", .data$model_type), "presence", "count"),
      model = selected$model[relevant$file_fingerprint]
    ) |>
    pivot_wider(names_from = "type", values_from = "model")
}

#' @importFrom assertthat assert_that
#' @importFrom dplyr bind_cols group_by filter inner_join mutate select
#' summarise transmute
#' @importFrom n2kanalysis n2k_inla
#' @importFrom rlang .data
prepare_imputation_model <- function(
  location_group, relevant, extra, metadata, seed
) {
  if (nrow(relevant) == 0) {
    model <- n2k_inla(
      data = relevant, result_datasource_id = "watervogels",
      scheme_id = "watervogels", formula = "count ~ 1",
      location_group_id = as.character(location_group),
      species_group_id = metadata$speciesgroup,
      model_type = "inla zeroinflatednbinomial0: year * (location + month)",
      first_imported_year = metadata$first_imported_year, seed = seed,
      last_imported_year = metadata$last_imported_year,
      family = "zeroinflatednbinomial0", analysis_date = metadata$analysis_date,
      status = "insufficient_data"
    )
    return(list(count = model))
  }
  assert_that(
    diff(range(relevant$year)) > 4, length(unique(relevant$location)) >= 6
  )
  relevant |>
    mutate(
      cyear = as.integer(.data$year - min(.data$year) + 1),
      imonth = as.integer(.data$month), .data$location, cyear2 = .data$cyear
    ) -> observations
  extra |>
    complete(
      .data$location, year = observations$year, month = observations$month,
      fill = list(minimum = 0, count = 0)
    ) |>
    mutate(
      cyear = as.integer(.data$year - min(relevant$year) + 1),
      imonth = as.integer(.data$month), .data$location, cyear2 = .data$cyear
    ) |>
    filter(
      min(observations$cyear) <= .data$cyear,
      .data$cyear <= max(observations$cyear)
    ) |>
    transmute(
      .data$count, .data$cyear, .data$imonth, .data$location, .data$minimum,
      .data$observation_id, .data$datafield_id, .data$year, .data$month,
      .data$cyear2
    ) -> extra_count
  form <- c(
    "1",
    "month"[length(unique(relevant$month)) > 1],
    "f(
  cyear, model = \"rw1\", constr = TRUE, scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(2, 0.01)))
)",
    "f(
  location, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)"
  )
  observations |>
    transmute(
      count = ifelse(.data$count > 0, .data$count, NA), .data$cyear,
      .data$imonth, .data$location, .data$minimum, .data$observation_id,
      .data$datafield_id, .data$year, .data$month, .data$cyear2
    ) -> truncated_zero
  observations |>
    transmute(
      present = ifelse(.data$minimum > 0, 1, 0), .data$cyear,
      .data$imonth, .data$location, .data$observation_id,
      .data$datafield_id, .data$year, .data$month
    ) -> present
  truncated_zero |>
    filter(.data$count > 0) |>
    count(.data$year, .data$month) |>
    complete(.data$year, .data$month, fill = list(n = 0)) |>
    group_by(.data$month) |>
    summarise(median = median(.data$n)) |>
    pull(.data$median) -> medians
  form |>
    c("f(
  cyear2, model = \"rw1\", constr = TRUE, replicate = imonth,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)"[all(medians >= 5)]) |>
    paste(collapse = " +\n") |>
    sprintf(fmt = "count ~ %s") |>
    n2k_inla(
      data = truncated_zero, status = "new", family = "zeroinflatednbinomial0",
      result_datasource_id = "watervogels", scheme_id = "watervogels",
      location_group_id = as.character(location_group), extra = extra_count,
      species_group_id = metadata$speciesgroup,
      model_type = "inla zeroinflatednbinomial0: year * (location + month)",
      first_imported_year = metadata$first_imported_year, seed = seed,
      last_imported_year = metadata$last_imported_year, minimum = "minimum",
      analysis_date = metadata$analysis_date, imputation_size = 100,
      control = list(
        control.family = list(
          list(hyper = list(theta2 = list(initial = -11, fixed = TRUE)))
        )
      )
    ) -> counts
  paste(form, collapse = " +\n") |>
    sprintf(fmt = "present ~ %s") |>
    n2k_inla(
      data = present, status = "new", family = "binomial",
      result_datasource_id = "watervogels", scheme_id = "watervogels",
      location_group_id = as.character(location_group),
      species_group_id = metadata$speciesgroup,
      model_type = "inla binomial: year * (location + month)",
      first_imported_year = metadata$first_imported_year, seed = seed,
      last_imported_year = metadata$last_imported_year,
      analysis_date = metadata$analysis_date, imputation_size = 100
    ) -> present

  list(present = present, count = counts)
}
