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
#' dataset, number of presences in the analysis dataset and SHA-1 of the
#' analysis dataset or NULL if not enough data.
#' @importFrom assertthat assert_that has_name noNA is.flag
#' @importFrom dplyr across count inner_join filter mutate select row_number
#' transmute
#' @importFrom git2rdata read_vc recent_commit write_vc
#' @importFrom purrr map map2 map2_dfr map_dfr
#' @importFrom rlang .data
#' @importFrom tidyr nest
#' @export
prepare_analysis_imputation <- function(
  speciesgroupspecies, location, analysis_path, raw_repo, seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
  assert_that(
    inherits(location, "data.frame"), has_name(location, "location"),
    has_name(location, "locationgroup"), has_name(location, "subset_month"),
    has_name(location, "start_year"), has_name(location, "end_year"),
    noNA(location$location), noNA(location$locationgroup),
    noNA(location$subset_month),
    inherits(speciesgroupspecies, "data.frame"),
    has_name(speciesgroupspecies, "speciesgroup"),
    has_name(speciesgroupspecies, "species"), noNA(speciesgroupspecies),
    anyDuplicated(speciesgroupspecies$speciesgroup) == 0,
    is.flag(verbose), noNA(verbose)
  )
  display(verbose, unique(speciesgroupspecies[["speciesgroup"]]))
  stopifnot(
    "Speciesgroups with multiple species not yet handled" =
      nrow(speciesgroupspecies) == 1
  )

  read_vc(file = "metadata", root = raw_repo) |>
    inner_join(speciesgroupspecies, by = c("species_id" = "species")) |>
    inner_join(
      read_vc(file = "species/speciesgroup", root = raw_repo) |>
        select("id", "distribution"),
      by = c("speciesgroup" = "id")
    ) |>
    mutate(
      datasource = as.character(.data$datasource),
      filename = sprintf("observation/%06i", .data$species_id),
      distribution = as.character(.data$distribution)
    ) -> metadata
  assert_that(
    has_name(metadata, "first_imported_year"),
    has_name(metadata, "last_imported_year")
  )

  rawdata <- try(read_vc(metadata$filename, root = raw_repo), silent = TRUE)
  if (inherits(rawdata, "try-error")) {
    return(tibble())
  }
  assert_that(
    has_name(rawdata, "location"), has_name(rawdata, "year"),
    has_name(rawdata, "month"), has_name(rawdata, "id"),
    has_name(rawdata, "complete"), has_name(rawdata, "count"),
    has_name(rawdata, "datafield_id"),
    noNA(select(rawdata, "datafield_id", "location", "year", "month", "id"))
  )
  stopifnot(
"Each combination of location, year and month must have exactly one
observation" = anyDuplicated(rawdata[, c("location", "year", "month")]) == 0
  )

  metadata$analysis_date <- recent_commit(
    metadata$filename, raw_repo, TRUE
  )$when

  rawdata |>
    mutate(
      month = factor(
        .data$month, levels = c(10:12, 1:3),
        labels = c(
          "October", "November", "December", "January", "February", "March"
        )
      )
    ) |>
    complete(
      .data$year, .data$month, .data$location, fill = list(datafield_id = -1)
    ) |>
    inner_join(location, by = "location") |>
    filter(
      !.data$subset_month |
        .data$month %in% c("November", "December", "January", "February"),
      is.na(.data$start_year) | .data$start_year <= .data$year,
      is.na(.data$end_year) | .data$year <= .data$end_year
    ) |>
    transmute(
      observation_id = ifelse(is.na(.data$id), -row_number(), .data$id),
      .data$datafield_id, .data$locationgroup, .data$location, .data$year,
      .data$month, minimum = pmax(0, .data$count),
      count = ifelse(.data$complete, .data$count, NA)
    ) |>
    arrange(.data$location, .data$year, .data$month) -> observation |>
    group_by(.data$locationgroup) |>
    nest() |>
    ungroup() |>
    mutate(
      relevant = map(.data$data, select_relevant_analysis),
      rare_observation = map(.data$relevant, "rare_observation"),
      relevant = map(.data$relevant, "observation"),
      model = map2(
        .data$locationgroup, .data$relevant, prepare_imputation_model,
        metadata = metadata, seed = seed
      ),
      filename = map_chr(
        .data$model, store_model, base = analysis_path, project = "watervogels",
        overwrite = FALSE
      )
    ) -> selected
  map2_dfr(
    selected$rare_observation, selected$locationgroup,
    ~mutate(.x, imputation_locationgroup = .y)
  ) |>
    write_vc(
      file = sprintf("rare/%05i", metadata$species_id), root = raw_repo,
      sorting = c("imputation_locationgroup", "observation_id")
    )

  map_dfr(selected$model, slot, "AnalysisMetadata") |>
    select(
      "result_datasource_id", "scheme_id", impute = "location_group_id",
      "species_group_id", "file_fingerprint", "first_imported_year",
      "last_imported_year", "analysis_date", "formula", "status",
      "location_group_id"
    ) |>
    mutate(filename = selected$filename)
}

#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate
#' @importFrom n2kanalysis n2k_inla
#' @importFrom rlang .data
prepare_imputation_model <- function(location_group, relevant, metadata, seed) {
  if (nrow(relevant) == 0) {
    model <- n2k_inla(
      data = relevant, result_datasource_id = metadata$datasource,
      scheme_id = "watervogels", formula = "count ~ 1",
      location_group_id = as.character(location_group),
      species_group_id = as.character(metadata$speciesgroup),
      model_type = sprintf(
        "inla %s: year * (location + month)", metadata$distribution
      ),
      first_imported_year = metadata$first_imported_year, seed = seed,
      last_imported_year = metadata$last_imported_year,
      family = metadata$distribution, analysis_date = metadata$analysis_date,
      status = "insufficient_data"
    )
    return(model)
  }
  assert_that(
    length(levels(relevant$month)) > 3, diff(range(relevant$year)) > 4,
    length(unique(relevant$location)) >= 6
  )
  relevant |>
    mutate(
      cyear = .data$year - min(.data$year) + 1, cyear2 = .data$cyear,,
      imonth = as.integer(.data$month), location = factor(.data$location),
      ilocation = as.integer(.data$location)
    ) |>
    n2k_inla(
      formula = "count ~ month +
        f(
          cyear, model = \"rw1\", scale.model = TRUE,
          hyper = list(theta = list(prior = \"pc.prec\", param = c(0.1, 0.01)))
        ) +
        f(
          cyear2, model = \"rw1\", scale.model = TRUE, replicate = ilocation,
          hyper = list(theta = list(prior = \"pc.prec\", param = c(0.01, 0.01)))
        ) +
        f(
          imonth, model = \"rw1\", constr = TRUE, replicate = cyear,
          hyper = list(theta = list(prior = \"pc.prec\", param = c(0.1, 0.01)))
        ) +
        f(
          location, model = \"iid\", constr = TRUE,
          hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
        )",
      result_datasource_id = metadata$datasource, scheme_id = "watervogels",
      location_group_id = as.character(location_group),
      species_group_id = as.character(metadata$speciesgroup),
      model_type = sprintf(
        "inla %s: year * (location + month)", metadata$distribution
      ),
      first_imported_year = metadata$first_imported_year, seed = seed,
      last_imported_year = metadata$last_imported_year,
      family = metadata$distribution, analysis_date = metadata$analysis_date,
      imputation_size = 100, minimum = "minimum"
    )
}
