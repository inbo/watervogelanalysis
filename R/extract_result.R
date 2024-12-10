#' Extract results from a model
#' @param x The model to extract results from
#' @param ... Additional arguments passed to the extraction method
#' @export
#' @importFrom assertthat assert_that
extract_results <- function(x, ...) {
  UseMethod("extract_results", x)
}

#' @export
extract_results.default <- function(x, ...) {
  stop("No extraction method for class ", class(x))
}

#' @export
#' @importFrom assertthat assert_that is.flag is.string noNA
#' @importFrom dplyr anti_join distinct mutate select
#' @importFrom git2rdata is_git2rdata verify_vc write_vc
#' @importFrom n2kanalysis order_manifest read_manifest read_model
#' @importFrom purrr map walk
#' @importFrom rlang .data
#' @importFrom tidyr unnest
extract_results.character <- function(
  x, base, project = "wateranalysis", raw_data, root, random = FALSE,
  verbose = TRUE, ...
) {
  assert_that(is.string(x), noNA(x), is.flag(random), noNA(random))
  verify_vc(
    "species/species", root = raw_data,
    variables = c("euring", "scientific", "nl", "fr")
  ) |>
    select("euring", "scientific", "nl", "fr") |>
    mutate(gbif = map(.data$scientific, get_gbif)) |>
    unnest("gbif") |>
    mutate(
      vernacular = ifelse(
        .data$language == "nld" & !is.na(.data$nl), .data$nl,
        ifelse(
          .data$language == "fra" & !is.na(.data$fr), .data$fr, .data$vernacular
        )
      )
    ) -> species
  species |>
    distinct(.data$euring, .data$scientific, gbif = .data$key) |>
    write_vc(
      file.path("data", "species"), root = root, sorting = "euring",
      optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "species"), root = root, name = "species",
    title = "List of species",
    field_description = c(
      euring = "The European bird ringing code.",
      scientific = "The scientific name of the species.",
      gbif = "GBIF identifier."
    )
  )
  species |>
    select("euring", "language", "vernacular") |>
    write_vc(
      file.path("data", "vernacular"), root = root,
      sorting = c("euring", "language"), optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "vernacular"), root = root, name = "vernacular",
    title = "Vernacular species names",
    field_description = c(
      euring = "The European bird ringing code.",
      language = "Identifier of the language.",
      vernacular = "The vernacular name of the species."
    )
  )

  verify_vc(
    "location/locationgroup", root = raw_data,
    variables = c("external_code", "description")
  ) |>
    select("external_code", "description") |>
    write_vc(
      file.path("data", "locationgroup"), root = root,
      sorting = "external_code", optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "locationgroup"), root = root,
    name = "locationgroup",
    title = "List of locationgroups",
    field_description = c(
      external_code = "The identifier of the location group.",
      description = "Full name of the location group"
    )
  )
  read_manifest(base = base, project = project, hash = x) |>
    order_manifest() -> manifest
  if (is_git2rdata("data/analysis", root = root)) {
    file.path("data", "analysis") |>
      verify_vc(root = root, variables = "analysis") -> done
    manifest <- manifest[!manifest %in% done$analysis]
    rm(done)
  }
  if (random) {
    manifest <- sample(manifest)
  }
  start_time <- Sys.time()
  for (i in seq_along(manifest)) {
    display(
      verbose = verbose,
      message = sprintf(
        "Processing %i from %i (%.2f%%) %s ETA %s %s", i, length(manifest),
        100 * (i - 1) / length(manifest),
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        format(
          start_time + (Sys.time() - start_time) * length(manifest)  / (i - 1),
          "%d %H:%M"
        ),
        manifest[i]
      )
    )
    model <- try(read_model(manifest[i], base = base, project = project))
    if (inherits(model, "try-error")) {
      next
    }
    extract_results(model, root = root)
    rm(model)
    gc(verbose = FALSE)
  }
  file.path("data", "relation") |>
    update_metadata(
      root = root, name = "relation", title = "Relation between analyses",
      description = "List the analyses and their parent analyses.",
      field_description = c(
        analysis = "The unique identifier of the analysis.",
        parent = "The parent analysis."
      )
    )
  file.path("data", "analysis") |>
    update_metadata(
      root = root, name = "analysis", title = "Analysis metadata",
      description = "List the analyses and their parent analyses.",
      field_description = c(
        species = "The species group identifier.",
        locationgroup = "The location group identifier.",
        model_type = "The type of the model.",
        analysis = "The unique identifier of the analysis.",
        fingerprint = "Status fingerprint of the analysis.",
        status = "The status of the analysis."
      )
    )
  return(invisible(NULL))
}

#' @export
#' @importFrom dplyr filter mutate select transmute
#' @importFrom git2rdata write_vc
extract_results.n2kModelImputed <- function(x, root, ...) {
  if (x@AnalysisMetadata$status != "converged") {
    return(invisible(NULL))
  }
  x@Results |>
    mutate(analysis = get_file_fingerprint(x)) -> result
  if (nrow(result) == 0) {
    x@AnalysisRelation |>
      select("analysis", parent = "parent_analysis") |>
      write_vc(
        file.path("data", "relation"), root = root, optimize = FALSE,
        append = TRUE, sorting = c("analysis", "parent")
      )
    x@AnalysisMetadata |>
      select(
        species = "species_group_id", locationgroup = "location_group_id",
        "model_type", analysis = "file_fingerprint",
        fingerprint = "status_fingerprint", "status"
      ) |>
      write_vc(
        file.path("data", "analysis"), root = root, optimize = FALSE,
        append = TRUE, sorting = "analysis"
      )
    return(invisible(NULL))
  }
  file.path(
    "data", tolower(x@AnalysisMetadata$location_group_id),
    x@AnalysisMetadata$species_group_id
  ) -> filename
  result |>
    filter(grepl("total:", .data$Parameter)) |>
    transmute(
      .data$analysis,
      winter = gsub("total: ", "", .data$Parameter) |>
        as.integer(),
      estimate = .data$Estimate, se = .data$SE, lcl = .data$LCL, ucl = .data$UCL
    ) |>
    write_vc(
      file.path(filename, "total"), root = root, digits = 4,
      sorting = c("analysis", "winter"), optimize = FALSE, append = TRUE
    )
  update_metadata(
    file = file.path(filename, "total"), root = root, name = "total",
    title = "Total results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.", "Links to the analysis table."
      ),
      winter = "The winter season. Refers to the year of January 1st.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  result |>
    filter(grepl("trend_", .data$Parameter)) |>
    transmute(
      .data$analysis,
      centre_winter = gsub("trend_(.*)_.*", "\\1", .data$Parameter) |>
        as.numeric(),
      duration = gsub("trend_.*_(.*)", "\\1", .data$Parameter) |>
        as.integer(),
      estimate = .data$Estimate, se = .data$SE, lcl = .data$LCL, ucl = .data$UCL
    ) |>
    write_vc(
      file.path(filename, "trend"), root = root, optimize = FALSE,
      sorting = c("analysis", "duration", "centre_winter"), append = TRUE,
      digits = c(centre_winter = 5, estimate = 4, se = 4, lcl = 4, ucl = 4)
    )
  update_metadata(
    file = file.path(filename, "trend"), root = root, name = "trend",
    title = "Trend results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.", "Links to the analysis table."
      ),
      centre_winter = "The centre of the winter season.",
      duration = "The duration of the trend.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  result |>
    filter(grepl("average_", .data$Parameter)) |>
    transmute(
      .data$analysis,
      centre_winter = gsub("average_(.*)_.*", "\\1", .data$Parameter) |>
        as.numeric(),
      duration = gsub("average_.*_(.*)", "\\1", .data$Parameter) |>
        as.integer(),
      estimate = .data$Estimate, se = .data$SE, lcl = .data$LCL, ucl = .data$UCL
    ) |>
    write_vc(
      file.path(filename, "average"), root = root, optimize = FALSE,
      sorting = c("analysis", "duration", "centre_winter"), append = TRUE,
      digits = c(centre_winter = 5, estimate = 4, se = 4, lcl = 4, ucl = 4)
    )
  update_metadata(
    file.path(filename, "average"), root = root, name = "average",
    title = "Average results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.", "Links to the analysis table."
      ),
      centre_winter = "The centre of the winter of the period.",
      duration = "The duration of period in years.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  result |>
    filter(grepl("difference_", .data$Parameter)) |>
    transmute(
      .data$analysis,
      centre_start = gsub("difference_(.*)_(.*)_.*", "\\1", .data$Parameter) |>
        as.numeric(),
      centre_end = gsub("difference_(.*)_(.*)_(.*)", "\\2", .data$Parameter) |>
        as.numeric(),
      duration = gsub("difference_(.*)_(.*)_(.*)", "\\3", .data$Parameter) |>
        as.integer(),
      estimate = .data$Estimate, se = .data$SE, lcl = .data$LCL, ucl = .data$UCL
    ) |>
    write_vc(
      file.path(filename, "difference"), root = root, optimize = FALSE,
      append = TRUE,
      sorting = c("analysis", "duration", "centre_start", "centre_end"),
      digits = c(
        centre_start = 5, centre_end = 5, estimate = 4, se = 4, lcl = 4, ucl = 4
      )
    )
  update_metadata(
    file = file.path(filename, "difference"), root = root, name = "difference",
    title = "Difference results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.", "Links to the analysis table."
      ),
      centre_start = "The centre of the winter of the start period.",
      centre_end = "The centre of the winter of the end period.",
      duration = "The duration of the period in years.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  result |>
    filter(grepl("^month", .data$Parameter)) |>
    transmute(
      .data$analysis,
      month = gsub("month", "", .data$Parameter) |>
        factor(
          levels = c(
            "October", "November", "December", "January", "February", "March"
          )
        ),
      estimate = .data$Estimate, se = .data$SE, lcl = .data$LCL, ucl = .data$UCL
    ) |>
    write_vc(
      file.path(filename, "month"), root = root, digits = 4,
      sorting = c("analysis", "month"), optimize = FALSE, append = TRUE
    )
  update_metadata(
    file = file.path(filename, "month"), root = root, name = "month",
    title = "Average seasonal pattern",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.", "Links to the analysis table."
      ),
      month = "The month of the winter season.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  x@AnalysisRelation |>
    select("analysis", parent = "parent_analysis") |>
    write_vc(
      file.path("data", "relation"), root = root, optimize = FALSE,
      append = TRUE, sorting = c("analysis", "parent")
    )
  x@AnalysisMetadata |>
    select(
      species = "species_group_id", locationgroup = "location_group_id",
      "model_type", analysis = "file_fingerprint",
      fingerprint = "status_fingerprint", "status"
    ) |>
    write_vc(
      file.path("data", "analysis"), root = root, optimize = FALSE,
      append = TRUE, sorting = "analysis"
    )
  return(invisible(NULL))
}

#' @export
#' @importFrom dplyr bind_cols group_by mutate select summarise transmute
#' @importFrom git2rdata write_vc
#' @importFrom n2kanalysis get_file_fingerprint
#' @importFrom tidyr pivot_longer
extract_results.n2kAggregate <- function(x, root, ...) {
  if (x@AnalysisMetadata$status != "converged") {
    return(invisible(NULL))
  }
  if (nrow(x@AggregatedImputed@Covariate) == 0) {
    x@AnalysisMetadata |>
      select(
        species = "species_group_id", locationgroup = "location_group_id",
        "model_type", analysis = "file_fingerprint",
        fingerprint = "status_fingerprint", "status"
      ) |>
      write_vc(
        file.path("data", "analysis"), root = root, optimize = FALSE,
        append = TRUE, sorting = "analysis"
      )
    return(invisible(NULL))
  }
  if ("month" %in% colnames(x@AggregatedImputed@Covariate)) {
    file.path(
      "data", tolower(x@AnalysisMetadata$location_group_id),
      x@AnalysisMetadata$species_group_id, "imputed_total_month"
    ) -> filename
    x@AggregatedImputed@Imputation |>
      bind_cols(x@AggregatedImputed@Covariate) |>
      pivot_longer(
        cols = -c("year", "month"), names_to = "sim", values_to = "count"
      ) |>
      group_by(winter = .data$year, .data$month) |>
      summarise(
        median = median(.data$count), min = min(.data$count),
        q05 = quantile(.data$count, prob = 0.05),
        q20 = quantile(.data$count, prob = 0.2),
        q35 = quantile(.data$count, prob = 0.35),
        q65 = quantile(.data$count, prob = 0.65),
        q80 = quantile(.data$count, prob = 0.8),
        q95 = quantile(.data$count, prob = 0.95),
        max = max(.data$count), .groups = "drop"
      ) |>
      mutate(
        analysis = get_file_fingerprint(x),
        month = factor(
          .data$month,
          levels = c(
            "January", "February", "March", "October", "November", "December"
          )
        )
      ) |>
      write_vc(
        filename, root = root, optimize = FALSE, append = TRUE, digits = 4,
        sorting = c("analysis", "winter", "month"), strict = FALSE
      )
    update_metadata(
      filename, root = root, name = "imputed_total_month",
      title = "Imputed total results per month",
      field_description = c(
        analysis = paste(
          "The unique identifier of the analysis.",
          "Links to the analysis table."
        ),
        winter = "The winter season. Refers to the year of Januari 1st.",
        month = "The month of the winter season.",
        median = "The median of the imputed total.",
        min = "The minimum of the imputed total.",
        q05 = "The 5th percentile of the imputed total.",
        q20 = "The 20th percentile of the imputed total.",
        q35 = "The 35th percentile of the imputed total.",
        q65 = "The 65th percentile of the imputed total.",
        q80 = "The 80th percentile of the imputed total.",
        q95 = "The 95th percentile of the imputed total.",
        max = "The maximum of the imputed total."
      )
    )
  } else {
    file.path(
      "data", tolower(x@AnalysisMetadata$location_group_id),
      x@AnalysisMetadata$species_group_id, "imputed_total"
    ) -> filename
    x@AggregatedImputed@Imputation |>
      bind_cols(x@AggregatedImputed@Covariate) |>
      pivot_longer(cols = -"year", names_to = "sim", values_to = "count") |>
      group_by(winter = .data$year) |>
      summarise(
        median = median(.data$count), min = min(.data$count),
        q05 = quantile(.data$count, prob = 0.05),
        q20 = quantile(.data$count, prob = 0.2),
        q35 = quantile(.data$count, prob = 0.35),
        q65 = quantile(.data$count, prob = 0.65),
        q80 = quantile(.data$count, prob = 0.8),
        q95 = quantile(.data$count, prob = 0.95),
        max = max(.data$count)
      ) |>
      mutate(analysis = get_file_fingerprint(x)) |>
      write_vc(
        filename, root = root, optimize = FALSE, append = TRUE, digits = 4,
        sorting = c("analysis", "winter"), strict = FALSE
      )
    update_metadata(
      filename, root = root, name = "imputed_total",
      title = "Imputed total results",
      field_description = c(
        analysis = paste(
          "The unique identifier of the analysis.",
          "Links to the analysis table."
        ),
        winter = "The winter season. Refers to the year of Januari 1st.",
        median = "The median of the imputed total.",
        min = "The minimum of the imputed total.",
        q05 = "The 5th percentile of the imputed total.",
        q20 = "The 20th percentile of the imputed total.",
        q35 = "The 35th percentile of the imputed total.",
        q65 = "The 65th percentile of the imputed total.",
        q80 = "The 80th percentile of the imputed total.",
        q95 = "The 95th percentile of the imputed total.",
        max = "The maximum of the imputed total."
      )
    )
  }
  x@AnalysisRelation |>
    select("analysis", parent = "parent_analysis") |>
    write_vc(
      file.path("data", "relation"), root = root, optimize = FALSE,
      append = TRUE, sorting = c("analysis", "parent")
    )
  x@AnalysisMetadata |>
    select(
      species = "species_group_id", locationgroup = "location_group_id",
      "model_type", analysis = "file_fingerprint",
      fingerprint = "status_fingerprint", "status"
    ) |>
    write_vc(
      file.path("data", "analysis"), root = root, optimize = FALSE,
      append = TRUE, sorting = "analysis"
    )
  return(invisible(NULL))
}

#' @export
#' @importFrom dplyr bind_cols group_by mutate select summarise transmute
#' @importFrom git2rdata write_vc
#' @importFrom n2kanalysis get_file_fingerprint
#' @importFrom tidyr pivot_longer
extract_results.n2kHurdleImputed <- function(x, root, ...) {
  if (x@AnalysisMetadata$status != "converged") {
    return(invisible(NULL))
  }
  file.path(
    "data", "model_check", tolower(x@AnalysisMetadata$location_group_id),
    x@AnalysisMetadata$species_group_id, "hurdle"
  ) -> filename
  x@Hurdle@Covariate |>
    select("year", "month", "location") |>
    bind_cols(x@Hurdle@Imputation) |>
    pivot_longer(
      cols = -c("year", "month", "location"), names_to = "sim",
      values_to = "count"
    ) |>
    group_by(winter = .data$year, .data$month, .data$location) |>
    summarise(
      median = median(.data$count), min = min(.data$count),
      q05 = quantile(.data$count, prob = 0.05),
      q20 = quantile(.data$count, prob = 0.2),
      q35 = quantile(.data$count, prob = 0.35),
      q65 = quantile(.data$count, prob = 0.65),
      q80 = quantile(.data$count, prob = 0.8),
      q95 = quantile(.data$count, prob = 0.95),
      max = max(.data$count), .groups = "drop"
    ) |>
    mutate(
      analysis = get_file_fingerprint(x),
      month = factor(
        .data$month,
        levels = c(
          "January", "February", "March", "October", "November", "December"
        )
      )
    ) |>
    write_vc(
      filename, root = root, optimize = FALSE, append = TRUE, strict = FALSE,
      sorting = c("analysis", "winter", "month", "location"),  digits = 4
    )
  update_metadata(
    filename, root = root, name = "hurdle",
    title = "Hurdle model check results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.",
        "Links to the analysis table."
      ),
      winter = "The winter season. Refers to the year of Januari 1st.",
      month = "The month of the winter season.",
      location = "Identifier of the location.",
      median = "The median of the imputed total.",
      min = "The minimum of the imputed total.",
      q05 = "The 5th percentile of the imputed total.",
      q20 = "The 20th percentile of the imputed total.",
      q35 = "The 35th percentile of the imputed total.",
      q65 = "The 65th percentile of the imputed total.",
      q80 = "The 80th percentile of the imputed total.",
      q95 = "The 95th percentile of the imputed total.",
      max = "The maximum of the imputed total."
    )
  )
  x@AnalysisRelation |>
    select("analysis", parent = "parent_analysis") |>
    write_vc(
      file.path("data", "relation"), root = root, optimize = FALSE,
      append = TRUE, sorting = c("analysis", "parent")
    )
  x@AnalysisMetadata |>
    select(
      species = "species_group_id", locationgroup = "location_group_id",
      "model_type", analysis = "file_fingerprint",
      fingerprint = "status_fingerprint", "status"
    ) |>
    write_vc(
      file.path("data", "analysis"), root = root, optimize = FALSE,
      append = TRUE, sorting = "analysis"
    )
  return(invisible(NULL))
}

#' @export
#' @importFrom dplyr bind_cols group_by mutate select summarise transmute
#' @importFrom git2rdata write_vc
#' @importFrom n2kanalysis get_file_fingerprint
#' @importFrom tidyr pivot_longer
extract_results.n2kInla <- function(x, root, ...) {
  if (x@AnalysisMetadata$status != "converged") {
    return(invisible(NULL))
  }
  file.path(
    "data", "model_check", tolower(x@AnalysisMetadata$location_group_id),
    x@AnalysisMetadata$species_group_id, "modelfit"
  ) -> filename
  x@Data |>
    select(winter = "year", "month", "location") |>
    bind_cols(x@Model$summary.linear.predictor[, c("mean", "sd")]) |>
    mutate(
      analysis = get_file_fingerprint(x),
      month = factor(
        .data$month,
        levels = c(
          "January", "February", "March", "October", "November", "December"
        )
      )
    ) |>
    write_vc(
      filename, root = root, optimize = FALSE, append = TRUE,
      sorting = c("analysis", "winter", "month", "location")
    )
  update_metadata(
    filename, root = root, name = "modelfit",
    title = "Model fit results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.",
        "Links to the analysis table."
      ),
      winter = "The winter season. Refers to the year of Januari 1st.",
      month = "The month of the winter season.",
      location = "Identifier of the location.",
      mean = "The mean of the linear predictor at the link scale.",
      sd = "The standard deviation of the linear predictor at the link scale."
    )
  )
  x@AnalysisMetadata |>
    select(
      species = "species_group_id", locationgroup = "location_group_id",
      "model_type", analysis = "file_fingerprint",
      fingerprint = "status_fingerprint", "status"
    ) |>
    write_vc(
      file.path("data", "analysis"), root = root, optimize = FALSE,
      append = TRUE, sorting = "analysis"
    )
  return(invisible(NULL))
}

#' @export
#' @importFrom dplyr filter mutate select transmute
#' @importFrom git2rdata write_vc
extract_results.n2kComposite <- function(x, root, ...) {
  if (x@AnalysisMetadata$status != "converged") {
    return(invisible(NULL))
  }
  file.path(
    "data", tolower(x@AnalysisMetadata$location_group_id),
    tolower(x@AnalysisMetadata$species_group_id), "difference"
  ) -> filename
  x@Index |>
    transmute(
      analysis = get_file_fingerprint(x), .data$value, .data$estimate,
      se = (.data$upper_confidence_limit - .data$estimate) / qnorm(0.975),
      lcl = .data$lower_confidence_limit, ucl = .data$upper_confidence_limit
    ) -> result
  result |>
    filter(grepl("difference_", .data$value)) |>
    transmute(
      .data$analysis,
      centre_start = gsub("difference_(.*)_(.*)_.*", "\\1", .data$value) |>
        as.numeric(),
      centre_end = gsub("difference_(.*)_(.*)_(.*)", "\\2", .data$value) |>
        as.numeric(),
      duration = gsub("difference_(.*)_(.*)_(.*)", "\\3", .data$value) |>
        as.integer(),
      .data$estimate, .data$se, .data$lcl, .data$ucl
    ) |>
    write_vc(
      filename, root = root, optimize = FALSE, append = TRUE,
      sorting = c("analysis", "duration", "centre_start", "centre_end"),
      digits = c(
        centre_start = 5, centre_end = 5, estimate = 4, se = 4, lcl = 4, ucl = 4
      )
    )
  update_metadata(
    filename, root = root, name = "difference",
    title = "Difference results",
    field_description = c(
      analysis = paste(
        "The unique identifier of the analysis.",
        "Links to the analysis table."
      ),
      centre_start = "The centre of the winter of the start period.",
      centre_end = "The centre of the winter of the end period.",
      duration = "The duration of the period in years.",
      estimate = "The estimate of the parameter in the log-scale.",
      se = "The standard error of the estimate in the log-scale.",
      lcl = "The lower confidence limit in the log-scale.",
      ucl = "The upper confidence limit in the log-scale."
    )
  )
  x@AnalysisRelation |>
    select("analysis", parent = "parent_analysis") |>
    write_vc(
      file.path("data", "relation"), root = root, optimize = FALSE,
      append = TRUE, sorting = c("analysis", "parent")
    )
  x@AnalysisMetadata |>
    select(
      species = "species_group_id", locationgroup = "location_group_id",
      "model_type", analysis = "file_fingerprint",
      fingerprint = "status_fingerprint", "status"
    ) |>
    write_vc(
      file.path("data", "analysis"), root = root, optimize = FALSE,
      append = TRUE, sorting = "analysis"
    )
  return(invisible(NULL))
}
