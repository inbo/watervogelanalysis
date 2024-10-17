#' Read the observations for the raw data source, save them to the git
#' repository
#' @param this_species the species in this species group
#' @param location a data frame with the full list of locations
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr bind_rows filter inner_join select transmute
#' @importFrom git2rdata update_metadata write_vc
#' @importFrom n2kanalysis get_datafield_id
#' @importFrom tidyr complete
#' @importFrom rlang .data
prepare_dataset_observation <- function(
  this_species, location, flemish_channel, walloon_repo, raw_repo, latest_year
) {
  assert_that(
    inherits(this_species, "data.frame"),
    has_name(this_species, "external_code_fl"), has_name(this_species, "first"),
    has_name(this_species, "external_code_wal"),
    inherits(location, "data.frame"), has_name(location, "id"),
    has_name(location, "external_code"), has_name(location, "start_date"),
    has_name(location, "end_date"), has_name(location, "region"),
  )

  flanders_id <- get_datafield_id(
    table = "FactAnalyseSetOccurrence", field = "OccurrenceKey",
    datasource = "W0004_00_Waterbirds database", root = raw_repo, stage = TRUE
  )
  read_observation(
    species_id = this_species$external_code_fl, first_year = this_species$first,
    latest_year = latest_year, flemish_channel = flemish_channel
  ) |>
    mutate(
      datafield = flanders_id,
      observation_id = as.character(.data$observation_id)
    ) |>
    inner_join(
      location |>
        filter(.data$region == "Flanders") |>
        select("location" = "id", "external_code"),
      by = "external_code"
    ) |>
    select(-"external_code") -> observation_flemish

  if (is.na(this_species$external_code_wal)) {
    observation_walloon <- data.frame()
  } else {
    wallonia_id <- get_datafield_id(
      table = "visit", field = "hash", datasource = "Wallonia waterbirds repo",
      root = raw_repo, stage = TRUE
    )
    read_observation_wallonia(
      species_id = this_species$external_code_wal, latest_year = latest_year,
      first_year = this_species$first, walloon_repo = walloon_repo
    ) |>
      mutate(datafield = wallonia_id) |>
      inner_join(
        location |>
          filter(.data$region == "Wallonia") |>
          select("location" = "id", "external_code"),
        by = "external_code"
      ) |>
      select(-"external_code") -> observation_walloon
  }
  bind_rows(observation_flemish, observation_walloon) |>
    select_relevant_import() |>
    mutate(
      month = factor(
        .data$month, levels = c(1:3, 10:12),
        labels = c(
          "January", "February", "March", "October", "November", "December"
        )
      )
    ) |>
    complete(
      .data$year, .data$month, .data$location,
      fill = list(count = NA_integer_, complete = NA_integer_)
    ) |>
    inner_join(
      location |>
        transmute(
          location = .data$id,
          start = round_date(.data$start_date, unit = "year") |>
            year(),
          end = round_date(.data$end_date, unit = "year") |>
            year()
        ),
      by = "location"
    ) |>
    filter(
      is.na(.data$start) | .data$start <= .data$year,
      is.na(.data$end) | .data$year <= .data$end
    ) |>
    select(-"start", -"end") |>
    mutate(location = factor(.data$location)) -> result
  if (nrow(result) == 0) {
    return(invisible(NULL))
  }
  filename <- sprintf("observation/%06i", this_species$euring)
  write_vc(
    x = result, file = filename, root = raw_repo, stage = TRUE,
    sorting = c("location", "year", "month"), strict = FALSE
  )
  update_metadata(
    file = filename, root = raw_repo, stage = TRUE,
    name = sprintf("observation_%06i", this_species$euring),
    title = paste("Relevant observations of", this_species$scientific),
    field_description = c(
      location = "Internal identifier of the location.",
      year = "Winter of the observation.",
      month = "Month of the observation.",
      count = "Number of observations.",
      complete = "Number of complete observations.",
      observation_id =
        "Identifier of the observation in the original data source.",
      datafield = "The matching id in the datafield table."
    )
  )
  return(invisible(NULL))
}
