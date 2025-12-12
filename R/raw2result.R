#' Transform raw species and locationgroup data to result data
#' @param raw_data Character string with path to the raw data directory.
#' @param root Character string with path to the results directory.
#' @export
#' @importFrom dplyr distinct mutate select
#' @importFrom git2rdata update_metadata verify_vc write_vc
#' @importFrom tidyr unnest
raw2result <- function(raw_data, root) {
  verify_vc(
    "species/species",
    root = raw_data,
    variables = c("euring", "scientific", "nl", "fr")
  ) |>
    select("euring", "scientific", "nl", "fr") |>
    mutate(gbif = map(.data$scientific, get_gbif)) |>
    unnest("gbif") |>
    mutate(
      vernacular = ifelse(
        .data$language == "nld" & !is.na(.data$nl),
        .data$nl,
        ifelse(
          .data$language == "fra" & !is.na(.data$fr),
          .data$fr,
          .data$vernacular
        )
      )
    ) -> species
  species |>
    distinct(.data$euring, .data$scientific, gbif = .data$key) |>
    write_vc(
      file.path("data", "species"),
      root = root,
      sorting = "euring",
      optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "species"),
    root = root,
    name = "species",
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
      file.path("data", "vernacular"),
      root = root,
      sorting = c("euring", "language"),
      optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "vernacular"),
    root = root,
    name = "vernacular",
    title = "Vernacular species names",
    field_description = c(
      euring = "The European bird ringing code.",
      language = "Identifier of the language.",
      vernacular = "The vernacular name of the species."
    )
  )

  verify_vc(
    "location/locationgroup",
    root = raw_data,
    variables = c("external_code", "description")
  ) |>
    select("external_code", "description") |>
    write_vc(
      file.path("data", "locationgroup"),
      root = root,
      sorting = "external_code",
      optimize = FALSE
    )
  update_metadata(
    file = file.path("data", "locationgroup"),
    root = root,
    name = "locationgroup",
    title = "List of locationgroups",
    field_description = c(
      external_code = "The identifier of the location group.",
      description = "Full name of the location group"
    )
  )
}
