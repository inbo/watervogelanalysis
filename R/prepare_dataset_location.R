#' Read the locations for the raw data source, save them to the git repository
#' and the results database
#' @return a data.frame with the locations
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom assertthat assert_that is.string noNA
#' @importFrom dplyr bind_rows distinct filter mutate select
#' @importFrom git2rdata update_metadata write_vc
#' @importFrom n2kanalysis get_datafield_id
#' @importFrom rlang .data
prepare_dataset_location <- function(
  flemish_channel, walloon_repo, raw_repo, scheme_id = "watervogels",
  first_date, latest_date = as.POSIXct(Sys.time())
) {
  assert_that(is.string(scheme_id), noNA(scheme_id))

  # read the locations
  fl_loc <- get_datafield_id(
    table = "DimLocationWV", field = "LocationWVCode",
    datasource = "W0004_00_Waterbirds database", root = raw_repo, stage = TRUE
  )
  wal_loc <- get_datafield_id(
    table = "location", field = "id", datasource = "Wallonia waterbirds repo",
    root = raw_repo, stage = TRUE
  )
  read_location(
    flemish_channel = flemish_channel, walloon_repo = walloon_repo,
    first_date = first_date, latest_date = latest_date
  ) |>
    mutate(
      datafield = ifelse(.data$region == "Flanders", fl_loc, wal_loc),
    ) -> location
  stopifnot("duplicated `id` for location" = anyDuplicated(location$id) == 0)
  location |>
    select(-"region") |>
    write_vc(
      file = "location/location", root = raw_repo, stage = TRUE,
      sorting = "id"
    ) -> hashes
  update_metadata(
    file = "location/location", root = raw_repo, stage = TRUE,
    name = "location", title = "List of locations",
    field_description = c(
      id = "Internal identifier of the location.",
      external_code = "Identifier of the location in the original data source.",
      datafield = "The matching id in the datafield table.",
      start_date = paste(
        "Optional start date of the location.",
        "If not provided, the location is assumed to be active from the first",
        "imported year."
      ),
      end_date = paste(
        "Optional end date of the location.",
        "If not provided, the location is assumed to be active until the last",
        "imported year."
      ),
      description = "Name of the location.",
      natura2000 = "Whether the location is part of the Natura2000 network."
    )
  )

  # read the Flemish location groups
  "WITH cte_parent AS (
      SELECT DISTINCT
          CAST(
            COALESCE(ParentLocationWVKey, LocationWVKey) AS VARCHAR
          ) AS parent
      FROM DimLocationWVParent
    )
SELECT
  lp.LocationGroupCode AS external_code,
  lp.LocationGroupNaam AS description, lp.LocationWVCode AS location
FROM FactLocationGroup AS lp
INNER JOIN cte_parent AS cp ON cp.parent = lp.LocationWVKey
WHERE
  LocationGroupTypeNaam = '(R-)Analyses' AND
  LocationGroupCode != 'A_EVRL' AND
  LocationGroupCode != 'VLAA'" |>
    dbGetQuery(conn = flemish_channel) |>
    mutate(
      location = paste("F", .data$location, sep = "_")
    ) -> extract_location_groups

  # define and save location groups
  # \u00EB is the ASCII code for e umlaut
  extract_location_groups |>
    distinct(.data$external_code, .data$description) |>
    mutate(
      impute = "VLAA",
      datafield = get_datafield_id(
        table = "FactLocationGroup", field = "LocationGroupCode", stage = TRUE,
        datasource = "W0004_00_Waterbirds database", root = raw_repo
      )
    ) |>
    bind_rows(
      data.frame(
        external_code = c("VLAA", "BEL", "WAL", "VLN2K", "BELN2K", "WALN2K"),
        description = c(
          "Vlaanderen", "Belgi\u00EB", "Walloni\u00EB", "Natura2000 Vlaanderen",
          "Natura2000 Belgi\u00EB", "Natura2000 Walloni\u00EB"
        ),
        impute = c("VLAA", "BEL", "BEL", "VLAA", "BEL", "BEL")
      )
    ) |>
    mutate(
      subset_months = .data$impute == "BEL",
      impute = factor(.data$impute)
    ) -> locationgroup
    stopifnot(
      "duplicated `external_code` for location groups" =
        anyDuplicated(locationgroup$external_code) == 0
    )
    write_vc(
      locationgroup, file = "location/locationgroup", root = raw_repo,
      sorting = "external_code", stage = TRUE
    ) |>
      c(hashes) -> hashes
  update_metadata(
    file = "location/locationgroup", root = raw_repo, stage = TRUE,
    name = "locationgroup", title = "List of groups of locations",
    description =
"A location group defines a set of locations for which we calculate the
statistics.",
    field_description = c(
      external_code =
        "The code of the location group as defined in the origal data source",
      datafield = "The matching id in the datafield table",
      description = "The name of the location group",
      impute =
        "Which location group to use for imputation of this location group",
      subset_months = "Whether to subset the months.
Flemish data covers 6 winter months, whereas Walloon data covers only 4 months."
    )
  )

  # define locations per location_group
  extract_location_groups |>
    select(locationgroup = "external_code", "location") |>
    bind_rows(
      location |>
        transmute(
          locationgroup = ifelse(.data$region == "Flanders", "VLAA", "WAL"),
          location = .data$id
        ),
      location |>
        filter(.data$natura2000 == 1) |>
        transmute(
          locationgroup = ifelse(.data$region == "Flanders", "VLN2K", "WALN2K"),
          location = .data$id
        ),
      location |>
        transmute(locationgroup = "BEL", location = .data$id),
      location |>
        filter(.data$natura2000 == 1) |>
        transmute(locationgroup = "BELN2K", location = .data$id)
    ) |>
    mutate(
      locationgroup = factor(.data$locationgroup),
      location = factor(.data$location)
    ) |>
    write_vc(
      "location/locationgroup_location", root = raw_repo,
      sorting = c("locationgroup", "location"), stage = TRUE
    ) |>
    c(hashes) -> hashes
  update_metadata(
    file = "location/locationgroup_location", root = raw_repo, stage = TRUE,
    name = "locationgroup_location",
    title = "List of locations per location group",
    field_description = c(
      locationgroup = "The identifier of the location group",
      location = "The identifier of the location"
    )
  )

  dataset <- data.frame(
    filename = hashes, fingerprint = names(hashes), import_date = Sys.time()
  )

  return(
    list(location = location, locationgroup = locationgroup, dataset = dataset)
  )
}
