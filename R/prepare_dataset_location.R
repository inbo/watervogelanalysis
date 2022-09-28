#' Read the locations for the raw data source, save them to the git repository
#' and the results database
#' @return a data.frame with the locations
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom dplyr %>% bind_rows distinct filter inner_join mutate rename
#' select
#' @importFrom rlang .data
#' @importFrom tibble tibble
#' @importFrom tidyr gather
#' @importFrom n2kupdate store_location_group_location
#' @importFrom git2rdata write_vc
#' @importFrom purrr pmap_chr
#' @importFrom digest sha1
prepare_dataset_location <- function(
  result_channel, flemish_channel, walloon_repo, raw_repo, scheme_id,
  first_date, latest_date = as.POSIXct(Sys.time())
) {
  assert_that(is.string(scheme_id))

  # read the locations
  read_location(
    result_channel = result_channel, flemish_channel = flemish_channel,
    walloon_repo = walloon_repo, first_date = first_date,
    latest_date = latest_date) %>%
    mutate(
      local_id = pmap_chr(
        list(
          datasource = .data$datasource, external_code = .data$external_code,
          description = .data$description
        ),
        function(datasource, external_code, description) {
          sha1(list(
              datasource = datasource, external_code = external_code,
              description = description
          ))
        }
      )
    ) -> location

  # define and save location groups
  # \u00EB is the ASCII code for e umlaut
  tibble(
    description = c(
      "Vlaanderen", "Walloni\u00EB", "Belgi\u0EB",
      "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\u00EB",
      "Vogelrichtlijn Belgi\u00EB"
    ),
    Flanders = c(TRUE, NA, TRUE, TRUE, NA, TRUE),
    Wallonia = c(NA, TRUE, TRUE, NA, TRUE, TRUE),
    SPA = c(0, 0, 0, 1, 1, 1),
    Impute = c(
      "Vlaanderen", "Walloni\u00EB", "Belgi\u00EB", "Vlaanderen",
      "Walloni\u00EB", "Belgi\u00EB"
    ),
    SubsetMonths = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    scheme = scheme_id
  ) %>%
    mutate(
      local_id = map2_chr(
        .data$scheme,
        .data$description,
        ~sha1(c(scheme = .x, description = .y))
      )
    ) -> location_group
  # get the fingerprint for imputation location.groups
  location_group %>%
    select(Impute = "local_id", ImputeDescription = "description") %>%
    inner_join(location_group, by = c("ImputeDescription" = "Impute")) ->
    location_group
  # define locations per location_group
  location_group %>%
    select(location_group = "local_id", "SPA", "Flanders", "Wallonia") %>%
    gather(
      key = "Region", value = "Include", "Flanders", "Wallonia", na.rm = TRUE
    ) -> lg
  location %>%
    select(location = "local_id", "Region", "SPA") -> l
  lg %>%
    filter(.data$SPA == 0) %>%
    inner_join(l, by = "Region") %>%
    select("location_group", "location") %>%
    bind_rows(
      lg %>%
        filter(.data$SPA == 1) %>%
        inner_join(l %>%
          filter(.data$SPA == 1),
          by = "Region"
        ) %>%
        select("location_group", "location")
    ) -> location_group_location
  # define datafield
  tibble(
    Region = c("Flanders", "Wallonia"),
    table_name = c("DimLocationWV", "location"),
    primary_key = c("LocationWVCode", "LocationID"),
    datafield_type = c("character", "character")
  ) %>%
    inner_join(
      location %>%
        select("Region", "datasource") %>%
        distinct(),
      by = "Region"
    ) %>%
    select(-"Region") %>%
    mutate(
      local_id = pmap_chr(
        list(
          tn = .data$table_name, pk = .data$primary_key,
          ft = .data$datafield_type, ds = .data$datasource
        ),
        function(tn, pk, ft, ds) {
          sha1(
            list(
              table_name = tn, primary_key = pk, datafield_type = ft,
              datasource = ds
            )
          )
        }
      )
    ) -> datafield
  datafield %>%
    select("datasource", datafield = "local_id") %>%
    inner_join(location, by = "datasource") -> location

  stored <- store_location_group_location(
    location_group_location = location_group_location %>%
      select(
        location_group_local_id = "location_group",
        location_local_id = "location"
      ),
    location_group = location_group %>%
      select("local_id", "description", "scheme"),
    location = location %>%
      select(
        "local_id", datafield_local_id = "datafield", "external_code",
        "description"
      ) %>%
      mutate(parent_local_id = NA_character_),
    datafield = datafield,
    conn = result_channel$con
  )

  # store the datasets in the git repository
  stored$location %>%
    select("local_id", "fingerprint", "datafield_local_id") %>%
    inner_join(
      datafield %>%
        select("local_id", "table_name", "datafield_type", "primary_key"),
      by = c("datafield_local_id" = "local_id")) %>%
    inner_join(location, by = "local_id") -> location
  location %>%
    select(ID = "fingerprint", "StartDate", "EndDate") %>%
    write_vc(
      file = "location", sorting = c("ID", "StartDate"), stage = TRUE,
      root = raw_repo
    ) -> location_sha
  stored$location_group %>%
    select("local_id", "fingerprint") %>%
    inner_join(location_group, by = "local_id") -> location_group
  location_group %>%
    select(ID = "fingerprint", "Impute", "SubsetMonths") %>%
    write_vc(
      file = "locationgroup.txt", sorting = c("ID", "Impute"), stage = TRUE,
      root = raw_repo
    ) -> locationgroup_sha
  location_group_location %>%
    inner_join(
      stored$location_group %>%
        select("local_id", lgf = "fingerprint"),
      by = c("location_group" = "local_id")
    ) %>%
    inner_join(
      stored$location %>%
        select("local_id", lf = "fingerprint"),
      by = c("location" = "local_id")
    ) %>%
    select(LocationGroupID = "lgf", LocationID = "lf") %>%
    write_vc(
      file = "locationgrouplocation.txt", stage = TRUE, root = raw_repo,
      sorting = c("LocationGroupID", "LocationID")
    ) -> locationgrouplocation_sha
  hashes <- c(location_sha, locationgroup_sha, locationgrouplocation_sha)

  dataset <- tibble(
    filename = hashes,
    fingerprint = names(hashes),
    import_date = Sys.time(),
    datasource = datasource_id_raw(result_channel = result_channel)
  )

  return(
    list(Location = location, LocationGroup = location_group, Dataset = dataset)
  )
}
