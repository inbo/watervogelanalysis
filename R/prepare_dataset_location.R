#' Read the locations for the raw datasource, save them to the git repository and the results database
#' @return a data.frame with the locations
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom dplyr %>% mutate select inner_join filter bind_rows distinct
#' @importFrom tibble tibble
#' @importFrom digest sha1
#' @importFrom tidyr gather
#' @importFrom n2kupdate store_location_group_location
#' @importFrom git2rdata write_vc
#' @importFrom purrr map2_chr pmap_chr
prepare_dataset_location <- function(
  result_channel,
  flemish_channel,
  walloon_repo,
  raw_repo,
  scheme_id,
  import_date = as.POSIXct(Sys.time())
){
  assert_that(is.string(scheme_id))

  # read the locations
  location <- read_location(
    result_channel = result_channel,
    flemish_channel = flemish_channel,
    walloon_repo = walloon_repo,
    import_date = import_date
  ) %>%
    mutate(
      fingerprint = pmap_chr(
        list(
          datasource = .data$datasource,
          external_code = .data$external_code,
          description = .data$description
        ),
        function(datasource, external_code, description) {
          sha1(
            list(
              datasource = datasource, external_code = external_code,
              description = description
            )
          )
        }
      )
    ) -> location

  # define and save location groups
  # \\u0137 is the ASCII code for e umlaut
  tibble(
    description = c(
      "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137",
      "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\\u0137",
      "Vogelrichtlijn Belgi\\u0137"
    ),
    Flanders = c(TRUE, NA, TRUE, TRUE, NA, TRUE),
    Wallonia = c(NA, TRUE, TRUE, NA, TRUE, TRUE),
    SPA = c(0, 0, 0, 1, 1, 1),
    Impute = c(
      "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vlaanderen",
      "Walloni\\u0137", "Belgi\\u0137"
    ),
    SubsetMonths = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    scheme = scheme_id
  ) %>%
    mutate(
      fingerprint = map2_chr(
        .data$scheme,
        .data$description,
        ~sha1(c(scheme = .x, description = .y))
      )
    ) -> location_group
  # get the fingerprint for imputation location.groups
  location_group %>%
    select(Impute = "fingerprint", ImputeDescription = "description") %>%
    inner_join(location_group, by = c("ImputeDescription" = "Impute")) ->
    location_group
  # define locations per location_group
  location_group %>%
    select(location_group = "fingerprint", "SPA", "Flanders", "Wallonia") %>%
    gather(
      key = "Region", value = "Include", "Flanders", "Wallonia", na.rm = TRUE
    ) -> lg
  l <- location %>%
    select(location = "fingerprint", "Region", "SPA")
  location_group_location <- lg %>%
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
    )
  # define datafield
  datafield_type <- tibble(description = "character")
  datafield <- tibble(
    Region = c("Flanders", "Wallonia"),
    table_name = c("DimLocationWV", "location.txt"),
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
      fingerprint = pmap_chr(
        list(
          tn = .data$table_name, pk = .data$primary_key, ft = .data$datafield_type,
          ds = .data$datasource
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
    )
  location <- datafield %>%
    select(datafield = "fingerprint", "datasource") %>%
    inner_join(location, by = "datasource")

  store_location_group_location(
    location_group_location = location_group_location %>%
      select(
        location_group_local_id = "location_group",
        location_local_id = "location"
      ),
    location_group = location_group %>%
      select(local_id = "fingerprint", "description", "scheme"),
    location = location %>%
      select(
        local_id = "fingerprint", datafield_local_id = "datafield",
        "external_code", "description"
      ) %>%
      mutate(parent_local_id = NA_character_),
    datafield = datafield %>%
      select(
        local_id = "fingerprint", "datasource", "table_name", "primary_key",
        "datafield_type"
      ),
    conn = result_channel$con
  )

  # store the datasets in the git repository
  location %>%
    select(ID = "fingerprint", "StartDate", "EndDate") %>%
    write_vc(
      file = "location.txt", sorting = c("ID", "StartDate"), stage = TRUE,
      root = raw_repo
    ) -> location_sha
  location_group %>%
    select(ID = "fingerprint", "Impute", "SubsetMonths") %>%
    write_vc(
      file = "locationgroup.txt", sorting = c("ID", "Impute"), stage = TRUE,
      root = raw_repo
    ) -> locationgroup_sha
  location_group_location %>%
    select(LocationGroupID = "location_group", LocationID = "location") %>%
    write_vc(
      file = "locationgrouplocation.txt", stage = TRUE, root = raw_repo,
      sorting = c("LocationGroupID", "LocationID")
    ) -> locationgrouplocation_sha
  hashes <- c(location_sha, locationgroup_sha, locationgrouplocation_sha)

  dataset <- tibble(
    filename = hashes,
    fingerprint = names(hashes),
    import_date = import_date,
    datasource = datasource_id_raw(result.channel = result_channel),
    stringsAsFactors = FALSE
  )

  return(
    list(Location = location, LocationGroup = location_group, Dataset = dataset)
  )
}
