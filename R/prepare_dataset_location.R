#' Read the locations for the raw datasource, save them to the git repository and the results database
#' @return a data.frame with the locations
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom dplyr %>% rowwise mutate_ data_frame select_ arrange_
#' @importFrom digest sha1
#' @importFrom tidyr gather_
#' @importFrom n2kupdate store_location_group_location
prepare_dataset_location <- function(
  result.channel,
  flemish.channel,
  walloon.connection,
  raw.connection,
  scheme.id
){
  assert_that(is.string(scheme.id))

  import.date <- as.POSIXct(Sys.time())

  # read the locations
  location <- read_location(
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection
  ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(datasource, external_code, description)))

  # define and save location groups
  # \\u0137 is the ASCII code for e umlaut
  location.group <- data_frame(
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
    scheme = scheme.id
  ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(scheme = scheme, description = description)))
  # get the fingerprint for imputation location.groups
  location.group <- location.group %>%
    select_(Impute = ~fingerprint, ImputeDescription = ~description) %>%
    inner_join(location.group, by = c("ImputeDescription" = "Impute"))
  # define locations per location_group
  lg <- location.group %>%
    select_(location_group = ~fingerprint, ~SPA, ~Flanders, ~Wallonia) %>%
    gather_(
      key_col = "Region",
      value_col = "Include",
      gather_cols = c("Flanders", "Wallonia"),
      na.rm = TRUE
    )
  l <- location %>%
    select_(location = ~fingerprint, ~Region, ~SPA)
  location.group.location <- lg %>%
    filter_(~SPA == 0) %>%
    inner_join(l, by = "Region") %>%
    select_(~location_group, ~location) %>%
    bind_rows(
      lg %>%
        filter_(~SPA == 1) %>%
        inner_join(l %>%
          filter_(~SPA == 1),
          by = "Region"
        ) %>%
        select_(~location_group, ~location)
    )
  # define datafield
  datafield.type <- data_frame(
    description = "character"
  )
  datafield <- data_frame(
    Region = c("Flanders", "Wallonia"),
    table_name = c("tblGebied", "location.txt"),
    primary_key = c("Code", "LocationID"),
    datafield_type = c("character", "character")
  ) %>%
    inner_join(
      location %>%
        select_(~Region, ~datasource) %>%
        distinct_(),
      by = "Region"
    ) %>%
    select_(~-Region) %>%
    rowwise() %>%
    mutate_(
      fingerprint = ~ sha1(
        c(table_name, primary_key, datafield_type, datasource)
      )
    )
  location <- datafield %>%
    select_(datafield = ~fingerprint, ~datasource) %>%
    inner_join(location, by = "datasource")

  store_location_group_location(
    location_group_location = location.group.location %>%
      select_(
        location_group_local_id = ~location_group,
        location_local_id = ~location
      ),
    location_group = location.group %>%
      select_(
        local_id = ~fingerprint,
        ~description,
        ~scheme
      ),
    location = location %>%
      select_(
        local_id = ~fingerprint,
        datafield_local_id = ~datafield,
        ~external_code,
        ~description
      ) %>%
      mutate_(
        parent_local_id = ~NA_character_
      ),
    datafield = datafield %>%
      select_(
        local_id = ~fingerprint,
        ~datasource,
        ~table_name,
        ~primary_key,
        ~datafield_type
      ),
    conn = result.channel$con
  )

  # store the datasets in the git repository
  location.sha <- write_delim_git(
    x = location %>%
      select_(ID = ~fingerprint, ~StartDate, ~EndDate) %>%
      arrange_(~ID),
    file = "location.txt",
    connection = raw.connection
  )
  locationgroup.sha <- write_delim_git(
    x = location.group %>%
      select_(
        ID = ~fingerprint,
        ~Impute,
        ~SubsetMonths
      ) %>%
      arrange_(~ID, ~Impute),
    file = "locationgroup.txt",
    connection = raw.connection
  )
  locationgrouplocation.sha <- write_delim_git(
    x = location.group.location %>%
      select_(
        LocationGroupID = ~location_group,
        LocationID = ~location
      ) %>%
      arrange_(~LocationGroupID, ~LocationID),
    file = "locationgrouplocation.txt",
    connection = raw.connection
  )

  dataset <- data.frame(
    filename = c(
      "location.txt", "locationgroup.txt", "locationgrouplocation.txt"
    ),
    fingerprint = c(location.sha, locationgroup.sha, locationgrouplocation.sha),
    import_date = import.date,
    datasource = datasource_id_raw(result.channel = result.channel),
    stringsAsFactors = FALSE
  )

  return(list(
    Location = location,
    LocationGroup = location.group,
    Dataset = dataset
  ))
}
