#' Read the locations for the raw datasource, save them to the git repository and the results database
#' @return a data.frame with the locations
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_multi_id write_delim_git read_delim_git
#' @importFrom n2kanalysis mark_obsolete_dataset
#' @importFrom RODBC odbcClose
#' @importFrom assertthat assert_that is.count
prepare_dataset_location <- function(
  result.channel,
  flemish.channel,
  walloon.connection,
  raw.connection,
  scheme.id
){
  assert_that(is.count(scheme.id))

  import.date <- Sys.time()

  # read the locations
  location <- read_location(
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    walloon.connection = walloon.connection
  )

  # store the locations
  database.id <- odbc_get_multi_id(
    data = location[, c("ExternalCode", "DatasourceID", "Description")],
    id.field = "ID",
    merge.field = c("ExternalCode", "DatasourceID"),
    table = "Location",
    channel = result.channel,
    create = TRUE
  )
  location <- merge(database.id, location)
  location$Description <- NULL

  # define and save location groups
  # \\u0137 is the ASCII code for e umlaut
  location.group <- data.frame(
    Description = c(
      "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137",
      "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\\u0137",
      "Vogelrichtlijn Belgi\\u0137"
    ),
    Flanders = c(TRUE, FALSE, TRUE, TRUE, FALSE, TRUE),
    Wallonia = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SPA = c(FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
    Impute = c(
      "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vlaanderen",
      "Walloni\\u0137", "Belgi\\u0137"
    ),
    SubsetMonths = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SchemeID = scheme.id
  )
  database.id <- odbc_get_multi_id(
    data = location.group[, c("SchemeID", "Description")],
    id.field = "ID",
    merge.field = c("SchemeID", "Description"),
    table = "LocationGroup",
    channel = result.channel,
    create = TRUE
  )
  location.group <- merge(database.id, location.group)
  location.group <- merge(
    location.group,
    location.group[, c("ID", "Description")],
    by.x = "Impute",
    by.y = "Description",
    suffixes = c("", ".y")
  )
  location.group$Impute <- location.group$ID.y

  location.group$ID.y <- NULL
  location.group$SchemeID <- NULL
  location.group$Description <- NULL

  # get the locations per location group
  flanders <- datasource_id_flanders(result.channel = result.channel)
  wallonia <- datasource_id_wallonia(result.channel = result.channel)
  locationgroup.location <- do.call(rbind, lapply(
    seq_along(location.group$ID),
    function(i) {
      if (location.group$SPA[i]) {
        selection <- location$SPA == 1
      } else {
        selection <- rep(TRUE, nrow(location))
      }
      if (location.group$Flanders[i]) {
        if (!location.group$Wallonia[i]) {
          selection <- selection & location$DatasourceID == flanders
        }
      } else {
        selection <- selection & location$DatasourceID == wallonia
      }
      data.frame(
        LocationGroupID = location.group$ID[i],
        LocationID = location$ID[selection]
      )
    }
  ))
  database.id <- odbc_get_multi_id(
    data = locationgroup.location[, c("LocationGroupID", "LocationID")],
    id.field = "ID", merge.field = c("LocationGroupID", "LocationID"),
    table = "LocationGroupLocation",
    channel = result.channel, create = TRUE
  )

  location <- location[
    order(location$ID),
    c("ID", "StartDate", "EndDate", "ExternalCode", "DatasourceID")
  ]
  location.group <- location.group[
    order(location.group$ID),
    c("ID", "Impute", "SubsetMonths")
  ]
  locationgroup.location <- locationgroup.location[
    order(
      locationgroup.location$LocationGroupID,
      locationgroup.location$LocationID
    ),
    c("LocationGroupID", "LocationID")
  ]


  # store the datasets in the git repository
  location.sha <- write_delim_git(
    x = location[, c("ID", "StartDate", "EndDate")],
    file = "location.txt",
    connection = raw.connection
  )
  locationgroup.sha <- write_delim_git(
    x = location.group,
    file = "locationgroup.txt",
    connection = raw.connection
  )
  locationgrouplocation.sha <- write_delim_git(
    x = locationgroup.location,
    file = "locationgrouplocation.txt",
    connection = raw.connection
  )

  dataset <- data.frame(
    FileName = c(
      "location.txt", "locationgroup.txt", "locationgrouplocation.txt"
    ),
    PathName = raw.connection@LocalPath,
    Fingerprint = c(location.sha, locationgroup.sha, locationgrouplocation.sha),
    ImportDate = import.date,
    Obsolete = FALSE
  )
  database.id <- odbc_get_multi_id(
    data = dataset,
    id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"),
    table = "Dataset",
    channel = result.channel, create = TRUE
  )
  mark_obsolete_dataset(channel = result.channel)

  return(location)
}
