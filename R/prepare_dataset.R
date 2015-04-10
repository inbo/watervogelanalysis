#' Prepare the raw datasets and save them to the git repository
#' 
#' The raw data is written to the git repository. All changes are always staged and committed. The commit is pushed when both username and password are provided.
#' @param scheme.id the id of the scheme
#' @param verbose Display a progress bar when TRUE (default)
#' @inheritParams n2khelper::auto_commit
#' @inheritParams n2khelper::odbc_connect
#' @export
#' @importFrom n2khelper check_single_logical check_single_strictly_positive_integer connect_result odbc_get_multi_id write_delim_git auto_commit odbc_get_id
#' @importFrom n2kanalysis select_factor_count_strictly_positive select_factor_threshold select_observed_range
#' @importFrom RODBC odbcClose
#' @importFrom plyr ddply
#' @examples
#' \dontrun{
#'  prepare_dataset()
#' }
prepare_dataset <- function(
  username, 
  password, 
  verbose = TRUE, 
  develop = TRUE, 
  scheme.id = odbc_get_id(
    table = "Scheme", variable = "Description", value = "Watervogels", develop = develop
  )
){
  verbose <- check_single_logical(verbose)
  scheme.id <- check_single_strictly_positive_integer(scheme.id)

  channel <- connect_result(develop = develop)
  
  #read and save locations to database
  import.date <- Sys.time()
  location <- read_location(develop = develop)
  
  database.id <- odbc_get_multi_id(
    data = location[, c("ExternalCode", "DatasourceID", "Description")],
    id.field = "ID", merge.field = c("ExternalCode", "DatasourceID"), table = "Location",
    channel = channel, create = TRUE
  )
  location <- merge(database.id, location)
  location$Description <- NULL
  
  # define and save location groups
  # \\u0137 is the ASCII code for e umlaut
  location.group <- data.frame(
    Description = c("Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\\u0137", "Vogelrichtlijn Belgi\\u0137"),
    Flanders = c(TRUE, FALSE, TRUE, TRUE, FALSE, TRUE),
    Wallonia = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SPA = c(FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
    Impute = c("Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137"),
    SubsetMonths = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SchemeID = scheme.id
  )
  database.id <- odbc_get_multi_id(
    data = location.group[, c("SchemeID", "Description")],
    id.field = "ID", merge.field = c("SchemeID", "Description"), table = "LocationGroup",
    channel = channel, create = TRUE
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
  flanders <- odbc_get_id(
    table = "Datasource", 
    variable = "Description", 
    value = "Raw data watervogels Flanders", 
    develop = develop
  )
  wallonia <- odbc_get_id(
    table = "Datasource", 
    variable = "Description", 
    value = "Raw data watervogels Wallonia", 
    develop = develop
  )
  location.group.location <- do.call(rbind, lapply(
    seq_along(location.group$ID),
    function(i){
      if(location.group$SPA[i]){
        selection <- location$SPA == 1
      } else {
        selection <- rep(TRUE, nrow(location))
      }
      if(location.group$Flanders[i]){
        if(!location.group$Wallonia[i]){
          selection <- selection & location$DatasourceID == flanders
        }
      } else {
        selection <- selection & location$DatasourceID != flanders
      }
      data.frame(
        LocationGroupID = location.group$ID[i],
        LocationID = location$ID[selection]
      )
    }
  ))
  database.id <- odbc_get_multi_id(
    data = location.group.location[, c("LocationGroupID", "LocationID")],
    id.field = "ID", merge.field = c("LocationGroupID", "LocationID"), 
    table = "LocationGroupLocation",
    channel = channel, create = TRUE
  )
  
  location <- location[
    order(location$ID), 
    c("ID", "StartDate", "EndDate", "ExternalCode", "DatasourceID")
  ]
  location.group <- location.group[order(location.group$ID), c("ID", "Impute", "SubsetMonths")]
  location.group.location <- location.group.location[
    order(
      location.group.location$LocationGroupID,
      location.group.location$LocationID
    ), 
    c("LocationGroupID", "LocationID")
  ]
  
  location.sha <- write_delim_git(
    x = location[, c("ID", "StartDate", "EndDate")], 
    file = "location.txt", path = "watervogel"
  )
  location.group.sha <- write_delim_git(
    x = location.group, file = "locationgroup.txt", path = "watervogel"
  )
  location.group.location.sha <- write_delim_git(
    x = location.group.location, file = "locationgrouplocation.txt", path = "watervogel"
  )
  
  dataset <- data.frame(
    FileName = c("location.txt", "locationgroup.txt", "locationgrouplocation.txt"),
    PathName = "watervogel",
    Fingerprint = c(location.sha, location.group.sha, location.group.location.sha),
    ImportDate = import.date,
    Obsolete = FALSE
  )
  database.id <- odbc_get_multi_id(
    data = dataset,
    id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"), 
    table = "Dataset", 
    channel = channel, create = TRUE
  )
  
  #read and save species
  species.list <- read_specieslist(limit = TRUE, develop = develop)
  database.id <- odbc_get_multi_id(
    data = species.list$species,
    id.field = "ID", merge.field = "ExternalCode", table = "Species",
    channel = channel, create = TRUE
  )
  species.constraint <- merge(database.id, species.list$species.constraint)
  
  
  
  # read and save observations
  if(verbose){
    progress <- "time"
  } else {
    progress <- "none"
  }
#   x <- subset(species.constraint, ExternalCode == 1210)
  dataset <- ddply(species.constraint, "ExternalCode", .progress = progress, function(x){
    import.date <- Sys.time()
    observation.flemish <- read_observation(
      species.id = x$ExternalCode[1], 
      first.winter = x$Firstyear[1], 
      species.covered = x$SpeciesCovered,
      develop = develop
    )
    observation.flemish$DatasourceID <- flanders
    
    observation.walloon <- read_observation_wallonia(
      species.id = x$ExternalCode[1], 
      first.winter = x$Firstyear[1]
    )
    if(is.null(observation.walloon)){
      observation <- observation.flemish
    } else {
      observation.walloon$DatasourceID <- wallonia
      observation <- rbind(observation.flemish, observation.walloon)
    }
    
    observation <- merge(
      observation, 
      location[, c("ID", "ExternalCode", "DatasourceID")], 
      by.x = c("LocationID", "DatasourceID"), 
      by.y = c("ExternalCode", "DatasourceID")
    )
    observation$LocationID <- observation$ID
    
    observation <- observation[
      order(observation$LocationID, observation$Date, observation$ObservationID),
      c("LocationID", "Date", "ObservationID", "Complete", "Count")
    ]
    observation <- select_relevant_import(observation)
    
    filename <- paste0(x$ID[1], ".txt")
    pathname <- "watervogel"
    if(is.null(observation)){
      observation.sha <- NA
    } else {
      observation.sha <- write_delim_git(
        x = observation, 
        file = filename, 
        path = pathname
      )
    }
    
    data.frame(
      FileName = filename,
      PathName = pathname,
      Fingerprint = observation.sha,
      ImportDate = import.date,
      Obsolete = FALSE
    )
  })
  dataset$ExternalCode <- NULL

  database.id <- odbc_get_multi_id(
    data = dataset,
    id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"), 
    table = "Dataset", 
    channel = channel, create = TRUE
  )

  # mark obsolete datasets
  sql <- "
    UPDATE
      Dataset
    SET
      Dataset.Obsolete = 1
    FROM
        Dataset
      INNER JOIN
        (
          SELECT
            FileName, 
            PathName, 
            Max(ImportDate) AS MostRecent
          FROM
            Dataset
          GROUP BY
            FileName, 
            PathName
        ) AS Recent
      ON 
        Dataset.FileName = Recent.FileName AND 
        Dataset.PathName = Recent.PathName
    WHERE
      Obsolete = 0 AND
      ImportDate < MostRecent
  "
  sqlQuery(channel = channel, query = sql)  

  auto_commit(
    package = environmentName(parent.env(environment())),
    username = username,
    password = password
  )

  odbcClose(channel)
  
  return(invisible(NULL))
}
