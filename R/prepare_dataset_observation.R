#' Read the observations for the raw datasource, save them to the git repository and the results database
#' @param this.constraint the constraints for this species group
#' @param location a data frame with the full list of locations
#' @param location_group_id the ID of the location group
#' @param dataset a data frame with a
#' @inheritParams read_specieslist
#' @inheritParams prepare_dataset
#' @inheritParams connect_flemish_source
#' @export
#' @importFrom n2khelper check_dataframe_variable odbc_get_id odbc_get_multi_id check_id read_delim_git
#' @importFrom lubridate round_date year month
#' @importFrom assertthat assert_that is.string
#' @importFrom utils sessionInfo
#' @importFrom dplyr %>% distinct_ count_ filter_ mutate_ bind_rows select_ inner_join arrange_ transmute_ bind_rows
#' @importFrom n2kupdate store_analysis_dataset get_analysis_version store_anomaly store_observation
prepare_dataset_observation <- function(
  this.constraint,
  location,
  location_group_id,
  flemish.channel,
  walloon.connection,
  result.channel,
  raw.connection,
  scheme.id,
  dataset
){
  check_dataframe_variable(
    df = this.constraint,
    variable = c(
      "Firstyear", "ExternalCode", "DatasourceID", "SpeciesCovered",
      "SpeciesGroupID"
    ),
    name = "this.constraint"
  )
  check_dataframe_variable(
    df = location,
    variable = c("fingerprint", "external_code", "datafield"),
    name = "location"
  )
  external <- this.constraint %>%
    distinct_(~DatasourceID, ~ExternalCode) %>%
    count_(~DatasourceID) %>%
    filter_(~n > 1)
  if (nrow(external)) {
    stop("Each datasource must use just one ExternalCode")
  }
  external <- this.constraint %>%
    distinct_(~SpeciesGroupID, ~Firstyear)
  if (nrow(external) != 1) {
    stop(
      "this.constraint must contain just one SpeciesGroupID and one Firstyear"
    )
  }
  assert_that(is.string(scheme.id))
  assert_that(is.string(location_group_id))

  import.date <- as.POSIXct(Sys.time())
  flanders.id <- datasource_id_flanders(result.channel = result.channel)
  observation.flemish <- read_observation(
    species.id = this.constraint %>%
      filter_(~DatasourceID == flanders.id) %>%
      distinct_(~ExternalCode) %>%
      mutate_(ExternalCode = ~as.integer(ExternalCode)) %>%
      unlist(),
    first.winter = unique(this.constraint$Firstyear),
    last.winter = unique(this.constraint$Lastyear),
    species.covered = unique(this.constraint$SpeciesCovered),
    flemish.channel = flemish.channel
  ) %>%
    mutate_(DatasourceID = ~flanders.id)

  wallonia.id <- datasource_id_wallonia(result.channel = result.channel)
  if (any(this.constraint$DatasourceID == wallonia.id)) {
    observation.walloon <- read_observation_wallonia(
      species.id = this.constraint %>%
        filter_(~DatasourceID == wallonia.id) %>%
        distinct_(~ExternalCode) %>%
        unlist(),
      first.winter = unique(this.constraint$Firstyear),
      last.winter = unique(this.constraint$Lastyear),
      walloon.connection = walloon.connection
    )
  } else {
    observation.walloon <- NULL
  }
  if (is.null(observation.walloon)) {
    observation <- observation.flemish
  } else {
    observation <- bind_rows(
      observation.flemish %>%
        mutate_(
          ObservationID = ~as.character(ObservationID),
          LocationID = ~as.character(LocationID)
        ),
      observation.walloon %>%
        mutate_(DatasourceID = ~wallonia.id)
    )
  }

  result <- observation %>%
    mutate_(
      LocationID = ~as.character(LocationID),
      Year = ~round_date(Date, unit = "year") %>%
        year(),
      fMonth = ~ month(Date) %>%
        as.factor()
    ) %>%
    select_(~-Date) %>%
    inner_join(
      location %>%
        select_(~external_code, ~fingerprint, ~datasource),
      by = c(
        "LocationID" = "external_code",
        "DatasourceID" = "datasource"
      )
    ) %>%
    select_(
      ~DatasourceID,
      ~ObservationID,
      LocationID = ~fingerprint,
      ~Year,
      ~fMonth,
      ~Count,
      ~Complete
    ) %>%
    select_relevant_import() %>%
    mutate_(local_id = ~paste(DatasourceID, ObservationID))
  datafield <- result %>%
    distinct_(~DatasourceID) %>%
    transmute_(
      local_id = ~DatasourceID,
      datasource = ~DatasourceID,
      table_name = ~ifelse(
        DatasourceID == flanders.id,
        "tblWaarneming",
        "visit.txt"
      ),
      primary_key = ~ifelse(
        DatasourceID == flanders.id,
        "ID",
        "ObservationID"
      ),
      datafield_type = ~ifelse(
        DatasourceID == flanders.id,
        "integer",
        "character"
      )
    )
  parameter <- result %>%
    distinct_(~fMonth) %>%
    transmute_(
      local_id = ~as.character(fMonth),
      description = ~as.character(fMonth),
      parent_parameter_local_id = ~"fMonth"
    ) %>%
    bind_rows(
      data.frame(
        local_id = c("observation parameter", "fMonth"),
        description = c("observation parameter", "fMonth"),
        parent_parameter_local_id = c(NA, "observation parameter"),
        stringsAsFactors = FALSE
      )
    )
  result <- store_observation(
    datafield = datafield,
    observation = result %>%
      transmute_(
        ~local_id,
        external_code = ~ObservationID,
        datafield_local_id = ~DatasourceID,
        location_local_id = ~LocationID,
        year = ~Year,
        parameter_local_id = ~as.character(fMonth)
      ),
    location = location %>%
      semi_join(datafield, by = c("datasource" = "local_id")) %>%
      transmute_(
        local_id = ~fingerprint,
        parent_local_id = ~NA_character_,
        ~description,
        datafield_local_id = ~datasource,
        ~external_code
      ),
    parameter = parameter,
    conn = result.channel$con
  ) %>%
  inner_join(
    result,
    by = "local_id"
  ) %>%
  select_(
    ObservationID = ~fingerprint,
    ~LocationID,
    ~Year,
    ~fMonth,
    ~Count,
    ~Complete
  ) %>%
  remove_duplicate_observation()

  if (is.null(result$Observation)) {
    observation.sha <- NA
    analysis.status <- "No data"
  } else {
    observation <- result$Observation %>%
      select_(
        ~LocationID, ~Year, ~fMonth, ~ObservationID, ~Complete,
        ~Count
      ) %>%
      arrange_(~LocationID, ~Year, ~fMonth)

    filename <- paste0(this.constraint$SpeciesGroupID[1], ".txt")
    observation.sha <- write_delim_git(
      x = observation,
      file = filename,
      connection = raw.connection
    )
    analysis.status <- "converged"
    dataset <- data.frame(
      filename = filename,
      fingerprint = observation.sha,
      import_date = import.date,
      datasource = dataset$datasource[1],
      stringsAsFactors = FALSE
    ) %>%
      bind_rows(dataset)
  }

  model_set <- data.frame(
    local_id = "import",
    description = "import",
    first_year = this.constraint$Firstyear[1],
    last_year = this.constraint$Lastyear[1],
    duration = this.constraint$Lastyear[1] - this.constraint$Firstyear[1] + 1,
    stringsAsFactors = FALSE
  )
  analysis_version <- get_analysis_version(sessionInfo())
  analysis <- data.frame(
    model_set_local_id = "import",
    location_group = location_group_id,
    species_group = this.constraint$SpeciesGroupID[1],
    last_year = this.constraint$Lastyear[1],
    seed = sample(.Machine$integer.max, 1),
    analysis_version = attr(analysis_version, "AnalysisVersion") %>%
      unname(),
    analysis_date = import.date,
    status = analysis.status,
    stringsAsFactors = FALSE
  ) %>%
    mutate_(
      file_fingerprint = ~sha1(list(
        dataset = arrange_(dataset, ~fingerprint) %>%
          select_(~fingerprint, ~datasource, ~filename, ~import_date),
        model_set = select_(
          model_set,
          ~description, ~first_year, ~last_year, ~duration
        ),
        location_group = location_group,
        species_group = species_group,
        last_year = last_year,
        seed = seed,
        analysis_date = analysis_date
      )),
      status_fingerprint = ~sha1(list(
        file_fingerprint = file_fingerprint,
        status = status,
        analysis_version = analysis_version
      ))
    )
  analysis_dataset <- data.frame(
    analysis = analysis$file_fingerprint,
    dataset = dataset$fingerprint,
    stringsAsFactors = FALSE
  )
  store_analysis_dataset(
    analysis = analysis,
    model_set = model_set,
    analysis_version = analysis_version,
    dataset = dataset,
    analysis_dataset = analysis_dataset,
    conn = result.channel$con
  )

  if (nrow(result$Duplicate) > 0) {
    anomaly_type <- data.frame(
      local_id = 1,
      description = "multiple observations for a single location time combination",
      stringsAsFactors = FALSE
    )
    anomaly <- result$Duplicate %>%
      transmute_(
        anomaly_type_local_id = ~1,
        analysis = ~analysis$file_fingerprint,
        observation = ~as.character(ObservationID),
        parameter_local_id = ~NA_character_
      )
    store_anomaly(
      anomaly = anomaly,
      anomaly_type = anomaly_type,
      conn = result.channel$con
    )
  }
  return(analysis$file_fingerprint)
}
