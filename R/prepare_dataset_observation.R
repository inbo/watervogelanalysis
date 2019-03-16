#' Read the observations for the raw datasource, save them to the git repository and the results database
#' @param this_species the species in this species group
#' @param location a data frame with the full list of locations
#' @param location_group_id the ID of the location group
#' @param dataset a data frame with a
#' @inheritParams prepare_dataset
#' @inheritParams connect_flemish_source
#' @export
#' @importFrom git2rdata read_vc write_vc
#' @importFrom assertthat assert_that is.string has_name
#' @importFrom utils sessionInfo
#' @importFrom dplyr %>% distinct count filter mutate bind_rows select inner_join arrange transmute pull
#' @importFrom tidyr complete
#' @importFrom n2kupdate store_analysis_dataset store_observation store_datafield
#' @importFrom n2kanalysis get_analysis_version
#' @importFrom digest sha1
#' @importFrom purrr pmap_chr
#' @importFrom rlang .data
prepare_dataset_observation <- function(
  this_species, location, location_group_id, flemish_channel, walloon_repo,
  result_channel, raw_repo, dataset
){
  assert_that(
    inherits(this_species, "data.frame"), inherits(location, "data.frame"),
    has_name(this_species, "DatasourceID"),
    has_name(this_species, "ExternalCode"),
    has_name(location, "fingerprint"), has_name(location, "external_code"),
    has_name(location, "datafield"), is.string(location_group_id)
  )
  if (anyDuplicated(this_species[, c("DatasourceID", "ExternalCode")])) {
    stop("Each datasource must use just one ExternalCode")
  }

  read_vc(file = "metadata", root = raw_repo) %>%
    filter(.data$species_id == unique(this_species$species_id)) -> metadata

  import_date <- as.POSIXct(Sys.time())
  flanders_id <- datasource_id_flanders(result_channel = result_channel)
  read_observation(
    species_id = this_species %>%
      filter(.data$DatasourceID == flanders_id) %>%
      mutate(ExternalCode = as.integer(.data$ExternalCode)) %>%
      dplyr::pull(.data$ExternalCode),
    first_year = metadata$first_imported_year,
    latest_year = metadata$last_imported_year,
    flemish_channel = flemish_channel
  ) %>%
    mutate(
      DatasourceID = flanders_id,
      ObservationID = as.character(.data$ObservationID)
    ) -> observation_flemish

  wallonia_id <- datasource_id_wallonia(result_channel = result_channel)
  if (wallonia_id %in% this_species$DatasourceID) {
    observation_walloon <- read_observation_wallonia(
      species_id = this_species %>%
        filter(.data$DatasourceID == wallonia_id) %>%
        dplyr::pull(.data$ExternalCode),
      first_year = metadata$first_imported_year,
      latest_year = metadata$last_imported_year,
      walloon_repo = walloon_repo
    )
  } else {
    observation_walloon <- NULL
  }
  if (is.null(observation_walloon)) {
    observation <- observation_flemish
  } else {
    observation <- bind_rows(
      observation_flemish,
      observation_walloon %>%
        mutate(DatasourceID = wallonia_id)
    )
  }

  observation %>%
    inner_join(
      location %>%
        select("external_code", DatasourceID = "datasource",
               LocationID = "fingerprint"),
      by = c("external_code", "DatasourceID")
    ) %>%
    select("DatasourceID", "TableName", "ObservationID", "Year", "Month",
           "LocationID", "Count", "Complete") %>%
    select_relevant_import() %>%
    mutate(
      Month = factor(.data$Month, levels = c(1:3, 10:12),
                     labels = c("Januari", "Februari", "March", "October",
                                "November", "December")),
      LocationID = factor(.data$LocationID)
    ) %>%
    complete(.data$Year, .data$Month, .data$LocationID) %>%
    mutate(
      DatasourceID = ifelse(is.na(.data$DatasourceID),
                     metadata$results_datasource_id[1], .data$DatasourceID),
      TableName = ifelse(is.na(.data$TableName), "observation",
                         .data$TableName)) -> result
  if (nrow(result) == 0) {
    observation_sha <- NA
    analysis_status <- "No data"
    model_set <- data.frame(local_id = "import", description = "import",
      first_year = metadata$first_imported_year,
      last_year = metadata$last_imported_year,
      duration = metadata$last_imported_year - metadata$first_imported_year + 1,
      stringsAsFactors = FALSE)
    analysis_version <- get_analysis_version(sessionInfo())
    analysis <- data.frame(
      model_set_local_id = "import", location_group = location_group_id,
      species_group = this_species$species_group_id[1],
      last_year = metadata$last_imported_year,
      seed = sample(.Machine$integer.max, 1),
      analysis_version = attr(analysis_version, "AnalysisVersion") %>%
        unname(),
      analysis_date = import_date, status = analysis_status,
      stringsAsFactors = FALSE
    ) %>%
      mutate(file_fingerprint = sha1(list(
          dataset = dataset %>%
            arrange(.data$fingerprint) %>%
            select("fingerprint", "datasource", "filename", "import_date"),
          model_set = model_set %>%
            select("description","first_year", "last_year", "duration"),
          location_group = .data$location_group,
          species_group = .data$species_group, last_year = .data$last_year,
          seed = .data$seed, analysis_date = .data$analysis_date
        )),
        status_fingerprint = sha1(list(
          file_fingerprint = .data$file_fingerprint, status = .data$status,
          analysis_version = .data$analysis_version
        ))
      )
    analysis_dataset <- data.frame(analysis = analysis$file_fingerprint,
                                   dataset = dataset$fingerprint,
                                   stringsAsFactors = FALSE)
    store_analysis_dataset(analysis = analysis, model_set = model_set,
                           analysis_version = analysis_version, dataset = dataset,
                           analysis_dataset = analysis_dataset,
                           conn = result_channel$con)

    return(analysis$file_fingerprint)
  }
  result %>%
    filter(is.na(.data$ObservationID)) %>%
    mutate(ObservationID = pmap_chr(
      list(d = .data$DatasourceID, y = .data$Year, m = .data$Month,
           l = .data$LocationID),
      function(d, y, m, l) {
        sha1(list(d, y, m, l))
      }
    )) %>%
    bind_rows(
      result %>%
        filter(!is.na(.data$ObservationID))
    ) %>%
    mutate(local_id = paste(.data$DatasourceID, .data$TableName,
                            .data$ObservationID),
           datafield_local_id = paste(.data$DatasourceID, .data$TableName)) ->
    result

  result %>%
    distinct(.data$DatasourceID, .data$TableName) %>%
    inner_join(
      x = data.frame(
        datasource = c(flanders_id, wallonia_id, wallonia_id,
                       metadata$results_datasource_id[1]),
        table_name = c("FactAnalyseSetOccurrence", "visit", "data",
                       "observation"),
        primary_key = c("ID", "ObservationID", "ObservationID", "id"),
        datafield_type = c("integer", "character", "character", "integer"),
        stringsAsFactors = FALSE
      ),
      by = c(datasource = "DatasourceID", table_name = "TableName")
    ) %>%
    bind_rows(
      location %>%
        distinct(.data$datasource, .data$table_name, .data$primary_key,
                 .data$datafield_type)
    ) %>%
    mutate(local_id = paste(.data$datasource, .data$table_name)) -> datafield
  store_datafield(datafield = datafield, conn = result_channel$con) %>%
    filter(.data$datasource == metadata$results_datasource_id[1],
           .data$table_name == "observation") %>%
    dplyr::pull("fingerprint") -> obs_df
  result %>%
    distinct(.data$Month) %>%
    transmute(local_id = as.character(.data$Month),
              description = as.character(.data$Month),
              parent_parameter_local_id = "Month") %>%
    bind_rows(
      data.frame(
        local_id = c("observation parameter", "Month"),
        description = c("observation parameter", "Month"),
        parent_parameter_local_id = c(NA, "observation parameter"),
        stringsAsFactors = FALSE
      )
    ) -> parameter
  store_observation(
    datafield = datafield,
    observation = result %>%
      transmute(.data$local_id, external_code = .data$ObservationID,
        .data$datafield_local_id, location_local_id = .data$LocationID,
        year = .data$Year, parameter_local_id = .data$Month
      ),
    location = location %>%
      transmute(local_id = .data$fingerprint, parent_local_id = NA_character_,
                .data$description, .data$external_code,
                datafield_local_id = paste(.data$datasource, .data$table_name)
      ),
    parameter = parameter,
    conn = result_channel$con
  ) %>%
  inner_join(result, by = "local_id") %>%
  mutate(DataFieldID = obs_df) %>%
  select("DataFieldID", ObservationID = "fingerprint", "LocationID", "Year",
         "Month", "Count", "Complete") -> result

  if (nrow(result) == 0) {
    observation_sha <- NA
    analysis_status <- "No data"
  } else {
    observation_sha <- write_vc(x = result,
      file = this_species$species_group_id[1],
      sorting = c("Year", "Month", "LocationID"), stage = TRUE, root = raw_repo)
    analysis_status <- "converged"
    data.frame(
      filename = observation_sha, fingerprint = names(observation_sha),
      import_date = import_date, datasource = dataset$datasource[1],
      stringsAsFactors = FALSE
    ) %>%
      bind_rows(dataset) -> dataset
  }

  model_set <- tibble(local_id = "import", description = "import",
    first_year = metadata$first_imported_year,
    last_year = metadata$last_imported_year,
    duration = metadata$last_imported_year - metadata$first_imported_year + 1)
  analysis_version <- get_analysis_version(sessionInfo())
  analysis <- tibble(
    model_set_local_id = "import", location_group = location_group_id,
    species_group = this_species$species_group_id[1],
    last_year = metadata$last_imported_year,
    seed = sample(.Machine$integer.max, 1),
    analysis_version = attr(analysis_version, "AnalysisVersion") %>%
      unname(),
    analysis_date = import_date, status = analysis_status
  ) %>%
    mutate(file_fingerprint = sha1(list(
        dataset = dataset %>%
          arrange(.data$fingerprint) %>%
          select("fingerprint", "datasource", "filename", "import_date"),
        model_set = model_set %>%
          select("description","first_year", "last_year", "duration"),
        location_group = .data$location_group,
        species_group = .data$species_group, last_year = .data$last_year,
        seed = .data$seed, analysis_date = .data$analysis_date
      )),
      status_fingerprint = sha1(list(
        file_fingerprint = .data$file_fingerprint, status = .data$status,
        analysis_version = .data$analysis_version
      ))
    )
  analysis_dataset <- data.frame(analysis = analysis$file_fingerprint,
                                 dataset = dataset$fingerprint,
                                 stringsAsFactors = FALSE)
  store_analysis_dataset(analysis = analysis, model_set = model_set,
                         analysis_version = analysis_version, dataset = dataset,
                         analysis_dataset = analysis_dataset,
                         conn = result_channel$con)

  return(analysis$file_fingerprint)
}
