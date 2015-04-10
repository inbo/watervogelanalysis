#' Prepare all datasets and a to do list of models
#' @export
#' @importFrom n2khelper check_single_character read_delim_git list_files_git check_single_strictly_positive_integer
#' @inheritParams prepare_analysis_dataset
prepare_analysis <- function(path = "."){
  path <- check_single_character(path)
  
  rawdata.files <- list_files_git(path = "watervogel")
  analysis <- lapply(rawdata.files, prepare_analysis_dataset, path = path)
  insufficient <- rawdata.files[sapply(analysis, is.null)]
  if(length(insufficient) > 0){
    dir.create(paste0(path, "/database"))
    save(
      insufficient, 
      file = paste0(path, "/database/insufficient_observation.rda")
    )
  }
  return(insufficient)
}
