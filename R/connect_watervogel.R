#' Opens an ODBC connection to the 'Watervogel' database
#' @export
#' @importFrom RODBC odbcDriverConnect
connect_watervogel <- function(){
  odbcDriverConnect("Driver=SQL Server;Server=INBOSQL03\\PRD;Database=Watervogels;Trusted_Connection=Yes;")
}
