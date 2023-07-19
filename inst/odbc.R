library(keyring)
library(odbc)
connection <- c(
  Driver = "{ODBC Driver 18 for SQL Server}",
  Server = "inbo-sql08-prd.inbo.be,1433", Database = "S0008_00_Meetnetten",
  Trusted_Connection = "yes"
)
connection <- c(
  Driver = "{ODBC Driver 18 for SQL Server}",
  Server = "inbo-sql08-prd.inbo.be,1433", Database = "W0008_00_Meetnetten",
  Trusted_Connection = "yes"
)
sprintf("%s=%s;", names(connection), connection) |>
  paste(collapse = "") -> connection_string
connection_string
conn <- dbConnect(odbc(), .connection_string = connection_string)
conn <- dbConnect(
  odbc(), driver = "{ODBC Driver 18 for SQL Server}",
  server = "inbo-sql08-prd.inbo.be,1433", database = "W0008_00_Meetnetten",
  Trusted_Connection = "yes"
)
conn <- dbConnect(
  odbc(), driver = "{ODBC Driver 18 for SQL Server}",
  server = "inbo-sql08-prd.inbo.be,1433", database = "W0008_00_Meetnetten",
  uid = key_get("watervogels", "uid"), pwd = key_get("watervogels", "pwd")
)
conn <- dbConnect(odbc(), dsn = "WatervogelsDWH")
