library(watervogelanalysis)
walloon.connection <- connect_walloon_source(
  result.channel = n2khelper::connect_result(develop = TRUE), 
  username = username, 
  password = password
)
duplicates <- import_walloon_source_data(
  location.file = "rStation.csv", 
  visit.file = "rVisits.csv", 
  data.file = "rData.csv", 
  path = ".", 
  walloon.connection = walloon.connection
)
