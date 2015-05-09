library(watervogelanalysis)
walloon.connection <- connect_walloon_source(
  result.channel = n2khelper::connect_result()
)
duplicates <- import_walloon_source_data(
  location.file = "rStation.csv", 
  visit.file = "rVisits.csv", 
  data.file = "rData.csv", 
  path = ".", 
  walloon.connection = walloon.connection
)
duplicates
