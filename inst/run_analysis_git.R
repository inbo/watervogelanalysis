library(watervogelanalysis)
prepare_analysis(
  analysis.path = "~/analysis", 
  raw.connection = git_connection(
    repo.path = "~/rawdata",
    local.path = "watervogel",
    username = username,
    password = password
  )
)
