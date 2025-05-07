library(git2rdata)
library(keyring)
library(knitr)
library(quarto)
library(tidyverse)
n_head <- Inf
root <- key_get("meetnetten", username = "watervogels_result")
results_folder <- file.path(root, "data")
target_folder <- file.path(root, "source", "website")
source_folder <- system.file("website", package = "watervogelanalysis")

file.path(target_folder, "css_styles") |>
  dir.create(showWarnings = FALSE, recursive = TRUE)

file.path(source_folder, "custom.css") |>
  file.copy(to = file.path(target_folder, "css_styles"), overwrite = TRUE)
file.path(source_folder, "index.md") |>
  file.copy(to = target_folder, overwrite = TRUE)
file.path(source_folder, "species_list.qmd") |>
  file.copy(to = target_folder, overwrite = TRUE)
file.path(source_folder, "libs") |>
  list.files(full.names = TRUE) |>
  file.copy(to = file.path(target_folder), overwrite = TRUE)

read_vc("locationgroup", results_folder) |>
  mutate(
    external_code = factor(
      .data$external_code,
      levels = c(
        "BEL",
        "BELN2K",
        "VLAA",
        "VLN2K",
        "WAL",
        "WALN2K",
        "NOH",
        "LO",
        "RO",
        "SEST",
        "WETL",
        "ZS",
        "ZSVAL",
        "ZR"
      )
    ),
    type = c("average", "maximum") |>
      list() |>
      rep(n())
  ) |>
  unnest("type") |>
  arrange(.data$external_code) |>
  head(2 * n_head) |>
  mutate(
    description = str_replace(.data$description, "België", "Belgium") |>
      str_replace("Vlaanderen", "Flanders") |>
      str_replace("Wallonië", "Brussels & Wallonia"),
    output_file = str_remove(.data$description, "& ") |>
      str_replace_all(" ", "-") |>
      tolower() |>
      sprintf(fmt = "%2$s/%3$s/%1$s.qmd", target_folder, .data$type)
  ) -> locationgroup
for (i in seq_len(nrow(locationgroup))) {
  message(locationgroup$output_file[i])
  dirname(locationgroup$output_file[i]) |>
    dir.create(showWarnings = FALSE, recursive = TRUE)
  file.path(source_folder, "location.qmd") |>
    knit_expand(
      this_location_name = locationgroup$description[i],
      this_location_code = locationgroup$external_code[i],
      type = locationgroup$type[i]
    ) |>
    writeLines(con = locationgroup$output_file[i])
}

read_vc("analysis", results_folder) |>
  filter(str_detect(.data$species, "^[0-9]+$")) |>
  count(species = as.integer(.data$species)) |>
  slice_max(.data$n, n = n_head, with_ties = FALSE) |>
  arrange(.data$species) |>
  inner_join(
    read_vc("species", results_folder),
    by = c("species" = "euring")
  ) |>
  arrange(.data$scientific) |>
  mutate(
    output_file = str_replace_all(.data$scientific, " ", "-") |>
      tolower() |>
      sprintf(fmt = "%2$s/%1$s.qmd", target_folder)
  ) -> species
for (i in seq_len(nrow(species))) {
  message(species$output_file[i])
  dirname(species$output_file[i]) |>
    dir.create(showWarnings = FALSE, recursive = TRUE)
  file.path(source_folder, "species.qmd") |>
    knit_expand(species = species$species[i]) |>
    writeLines(con = species$output_file[i])
}

file.path(source_folder, "_quarto.yml") |>
  readLines() |>
  c(
    "    - text: \"Introduction\"",
    "      file: index.md",
    "    - section: \"By region\"",
    "      contents:",
    locationgroup |>
      mutate(
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "         - text: \"winter %2$s\"\n           file: %1$s",
            .data$type
          )
      ) |>
      arrange(.data$type) |>
      group_by(.data$description) |>
      summarise(
        first = head(.data$output_file, 1) |>
          str_remove(paste0(target_folder, "/")),
        yml = paste(.data$yml, collapse = "\n")
      ) |>
      mutate(
        yml = sprintf(
          "       - section: \"%s\"\n         file: %s\n         contents:\n%s",
          .data$description,
          .data$first,
          .data$yml
        )
      ) |>
      pull(.data$yml),
    "    - section: \"By species\"",
    "      file: species_list.qmd",
    "      contents:",
    "      - species_list.qmd",
    species |>
      mutate(
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "      - text: \"%2$s\"\n        file: %1$s",
            .data$scientific
          )
      ) |>
      pull(.data$yml)
  ) |>
  writeLines(file.path(target_folder, "_quarto.yml"))
old_wd <- getwd()
on.exit(setwd(old_wd), add = TRUE)
setwd(target_folder)
quarto_add_extension("inbo/flandersqmd-website@draft", no_prompt = TRUE)
quarto_preview()
