library(git2rdata)
library(keyring)
library(knitr)
library(quarto)
library(tidyverse)
n_head <- Inf
root <- key_get("meetnetten", username = "watervogels_result")
results_folder <- file.path(root, "data")
target_folder <- file.path(root, "source", "website")
dir.create(target_folder, showWarnings = FALSE)

system.file("css_styles", package = "INBOmd") |>
  file.copy(to = target_folder)
file.path(target_folder, "css_styles") |>
  file.copy(from = "custom.css", overwrite = TRUE)
c("index.qmd", "species_list.qmd") |>
  file.copy(target_folder, overwrite = TRUE)

read_vc("analysis", results_folder) |>
  # filter(
  #   .data$locationgroup %in%
  #     c("BEL", "VLAA", "WAL", "WALN2K", "BELN2K", "VLN2K")
  # ) |>
  filter(str_detect(.data$species, "^[0-9]+$")) |>
  count(species = as.integer(.data$species), .data$locationgroup) |>
  slice_max(.data$n, n = n_head, with_ties = FALSE) |>
  arrange(.data$species, .data$locationgroup) |>
  filter(
    map2_lgl(
      .data$species, .data$locationgroup,
      ~sprintf(
          "%s/%s/trend.csv",
          str_replace_all(.y, " ", "-") |>
            tolower(),
          .x
        ) |>
        is_git2rdata(root = results_folder)
      )
  ) |>
  inner_join(
    read_vc("species", results_folder), by = c("species" = "euring")
  ) |>
  inner_join(
    read_vc("locationgroup", results_folder) |>
      mutate(
        description = str_replace_all(.data$description, "België", "Belgium") |>
          str_replace_all("Vlaanderen", "Flanders") |>
          str_replace_all("Wallonië", "Wallonia")
      ),
    by = c("locationgroup" = "external_code")
  ) |>
  mutate(
    output_file = str_replace_all(.data$scientific, " ", "-") |>
      tolower() |>
      sprintf(
        fmt = "%2$s/%1$s/%3$s.qmd", target_folder,
        tolower(.data$description) |>
          str_replace_all(" ", "-") |>
          str_replace("ë", "e")
      )
  ) -> species_location

for (i in seq_len(nrow(species_location))) {
  message(species_location$output_file[i])
  dirname(species_location$output_file[i]) |>
    dir.create(showWarnings = FALSE, recursive = TRUE)
  knit_expand(
    "species_location.qmd", this_species_name = species_location$scientific[i],
    species = species_location$species[i],
    locationgroup = species_location$locationgroup[i],
    this_location_name = species_location$description[i],
    this_default_reference = 2024
  ) |>
    writeLines(con = species_location$output_file[i])
}

species_location |>
  distinct(.data$species, .data$scientific) |>
  mutate(
    output_file = str_replace_all(.data$scientific, " ", "-") |>
      tolower() |>
      sprintf(fmt = "%2$s/%1$s/index.qmd", target_folder)
  ) -> species
for (i in seq_len(nrow(species))) {
  message(species$output_file[i])
  dirname(species$output_file[i]) |>
    dir.create(showWarnings = FALSE, recursive = TRUE)
  knit_expand(
    "species.qmd", this_species_name = species$scientific[i]
  ) |>
    writeLines(con = species$output_file[i])
}

readLines("_quarto.yml") |>
  c(
    "    - text: \"Introduction\"", "      file: index.qmd",
    "    - section: \"By species\"", "      contents:",
    species_location |>
      mutate(
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "        - text: \"%2$s\"\n          file: %1$s",
            .data$description
          )
      ) |>
      group_by(.data$species) |>
      summarise(
        yml = paste(.data$yml, collapse = "\n") |>
          sprintf(fmt = "\n        contents:\n%s")
      ) |>
      left_join(x = species, by = "species") |>
      arrange(.data$species) |>
      transmute(
        yml = replace_na(.data$yml, ""),
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "      - text: \"%2$s\"
        file: %1$s%3$s",
            .data$scientific, .data$yml
          )
      ) |>
      pull(.data$yml)
  ) |>
  writeLines(file.path(target_folder, "_quarto.yml"))

species$output_file |>
  dirname() |>
  sort() |>
  walk(
    ~quarto_render(.x, use_freezer = TRUE, cache = TRUE, as_job = FALSE),
    .progress = TRUE
  )
