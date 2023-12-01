library(git2rdata)
library(knitr)
library(quarto)
library(tidyverse)
conflicted::conflicts_prefer(dplyr::pull)
n_head <- Inf
target_folder <- "../quarto"
dir.create(target_folder, showWarnings = FALSE)

system.file("css_styles", package = "INBOmd") |>
  file.copy(to = target_folder, recursive = TRUE)
file.path(target_folder, "css_styles") |>
  file.copy(from = "custom.css", overwrite = TRUE)
c("index.qmd", "species_list.qmd") |>
  file.copy(target_folder, overwrite = TRUE)

read_vc("species", "../results") |>
  select("id", species = "name_sc") |>
  inner_join(read_vc(
    "speciesgroup_species", "../results"), by = c("id" = "species")
  ) |>
  semi_join(
    read_vc("observed", "../results") |>
      mutate(speciesgroup = as.integer(.data$species_group_id)),
    by = "speciesgroup"
  ) |>
  arrange(.data$speciesgroup) |>
  head(n_head) |>
  mutate(
    output_file = str_replace_all(.data$species, " ", "-") |>
      tolower() |>
      sprintf(fmt = "%2$s/%1$s/index.qmd", target_folder)
  ) -> species
for (i in seq_len(nrow(species))) {
  dirname(species$output_file[i]) |>
    dir.create(showWarnings = FALSE)
  knit_expand(
    "species.qmd", species = species$species[i],
    species_group_id = species$speciesgroup[i]
  ) |>
    writeLines(species$output_file[i])
}

read_vc("observed", "../results") |>
  mutate(speciesgroup = as.integer(.data$species_group_id)) |>
  inner_join(species, by = "speciesgroup") |>
  distinct(.data$species, .data$speciesgroup, .data$location_group_id) |>
  inner_join(
    read_vc("locationgroup", "../results") |>
      transmute(
        location_group_id = as.character(.data$id),
        locationgroup = .data$description
      ),
    by = "location_group_id"
  ) |>
  arrange(.data$speciesgroup, as.integer(.data$location_group_id)) |>
  head(n_head) |>
  mutate(
    output_file = sprintf(
      "%s/%s/%s.qmd", target_folder, .data$species, .data$locationgroup
    ) |>
      tolower() |>
      str_replace_all(" ", "-"),
    title = sprintf("_%s_ in `%s`", .data$species, .data$locationgroup)
  ) |>
  left_join(
    read_vc("yearly_change", "../results") |>
      group_by(
        speciesgroup = as.integer(.data$species_group_id),
        .data$location_group_id
      ) |>
      summarise(reference = min(.data$reference), .groups = "drop"),
    by = c("speciesgroup", "location_group_id")
  ) -> species_location
for (i in seq_len(nrow(species_location))) {
  dirname(species_location$output_file[i]) |>
    dir.create(showWarnings = FALSE)
  knit_expand(
    "species_location.qmd", this_species_name = species_location$species[i],
    this_species = species_location$speciesgroup[i],
    this_location = species_location$location_group_id[i],
    this_location_name = species_location$locationgroup[i],
    title = species_location$title[i],
    this_default_reference = species_location$reference[i]
  ) |>
    writeLines(species_location$output_file[i])
}

read_vc("locationgroup", "../results") |>
  transmute(
    location_group_id = as.character(.data$id),
    locationgroup = .data$description
  ) |>
  head(n_head) |>
  mutate(
    output_file = sprintf(
      "%s/%s/index.qmd", target_folder, .data$locationgroup
    ) |>
      tolower() |>
      str_replace_all(" ", "-")
  ) -> location
for (i in seq_len(nrow(location))) {
  dirname(location$output_file[i]) |>
    dir.create(showWarnings = FALSE)
  knit_expand(
    "location.qmd", this_location = location$location_group_id[i],
    this_location_name = location$locationgroup[i]
  ) |>
    writeLines(location$output_file[i])
}

readLines("_quarto.yml") |>
  c(
    "    - text: \"Introduction\"", "      file: index.qmd",
    "    - text: \"Species list\"", "      file: species_list.qmd",
    "    - section: \"By location\"", "      contents:",
    sprintf(
      "      - text: %s\n        file: %s", location$locationgroup,
      gsub(paste0(target_folder, "/"), "", location$output_file)
    ),
    "    - section: \"By species\"", "      contents:",
    species_location |>
      mutate(
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "        - text: \"%2$s\"\n          file: %1$s",
            .data$locationgroup
          )
      ) |>
      group_by(.data$species) |>
      summarise(
        yml = paste(.data$yml, collapse = "\n") |>
          sprintf(fmt = "\n        contents:\n%s")
      ) |>
      left_join(x = species, by = "species") |>
      arrange(.data$speciesgroup) |>
      transmute(
        yml = replace_na(.data$yml, ""),
        yml = str_remove(.data$output_file, paste0(target_folder, "/")) |>
          sprintf(
            fmt = "      - text: \"%2$s\"
        file: %1$s%3$s",
            .data$species, .data$yml
          )
      ) |>
      pull(.data$yml)
  ) |>
  writeLines(file.path(target_folder, "_quarto.yml"))

species$output_file |>
  dirname() |>
  walk(
    ~quarto_render(.x, use_freezer = TRUE, cache = TRUE, as_job = FALSE),
    .progress = TRUE
  )
location$output_file |>
  dirname() |>
  walk(
    ~quarto_render(.x, use_freezer = TRUE, cache = TRUE, as_job = FALSE),
    .progress = TRUE
  )
target_folder |>
  list.files(pattern = ".qmd", full.names = TRUE) |>
  walk(
    ~quarto_render(.x, use_freezer = TRUE, cache = TRUE, as_job = FALSE),
    .progress = TRUE
  )
