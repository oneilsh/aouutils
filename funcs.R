library(ROhdsiWebApi)
library(dplyr)
library(allofus)

con <- aou_connect()

# given a cohort ID from ATLAS, pulls it (quick wrapper around allofus aou_atlas_cohort function: https://roux-ohdsi.github.io/allofus/man/aou_atlas_cohort.html)
atlas_cohort <- function(cohort_id, 
                         atlas_base_url = "https://atlas-demo.ohdsi.org/WebAPI", 
                         collect = FALSE) {
    cd <- ROhdsiWebApi::getCohortDefinition(cohort_id, atlas_base_url)
    cd_sql <- ROhdsiWebApi::getCohortSql(cd, atlas_base_url, generateStats = FALSE)
    cohort_df <- allofus::aou_atlas_cohort(
        cohort_definition = cd,
        cohort_sql = cd_sql,
        debug = FALSE,
        collect = collect
    )
    cohort_df
}

# given a concept set ID from ATLAS, matches it against a cohort.
# basically a quick wrapper around allofus aou_concept_set, but
# grabbing the concept IDs from atlas first w/ ROhdsiWebApi
# https://roux-ohdsi.github.io/allofus/man/aou_concept_set.html
#
# NOTE: if the number of individual concepts is very large, this won't work
# because it builds the SQL by embedding the list of concept IDs and there a max query length.
# Alternative strategy: robustify this by creating a temp table for joining against
# with allofus::aou_create_temp_table, and modify allofus::aou_concept_set to use it (or create it), 
# or use concept_ancestor per usual w/ OMOP
atlas_concept_set <- function(concept_set_id,
                              cohort,
                              atlas_base_url = "https://atlas-demo.ohdsi.org/WebAPI", 
                              ...) {

  cd <- ROhdsiWebApi::getConceptSetDefinition(concept_set_id, atlas_base_url)
  resolved <- ROhdsiWebApi::resolveConceptSet(cd, atlas_base_url)
  concept_ids <- unique(as.integer(resolved))  # already a vector

  aou_concept_set(cohort = cohort, concepts = concept_ids, con = con, ...)
}

# print a tally, returns input for use in |> 
# (tally() can be called on a dbplyr table to count rows/groups if grouped)
tally_through <- function(df, desc = "Rows: ") {
  cat(desc)
  cat("\n")
  cat(paste(capture.output(tally(df)), collapse = "\n"))
  df
}

# given an OHDSI cohort (person_ids with start/end dates), a set of concept_ids, and set of domains to look in,
# selects those patients with any incidence in the date range,
# and gives the date of the first. Basically a wrapper with 'first date' logic around aou_concept_set,
# with the same caveats as above)
first_concept_set_dates <- function(cohort, 
                                    concepts,
                                    domains,
                                    start_date = "observation_period_start_date", 
                                    end_date = "observation_period_end_date",
                                    date_col_name = "first_cset_date"
                                    ) {
    # for use later in the pipeline
    rename_lookup <- setNames("concept_date", date_col_name)
    
    concept_dates <- aou_concept_set(cohort, 
                                     concepts = concepts, 
                                     start_date = start_date, 
                                     end_date = end_date,
                                     output = "all") |>
    filter(!is.na(concept_date)) |>
    group_by(person_id) |>
    dbplyr::window_order(concept_date) |>
    filter(row_number() == 1) |>
    select(person_id, concept_date) |>
    rename(all_of(rename_lookup))
    
    return(concept_dates)
}

# pulls the head of a remote table and returns it as
# a plain data.frame for nice printing in jupyter notebook
glimpse <- function(df) {
    df |> arrange(person_id) |> head() |> as.data.frame()
}
