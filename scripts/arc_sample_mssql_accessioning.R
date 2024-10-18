################################
# ARC MSSQL Sample Accessioning
# v2.0; 3/13/2024
################################
# Need to add classification. Samples shipped after 5/23 are for Q1.
# All samples prior is for pilot.

#' @export
arc_sample_accession = function(manifest) {
  # Dependencies and Setup ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)

  # Load the necessary scripts ----
  source("scripts/helper_scripts/db_connect.R")
  source("scripts/helper_scripts/db_notification.R")

  # Preemptive connection check. Abort if false.
  lc_conn = connect_db()

  # Body ----
  # If connection can be made, proceed
  if (is.null(lc_conn) == F) {
    # Pull reference fieldnames ----
    arc_sample_fields = dbListFields(lc_conn, "specimens", schema_name = "arc")
    arc_sample_fields = arc_sample_fields[arc_sample_fields != "pilot_study"]
    # ARC is shit, so accommodate their MANY ways of labeling donor_id
    names(manifest)[names(manifest) == "donor_ID"] = "donor_id"
    names(manifest)[names(manifest) == "pilot"] = "pilot_study"
    manifest$bco = "ARC"

    # Check for fields corresponding to ARC sample table ----
    if (all(arc_sample_fields %in% names(manifest))) {
      cat("Manifest field check complete, standardizing entries..\n", fill = T)
      # Renaming the fields to how it's captured in the database
      suzs = manifest %>% select(all_of(arc_sample_fields))
      suzs$aliquot_type[suzs$aliquot_type == "unavailable"] = NA
      suzs$aliquot_anticoagulant[suzs$aliquot_anticoagulant == "unavailable"] = NA
      # Turn date fields into date object if they aren't already
      #  Collection date
      if (is.Date(suzs$collection_date) == F) {
        suzs$collection_date = as.Date(suzs$collection_date,
                                       tryFormats = c("%m/%d/%Y", "%Y-%m-%d"))
      }
      #  Manifest date, for sampling period categorization
      if (is.Date(suzs$manifest_date) == F) {
        suzs$manifest_date = as.Date(suzs$manifest_date,
                                     tryFormats = c("%m/%d/%Y", "%Y-%m-%d"))
      }
      # Adding the ARC prefix in front of the ARC donor IDs
      suzs$donor_id[nchar(suzs$donor_id) == 7 & substr(suzs$donor_id,1,3) != "ARC"] = paste0(
        "ARC",suzs$donor_id[nchar(suzs$donor_id) == 7 & substr(suzs$donor_id,1,3) != "ARC"])
      # Accession into the Db ----
      #  Add the pilot_study variable
      arc_samples = dbReadTable(lc_conn, Id(schema = "arc", table = "specimens"))
      # arc_pilot_donors = arc_samples %>% filter(pilot_study == T)
      # suzs$pilot_study[suzs$donor_id %in% arc_pilot_donors$donor_id] = T
      #  Check for the list of DINs already in the Db
      arc_sample_list = arc_samples %>% pull("din")
      arc_samples_length = length(arc_sample_list)
      #  Accessioning sample data with transaction
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (suzs$din %in% arc_sample_list)) {
          cat("Duplicate DINs found, subsetting for unique IDs", fill = T)
          suzs = subset(suzs, suzs$din %in% arc_sample_list == F)
          dbWriteTable(lc_conn, Id(schema = "arc", table = "specimens"), suzs, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          dbWriteTable(lc_conn, Id(schema = "arc", table = "specimens"), suzs, append = TRUE)
        }
        # Check for if accessioning was successful
        arc_sample_list = lc_conn %>% tbl(in_schema("arc","specimens")) %>% pull("din")
        # Rollback in case of error
        if (length(arc_sample_list) - arc_samples_length != nrow(suzs)) {
          dsc = length(arc_sample_list) - (arc_samples_length + nrow(suzs))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," DINs failed to be imported. Aborting..\n"), fill = T)
          dbBreak()
        } else {
          cat("All ARC sample data was successfully imported", fill = T)
        }
      })
      # Check for if accessioning was successful
      arc_sample_list = lc_conn %>%
        tbl(in_schema("arc","specimens")) %>%
        pull("din")
      # Rollback in case of error
      if (length(arc_sample_list) - arc_samples_length != nrow(suzs)) {
        dsc = length(arc_sample_list) - (arc_samples_length + nrow(suzs))
        if (dsc < 0) {
          dsc = dsc - dsc*2
        }
        cat(paste0(dsc," DINs failed to be imported. Aborting..\n"), fill = T)
        return(FALSE)
      } else {
       # post_db_changes(bco = "ARC", changes_nrow = nrow(suzs),
       #                table = "samples", action = "created")
        return(TRUE)
      }
    } else {
      stop("Required sample fields are missing. Skipping sample accessioning.")
      return(FALSE)
    }
  } else {
    return(FALSE)
    stop("Can't connect to the Db. Check your VPN or contact IT.")
  }
}

