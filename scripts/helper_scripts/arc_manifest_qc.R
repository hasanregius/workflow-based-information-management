################################
# ARC Sample Manifest QC Script
# v1.1; 3/13/2024
################################

#' @export
arc_manifest_qc = function(manifest, fname) {
  # Dependencies ----
  require(glue)
  require(dplyr)
  require(stringr)
  require(odbc)
  require(DBI)
  require(striprtf)

  # Directories
  submission_dir = "~/rdc_files/submissions/"
  accepted_dir = "~/rdc_files/accepted/"
  validation_dir = "~/rdc_files/dvr/"

  # Static lists
  #  Fieldnames to extract
  arc_sample_fields = c("din","donor_id","cohort","collection_date",
                        "aliquot_type","aliquot_anticoagulant")
  values_anticoag = c("none", "edta", "multiple", "unavailable")
  values_aqtype = c("plasma","serum", "unavailable")
  values_cohort = c("IV","INV","NIV","NINV")

  # Default value
  if (missing(manifest) | missing(fname)) {
    stop("No manifest to perform QC on")
  }

  # Body ----
  # Start QC
    # Read file in ----
    cat("Initiating QC process", fill = T)
    cat("---------------------- \n", fill = T)

    # Setup ----
    #  Empty data frame for the validation report
    columns = c("issue","din","value")
    dvr = data.frame(matrix(nrow = 0, ncol = length(columns)))

    #  Remove all whitespaces
    cols_to_be_rectified = names(manifest)[vapply(manifest, is.character, logical(1))]
    for (i in 1:length(cols_to_be_rectified)) {
      manifest[,cols_to_be_rectified[i]] = gsub("\\s+","",manifest[,cols_to_be_rectified[i]])
      manifest[,cols_to_be_rectified[i]] = trimws(manifest[,cols_to_be_rectified[i]], which = "both")
    }

    # Field Check ----
    cat("Checking for incorrect fields and/or fieldnames", fill = T)
    names(manifest) = tolower(names(manifest))
    manifest = manifest %>% select(all_of(arc_sample_fields))
    # Skip entry check if fields are incorrect
    if (!(FALSE %in% (names(manifest) %in% arc_sample_fields)) &
        (!(FALSE %in% (all_of(arc_sample_fields) %in% names(manifest))))) {
      cat("> Passed field check, no errors found \n", fill = T)
      # din ----
      # Checking for NA entries
      cat("Checking for incorrect DIN entries", fill = T)
      if (T %in% is.na(manifest$din)) {
        #  If NA din entries were found ----
        temp = subset(manifest, is.na(manifest$din))
        cat(glue("> {nrow(temp)} NA entries found in the DIN field", fill = T))
        #  add to the DVR
        temp = data.frame(
          issue = "NA_value_din",
          din = NA,
          value = temp$donor_id)
        dvr = rbind(dvr, temp)
      } else {
        cat("> No NA in the DIN field. Checking for DIN format", fill = T)
        #  Checking for the number of characters ----
        if (mean(nchar(manifest$din)) != 13) {
          temp = subset(manifest, nchar(manifest$din) != 13)
          cat(glue("> {nrow(temp)} had an incorrect nchar for DIN"), fill = T)
          temp = data.frame(
            issue = "incorrect_nchar_din",
            din = temp$din,
            value = NA)
          dvr = rbind(dvr, temp)
        } else {
          cat("> No incorrect # of characters for DIN found", fill = T)
        }
        #  Checking for the correct format ----
        temp = subset(manifest, str_detect(manifest$din, "W", negate = T))
        if (nrow(temp) != 0) {
          cat(glue("> {nrow(temp)} had an incorrect DIN format \n "), fill = T)
          temp = data.frame(
            issue = "invalid_din_format",
            din = temp$din,
            value = NA)
          dvr = rbind(dvr, temp)
        } else {
          cat("> All dins were in the correct format \n", fill = T)
        }
      }

      # donorid ----
      cat("Checking Donor ID entries", fill = T)
      # Checking for NA entries
      if (T %in% is.na(manifest$donor_id)) {
        # If NA entries were found ----
        temp = subset(manifest, is.na(manifest$donor_id))
        cat(glue("> {nrow(temp)} NA entries in donor ID field", fill = T))
        temp = data.frame(
          issue = "NA_value_donorid",
          din = temp$din,
          value = temp$donor_id)
        dvr = rbind(dvr, temp)
      } else {
        # Checking for the number of characters ----
        if (mean(nchar(manifest$donor_id)) != 7 &
            mean(nchar(manifest$donor_id)) != 10) {
          temp = subset(manifest, nchar(manifest$donor_id) != 7)
          cat(glue("> {nrow(temp)} donor IDs had incorrect length \n", fill = T))
          temp = data.frame(
            issue = "invalid_donorid_nchar",
            din = temp$din,
            value = temp$donor_id)
          dvr = rbind(dvr, temp)
        } else {
          cat("> All donor IDs are the correct length \n", fill = T)
        }
      }
      # Collection_date ----
      cat("Checking collection date entries", fill = T)
      manifest$collection_date = as.Date(manifest$collection_date,
                                         tryFormats = c("%m/%d/%Y","%m-%d-%Y","%Y-%m-%d"))
      if (class(manifest$collection_date) != "Date") {
        # Formatting failed ----
        cat("> Failed to convert collection_date field to 'Date' format \n", fill = T)
        nr = nrow(dvr)+1
        dvr[nr,] = c("incorrect_date_formatting", NA, NA)
      } else {
        # Check for how many is NA (couldn't be converted) ----
        if (NA %in% manifest$collection_date) {
          valid = F
          temp = subset(manifest, is.na(manifest$collection_date))
          cat(glue("> {nrow(temp)} collection dates are NA"))
          temp = data.frame(
            issue = "NA_collection_date",
            din = temp$din,
            value = NA
          )
        } else {
          # Check for invalid dates (later than or on today's date) ----
          temp = subset(manifest, manifest$collection_date >= Sys.Date())
          if (nrow(temp) != 0) {
            valid = F
            cat(glue("> {nrow(temp)} had an invalid date \n"), fill = T)
            temp = data.frame(
              issue = "invalid_collection_date",
              din = temp$din,
              value = temp$collection_date)
            dvr = rbind(dvr, temp)
          } else {
            cat("> No invalid entries detected. \n", fill = T)
          }
        }
      }

      # cohort ----
      # Checking for invalid cohort entries
      cat("Checking for valid cohort values", fill = T)
      if (NA %in% manifest$cohort) {
        # if NA cohort entries found ----
        temp = subset(manifest, is.na(manifest$cohort))
        cat(glue("> {nrow(temp)} had NA as cohort"))
        temp = data.frame(
          issue = "NA_cohort_value",
          din = temp$din,
          value = NA
        )
        dvr = rbind(dvr, temp)
      } else {
        # Check if values are permitted ----
        temp = subset(manifest, manifest$cohort %in% values_cohort == F)
        if (nrow(temp) != 0) {
          can_check_db = F
          cat(glue("> {nrow(temp)} had invalid cohort values \n"), fill = T)
          temp = data.frame (
            issue = "invalid_cohort_entry",
            din = temp$din,
            value = temp$cohort
          )
          dvr = rbind(dvr, temp)
        } else {
          can_check_db = T
          cat("> Value check complete. No errors found.", fill = T)
        }
        # Check for consistency of cohort entries ----
        # Check for if we can connect to the Db
        arc_conn = connect_db()

        if (can_check_db == T & is.null(arc_conn) == F) {
          # Pull arc.specimens table
          db_samples = dbReadTable(arc_conn, Id(schema = "arc", table = "specimens")) %>%
            select(donor_id, cohort) %>%
            distinct()

          # Add prefix if it wasnt added
          if (substr(manifest[1,]$donor_id,1,3) != "ARC") {
            manifest$donor_id = paste0("ARC",manifest$donor_id)
          }

          # Subset for any inconsistent data ----
          if (T %in% (manifest$donor_id %in% db_samples$donor_id)) {
            temp = subset(manifest, manifest$donor_id %in% db_samples$donor_id)
            temp = subset(temp, temp$cohort != db_samples$cohort &
                            temp$donor_id == db_samples$donor_id)
            if (nrow(temp) != 0) {
              # Format it for reporting
              temp = data.frame(
                issue = "inconsistent_cohort_entry",
                din = temp$din,
                value = temp$donor_id)
              # Send a message
              cat(glue("> {nrow(temp)} inconsistent cohort values found \n"), fill = T)
              # Append to report
              dvr = rbind(dvr, temp)
            } else {
              cat("> Cohort value check complete. No inconstent entries detected. \n", fill = T)
            }
          } else {
            cat("> All donor IDs are new to the Db. Skipping cosistency check \n", fill = T)
          }
        } else {
          cat("> Can't check validity of cohort assignment. Aborting. \n", fill = T)
          rn = nrow(dvr)+1
          dvr[rn,] = c("no_db_connection", NA ,NA)
        }
      }
      # aliquot_type ----
      # Subset the incorrect data
      cat("Checking for aliquot type values", fill = T)
      temp = subset(manifest,
                    is.na(manifest$aliquot_type) |
                      !(manifest$aliquot_type %in% values_aqtype))
      if (nrow(temp) != 0) {
        cat(paste0("> ",nrow(temp)," Incorrect aliquot type values found \n"), fill = T)
        # Format it to report format
        temp = data.frame(
          issue = "invalid_aliquot_type_entry",
          din = temp$din,
          value = temp$aliquot_type)
        dvr = rbind(dvr, temp)
      } else {
        cat("> Aliquot type value check complete. No invalid entries detected \n", fill = T)
      }

      # aliquot_anticoagulant ----
      cat("Checking for anticoagulant values", fill = T)
      temp = subset(
        manifest,
        is.na(manifest$aliquot_anticoagulant) |
          !(manifest$aliquot_anticoagulant %in% values_anticoag))
      if (nrow(temp) != 0) {
        cat(paste0("> ",nrow(temp)," incorrect anticoagulant values found \n"), fill = T)
        # Format it to report format
        temp = data.frame(
          issue = "invalid_anticoagulant_entry",
          din = temp$din,
          value = temp$aliquot_anticoagulant)
        dvr = rbind(dvr, temp)
      } else {
        # Send a message
        cat("> Anticoagulant value check complete. No invalid entries detected \n", fill = T)
      }
    } else {
      cat("> Incorrect fields/fieldnames detected \n", fill = T)
      # Append to validation report
      badfields = names(manifest)[names(manifest) %in% arc_sample_fields == F]
      dvr = data.frame(
        issue = "incorrect_fieldname",
        din = NA,
        value = badfields
      )
    }
  # Report QC findings, if any ----
  if (nrow(dvr) == 0) {
    cat("Submission passed validation \n\n", fill = T)
    return(TRUE)
  } else {
    #  Write the report and open it
    cat("Submission failed validation", fill = T)
    dvr_fname = gsub(".csv", "_validation.csv", fname)
    dvrcon = paste0(validation_dir,dvr_fname)
    write.csv(dvr, file = dvrcon, row.names = T)
    return(FALSE)
  }
}
