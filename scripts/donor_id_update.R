###########################
# Donor ID Updating Script
# v1.3; 3/13/2024
###########################
# Note: the script requires the RDC
# suite of scripts to function

#' @export
update_donor_ids = function(file_directory) {
  # Dependencies ----
  require(yaml)
  require(dplyr)
  require(progress)
  require(glue)

  # Read in the config
  conf = read_yaml("scripts/config.yml")

  # Load the scripts
  script_names = c("db_connect.R","update_mssql_db.R","db_notification.R")
  if (all(file.exists(paste0("scripts/helper_scripts/",script_names)))) {
    for (i in 1:length(script_names)) {
      source(paste0("scripts/helper_scripts/",script_names[i]))
    }
  } else {
    stop("no database connection script")
  }

  # Static variables
  fieldnames = c("SOURCE_DONOR","TARGET_DONOR","MERGE_DATE_YYYYMMDD")

  # Process the file ----
  filename = str_split(file_directory,"/")[[1]][length(str_split(file_directory,"/")[[1]])]
  cat(glue("Reading in the merged donor id list: {filename}"), fill = T)
  file = read.csv(file_directory)
  if (all(fieldnames %in% names(file))) {
    cat("> All required fieldnames are present \n", fill = T)
    # Standardizing the file
    file = file %>%
      select(previous_donor_id = fieldnames[1],
             current_donor_id = fieldnames[2],
             change_date = fieldnames[3]) %>%
      mutate(change_date = as.Date(as.character(change_date), format = "%Y%m%d"),
             update_file = filename)
    file$previous_donor_did = NA
    file$current_donor_did = NA

    # Standardizing the donor_ids
    if (TRUE %in% (nchar(file$previous_donor_id) != 7)) {
      if (TRUE %in% (nchar(file$previous_donor_id) > 7)) {
        stop("donor_id character count is too long")
      } else {
        file$previous_donor_id[nchar(file$previous_donor_id)<7] = str_pad(file$previous_donor_id[nchar(file$previous_donor_id)<7], width = 7, side = "left", pad = "0")
      }
    }
    if (TRUE %in% (nchar(file$current_donor_id) != 7)) {
      if (TRUE %in% (nchar(file$current_donor_id) > 7)) {
        stop("donor_id character count is too long")
      } else {
        file$current_donor_id[nchar(file$current_donor_id)<7] = str_pad(file$current_donor_id[nchar(file$current_donor_id)<7], width = 7, side = "left", pad = "0")
      }
    }
    file$previous_donor_id = paste0("VTL",file$previous_donor_id)
    file$current_donor_id = paste0("VTL",file$current_donor_id)
    file$merged_id = paste0(file$previous_donor_id, file$current_donor_id)
  } else {
    stop("incorrect file format")
  }

  # Pull the relevant tables from the database ----
  conn = connect_db()
  cat("Pulling the appropriate tables from the study database", fill = T)
  if (!is.null(conn)) {
    donors = dbReadTable(conn, "donors")
    merged = dbReadTable(conn, "merged_donor_ids")
    merged_nrow = nrow(merged)
    merged$merged_id = paste0(merged$previous_donor_id, merged$current_donor_id)
    cat("> All relevant tables pulled \n", fill = T)
  } else {
    stop("connection to the database couldn't be made")
  }

  # Subset for relevant donor ids ----
  cat("Subsetting for relevant donor ids", fill = T)
  cat(glue("> Initial file has {nrow(file)} records"), fill = T)
  file = file %>%
    filter(previous_donor_id %in% donors$donor_id &
             merged_id %in% merged$merged_id == F &
             !duplicated(merged_id)) %>%
    select(-merged_id)
  cat(glue("> {nrow(file)} records will be used to update the database  \n"), fill = T)

  # Add in the previous donor dids from the database ----
  for (i in 1:nrow(file)) {
    file[i,]$previous_donor_did = donors$donor_did[donors$donor_id == file[i,]$previous_donor_id]
  }

  # Check for current donor ids in the existing donors table ----
  cat("Checking for donor id records to merge in the database", fill = T)
  dup_target = subset(file, file$current_donor_id %in% donors$donor_id)
  file = subset(file, file$current_donor_id %in% donors$donor_id == F)

  # If there are current donor ids already in the donors table, process it ----
  if (nrow(dup_target) > 0) {
    cat(glue("> {nrow(dup_target)} donor ids require a merge"), fill = T)
    # Update the reference table to also include previous donor did ----
    for (i in 1:nrow(dup_target)) {
      # Looking for previous donor_did
      previous_did = donors$donor_did[donors$donor_id == dup_target[i,]$previous_donor_id]
      dup_target[i,]$previous_donor_did = previous_did
      # Update the reference table with previous_did
      file$previous_donor_did[file$current_donor_id == dup_target[i,]$current_donor_id] = previous_did
      # Looking for current donor_did
      current_did = donors$donor_did[donors$donor_id == dup_target[i,]$current_donor_id]
      dup_target[i,]$current_donor_did = current_did
    }
    # Visits table processing ----
    visits = dbReadTable(conn, "visits")
    visits_dup = dup_target %>% filter(previous_donor_id %in% visits$donor_id)
    # If there are preexisting records, update them
    if (nrow(visits_dup) > 0) {
      # Set up a progress bar with a total number of updates
      cat(glue("  > Processing {nrow(visits_dup)} records in the visits table.."), fill = T)
      nrow_update = nrow(visits_dup)
      rows_left = nrow_update
      progbar = progress_bar$new(total = nrow_update, format = "[:bar] :percent ETA: :eta", width = 50)
      # Begin a transaction
      dbBegin(conn)
      # Loop over and paginate the update
      while(rows_left > 0) {
        if (rows_left >= 1000) {
          # Update the database
          dbExecute(conn, paste0("UPDATE dbo.visits SET donor_id = ? WHERE donor_id = ?"),
                    params = list(visits_dup[1:1000,"current_donor_id"], visits_dup[1:1000,"previous_donor_id"]))
          # Advance the progress bar
          progbar$tick(1000)
          # Remove the updated records
          visits_dup = visits_dup[1001:nrow(visits_dup),]
          rows_left = nrow(visits_dup)
        } else {
          # Update the database
          dbExecute(conn, paste0("UPDATE dbo.visits SET donor_id = ? WHERE donor_id = ?"),
                    params = list(visits_dup[1:nrow(visits_dup),"current_donor_id"], visits_dup[1:nrow(visits_dup),"previous_donor_id"]))
          # Advance the progress bar
          progbar$tick(nrow(visits_dup))
          # Remove the updated records
          rows_left = rows_left-nrow(visits_dup)
        }
      }
      # Commit the transaction
      dbCommit(conn)
    } else {
      cat("  > No records to process in the visits table", fill = T)
    }
    # Survey_responses table processing ----
    survey_responses = dbReadTable(conn, "survey_responses")
    init_nrow = nrow(survey_responses)
    survey_responses_dup = dup_target %>% filter(previous_donor_did %in% survey_responses$donor_did)
    nrow_survey_responses_dup = nrow(survey_responses_dup)
    # If there are preexisting records, update them
    if (nrow(survey_responses_dup) > 0) {
      # Set up a progress bar with a total number of updates
      nrow_update = nrow(survey_responses_dup)
      rows_left = nrow_update
      progbar = progress_bar$new(total = nrow_update, format = "[:bar] :percent ETA: :eta", width = 50)
      # Begin a transaction
      dbBegin(conn)
      # Loop over and paginate the update
      while(rows_left > 0) {
        if (rows_left >= 1000) {
          # Update the database
          dbExecute(conn, paste0("UPDATE dbo.survey_responses SET donor_did = ? WHERE donor_did = ?"),
                    params = list(survey_responses_dup[1:1000,"current_donor_did"], survey_responses_dup[1:1000,"previous_donor_did"]))
          # Advance the progress bar
          progbar$tick(1000)
          # Remove the updated records
          survey_responses_dup = survey_responses_dup[1001:nrow(survey_responses_dup),]
          rows_left = nrow(survey_responses_dup)
        } else {
          # Update the database
          dbExecute(conn, paste0("UPDATE dbo.survey_responses SET donor_did = ? WHERE donor_did = ?"),
                    params = list(survey_responses_dup[1:nrow(survey_responses_dup),"current_donor_did"], survey_responses_dup[1:nrow(survey_responses_dup),"previous_donor_did"]))
          # Advance the progress bar
          progbar$tick(nrow(survey_responses_dup))
          # Remove the updated records
          rows_left = rows_left-nrow(survey_responses_dup)
        }
      }
      survey_responses = dbReadTable(conn, "survey_responses")
      if (nrow(survey_responses) - init_nrow == nrow_survey_responses_dup) {
        cat(glue("  > Processed {nrow_survey_responses_dup} records in the survey_responses table.."), fill = T)
        # Commit the transaction
        dbCommit(conn)
      } else {
        cat("  > Failed to process records in the survey_responses table", fill = T)
      }
    } else {
      cat("  > No records to process in the survey_responses table", fill = T)
    }

    # Remove the old merged donor ids from the donors table ----
    nrow_update = nrow(dup_target)
    rows_left = nrow_update
    init_donors_nrow = nrow(donors)
    # Begin a transaction
    dbBegin(conn)
    # Remove from the database
    dbExecute(conn, paste0("DELETE FROM dbo.donors WHERE donor_id = ? AND donor_did = ?"),
              params = list(dup_target[,"previous_donor_id"], dup_target[,"previous_donor_did"]))
    # Check for if the record deletion was successful
    donors = dbReadTable(conn, "donors")
    final_donors_nrow = nrow(donors)
    if (init_donors_nrow - final_donors_nrow == nrow_update) {
      cat("  > Record(s) successfully removed from the donors table \n", fill = T)
      # Post notification
      # post_db_changes(bco = "VTL", table = "donors", changes_nrow = nrow_update,
      #                 action = "deleted to reflect donor id merge")
      # Commit the transaction
      dbCommit(conn)
    } else {
      cat("  > Could not remove record(s) from the donors table \n", fill = T)
      dbRollback(conn)
    }
  } else {
    cat("> no donor id records require a merge \n", fill = T)
  }

  # Update the donors table ----
  cat(glue("Donors table update"), fill = T)
  file_update = rbind(file, dup_target)

  #  Update the donor_id ----
  donorids = donors$donor_id
  nrow_update = nrow(file)
  dbBegin(conn)
  # Update the donor ids
  drow = dbExecute(conn, paste0("UPDATE donors SET donor_id = ? WHERE donor_id = ?"),
                   params = list(file[,"current_donor_id"], file[,"previous_donor_id"]))
  if (drow == nrow_update) {
    cat(glue("> Updated all {nrow_update} donor ids \n"), fill = T)
    # Commit the transaction
    dbCommit(conn)
  } else {
    cat(glue("> Failed to update all {nrow_update} donor ids \n"), fill = T)
    dbRollback(conn)
  }

  #  Update the donor_did ----
  donordids = conn %>% tbl("donors") %>% pull("donor_did")
  did_update = file_update %>% filter(!is.na(current_donor_did) & previous_donor_did %in% donordids)
  nrow_update = nrow(did_update)
  dbBegin(conn)
  # Update the donor dids
  rows = dbExecute(conn, paste0("UPDATE donors SET donor_did = ? WHERE donor_did = ?"),
                   params = list(did_update[,"current_donor_did"], did_update[,"previous_donor_did"]))
  if (rows == nrow_update) {
    cat(glue("> Updated all {rows} donor dids \n"), fill = T)
    # Commit the transaction
    dbCommit(conn)
  } else {
    cat(glue("> Failed to update all {nrow_update} donor dids \n"), fill = T)
    dbRollback(conn)
  }

  #  Notify changes for the donors table ----
  notify_num = max(rows, drows)
  post_db_changes("VTL", "donors", notify_num, "updated to reflect donor id merge")

  # Append the reference table ----
  merged_ref = dbReadTable(conn, "merged_donor_ids") %>%
    mutate(ck = paste0(previous_donor_id, current_donor_id))
  merged_nrow = nrow(merged_ref)
  ref_table = file_update %>%
    mutate(ck = paste0(previous_donor_id, current_donor_id)) %>%
    filter(ck %in% merged_ref$ck == F) %>%
    select(all_of(dbListFields(conn, "merged_donor_ids")))
  dbWithTransaction(conn, {
    dbWriteTable(conn, "merged_donor_ids", ref_table, append = T)
    merged_ref = dbReadTable(conn, "merged_donor_ids")
    if (nrow(merged_ref) - merged_nrow == nrow(ref_table)) {
      cat("All records in the reference table updated", fill = T)
      # post_db_changes("VTL", "merged_donor_ids", nrow(ref_table), "updated to reflect donor id merge")
    } else {
      dbRollback()
    }
  })
}
