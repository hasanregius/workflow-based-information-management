################################
# ARC MSSQL Result Accessioning
# v1.4; 03/13/2024
################################

#' @export
arc_result_accession = function(report) {
  # Packages and Dependencies ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)

  # Scripts
  source("scripts/helper_scripts/db_notification.R")
  source("scripts/helper_scripts/db_connect.R")
  lc_conn = connect_db()

  # If connection can be made, proceed ----
  if (is.null(lc_conn) == F) {
    # Pull a list of the fieldnames
    fnames_cv2gs = dbListFields(lc_conn, "cv2gs", schema = "arc")
    fnames_cv2tn = dbListFields(lc_conn, "cv2tn", schema = "arc")

    # Ortho IgG Quantitative ----
    if (all(fnames_cv2gs[1:7] %in% names(report))) {
      cat("Accessioning IgG S Results~", fill = T)
      report$final_result = 1
      # Pulling the required fields
      cv2gs = report %>% select(all_of(fnames_cv2gs))
      # Check if all cv2gs results have been accessioned before
      cv2gs_list = dbReadTable(lc_conn, Id(schema = "arc", table = "cv2gs")) %>%
        mutate(final_result = case_when(
          final_result == TRUE ~ 1,
          final_result == FALSE ~ 0),
          ck = paste0(din,"-",final_result)) %>%
        distinct()
      cv2gs_list = cv2gs_list$ck
      cv2gs_length = length(cv2gs_list)
      # Prepare the data ----
      cv2gs = na.exclude(cv2gs) %>% distinct()
      if (class(cv2gs$vitros_quant_s_igg_test_date) != "Date") {
        cv2gs$vitros_quant_s_igg_test_date = as.Date(cv2gs$vitros_quant_s_igg_test_date, format = "%Y-%m-%d")
      }
      cv2gs = cv2gs %>%
        filter(vitros_quant_s_igg_interpretation %in% c('reactive','non-reactive','qns','inconclusive','invalid')) %>%
        mutate(ck = paste0(din,"-",final_result)) %>%
        distinct()

      # Accessioning sample data with transaction
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (cv2gs$ck %in% cv2gs_list)) {
          cat("Duplicate IgG results found, subsetting for unique DINs", fill = T)
          cv2gs = cv2gs %>%
            filter(ck %in% cv2gs_list == F) %>%
            select(-ck)
          dbWriteTable(lc_conn, Id(schema = "arc", table = "cv2gs"), cv2gs, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          cv2gs = select(cv2gs, -"ck")
          dbWriteTable(lc_conn, Id(schema = "arc", table = "cv2gs"), cv2gs, append = TRUE)
        }
        # Check for if accessioning was successful
        cv2gs_list = dbReadTable(lc_conn, Id(schema = "arc", table = "cv2gs")) %>%
          mutate(final_result = case_when(
            final_result == TRUE ~ 1,
            final_result == FALSE ~ 0),
            ck = paste0(din,"-",final_result))
        # Rollback in case of error
        delta = (cv2gs_length + nrow(cv2gs)) - nrow(cv2gs_list)
        if (delta != 0) {
          if (delta < 0) {
            delta = delta - delta*2
          }
          cat(paste0(delta," DINs failed to be imported for Ortho IgG. Aborting..\n"), fill = T)
          dbBreak()
        } else {
          cat("All ARC Ortho IgG data was successfully imported", fill = T)
          if (nrow(cv2gs) > 0) {
            cat(paste0(nrow(cv2gs)," results were accessioned for Ortho IgG\n"))
          }
        }
      })
    }

    # Ortho NC ----
    names(report)[names(report) == "nc_sco"] = "nc_quant"
    if ("nc_final_result" %in% names(report) == F) {
      report$nc_final_result = T
    }
    if ("nc_total_ig_test" %in% names(report) == F) {
      report$nc_total_ig_test = "ortho"
    }
    if ("nc_study_source" %in% names(report) == F) {
      report$nc_study_source = "rdc"
    }
    if (all(fnames_cv2tn %in% names(report))) {
      cat("\nAccessioning Ortho Total IG N results~", fill = T)
      # Pulling the required fields
      cv2tn = report %>%
        filter(nc_interpretation %in% c('reactive','non-reactive','qns')) %>%
        select(all_of(fnames_cv2tn)) %>%
        group_by(din, nc_dilution_factor) %>%
        mutate(ck = paste0(din, nc_dilution_factor, nc_study_source)) %>%
        ungroup() %>%
        distinct()

      # Prepare the data ----
      cv2tn = na.exclude(cv2tn)
      if (class(cv2tn$nc_total_ig_test_date) != "Date") {
        cv2tn$nc_total_ig_test_date = as.Date(cv2tn$nc_total_ig_test_date, format = "%Y-%m-%d")
      }

      # Pull cv2tn results have been accessioned before
      cv2tn_db = lc_conn %>%
        tbl(in_schema("arc","cv2tn")) %>%
        collect() %>%
        mutate(ck = paste0(din, nc_dilution_factor, nc_study_source))
      cv2tn_list = unique(cv2tn_db$ck)
      cv2tn_length = length(cv2tn_list)

      # Accessioning NC data with transaction ----
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (cv2tn$ck %in% cv2tn_list)) {
          cat("\nDuplicate NC results found, subsetting for unique DINs", fill = T)
          cv2tn = cv2tn %>%
            filter(ck %in% cv2tn_list == F) %>%
            select(-ck)
          # Designation of final result based on dilutions
          cv2tn$nc_final_result = F
          dins = unique(cv2tn$din[cv2tn$nc_dilution_factor == 20])
          cv2tn$nc_final_result[cv2tn$din %in% dins == F] = T
          if (length(dins > 0)) {
            for (i in 1:length(dins)) {
              nc_dilfactors = cv2tn$nc_dilution_factor[cv2tn$din == dins[i]]
              cv2tn$nc_final_result[cv2tn$din == dins[i] & cv2tn$nc_dilution_factor == max(nc_dilfactors)] = T
              cv2tn = cv2tn %>%
                mutate(ck = paste0(din, nc_dilution_factor, nc_study_source),
                       nc_total_ig_test_date = na_if(nc_total_ig_test_date, as.Date("2222-01-01")),
                       nc_total_ig_reagent_lot = na_if(nc_total_ig_reagent_lot, 9998)) %>%
                group_by(ck) %>%
                slice(which.max(nc_total_ig_test_date)) %>%
                ungroup() %>%
                distinct()
            }
          }
          dbWriteTable(lc_conn, Id(schema = "arc", table = "cv2tn"), cv2tn, append = TRUE)
        } else {
          cat("\nNo duplicate records found. Attempting to import all results", fill = T)
          cv2tn %>% select(-ck) -> cv2tn
          # Designation of final result based on dilutions
          dins = unique(cv2tn$din)
          cv2tn$nc_final_result = F
          for (i in 1:length(dins)) {
            nc_dilfactors = cv2tn$nc_dilution_factor[cv2tn$din == dins[i]]
            cv2tn$nc_final_result[cv2tn$din == dins[i] & cv2tn$nc_dilution_factor == max(nc_dilfactors)] = T
          }
          cv2tn = cv2tn %>%
            mutate(ck = paste0(din, nc_dilution_factor, nc_study_source),
                   nc_total_ig_test_date = na_if(nc_total_ig_test_date, as.Date("2222-01-01")),
                   nc_total_ig_reagent_lot = na_if(nc_total_ig_reagent_lot, 9998)) %>%
            group_by(ck) %>%
            slice(which.max(nc_total_ig_test_date)) %>%
            ungroup() %>%
            distinct()
          cv2tn = cv2tn %>% select(-ck)
          dbWriteTable(lc_conn, Id(schema = "arc", table = "cv2tn"), cv2tn, append = TRUE)
        }
        # Check for if accessioning was successful
        cv2tn_db = lc_conn %>%
          tbl(in_schema("arc","cv2tn")) %>%
          collect() %>%
          mutate(ck = paste0(din, nc_dilution_factor, nc_study_source))
        cv2tn_list = unique(cv2tn_db$ck)

        # Rollback in case of error
        delta = length(cv2tn_list) - (cv2tn_length + nrow(cv2tn))
        if (delta != 0) {
          if (delta < 0) {
            delta = delta - delta*2
          }
          cat(paste0(dsc," DINs failed to be imported for Ortho NC. Aborting..\n"), fill = T)
          dbBreak()
        } else {
          if (nrow(cv2tn) > 0) {
          cat(paste0(nrow(cv2tn)," results were accessioned for Ortho NC\n"))
          } else {
            cat("AC Ortho NC Import complete. No new result.", fill = T)
         }
        }
      })
    }
  } else {
    stop("Can't connect to the Db. Check your connection or contact IT \n")
  }
}
