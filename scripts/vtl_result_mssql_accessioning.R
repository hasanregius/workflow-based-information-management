################################
# VTL MSSQL Result Accessioning
# v2.1; 3/13/2024
################################

#' @export
vtl_result_accession = function(report, cts_results) {
  # Packages and Dependencies ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)

  source("scripts/helper_scripts/db_connect.R")
  source("scripts/helper_scripts/db_notification.R")
  lc_conn = connect_db()

  # If connection can be made, proceed ----
  if (!is.null(lc_conn)) {
    # Pull all fieldnames of the results tables ----
    fnames_samples = dbListFields(lc_conn, "samples")
    fnames_cv2gs = dbListFields(lc_conn, "cv2gs")
    fnames_nc = dbListFields(lc_conn, "nc_total_ig_results")

    # Sample information ----
    cts_metadata = cts_results %>%
      filter(din %in% report$din) %>%
      select(din, sample_status, aliquot_type, aliquot_anticoagulant) %>%
      distinct()
    # Accession if all fieldnames are present
    if (all(fnames_samples %in% names(cts_metadata))) {
      # Pulling the required fields
      samples = cts_metadata %>% select(all_of(fnames_samples))
      # Check if all samples results have been accessioned before
      samples_list = lc_conn %>% tbl("samples") %>% pull("din")
      samples_length = length(samples_list)

      # Accessioning sample data with transaction
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (samples$din %in% samples_list)) {
          cat("Duplicate sample records found, subsetting for unique DINs", fill = T)
          samples = subset(samples, samples$din %in% samples_list == F)
          dbWriteTable(lc_conn, "samples", samples, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          dbWriteTable(lc_conn, "samples", samples, append = TRUE)
        }
        # Check for if accessioning was successful
        samples_list = lc_conn %>% tbl("samples") %>% pull("din")
        # Rollback in case of error
        if (length(samples_list) - samples_length != nrow(samples)) {
          dsc = length(samples_list) - (samples_length + nrow(samples))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," DINs failed to be imported for Ortho IgG. Aborting.\n"), fill = T)
          dbBreak()
        } else {
          cat("All VTL samples data was successfully imported. \n", fill = T)
          if (nrow(samples) > 0) {
            # post_db_changes(bco = "VTL", table = "samples", action = "created",
            #                changes_nrow = nrow(samples))
          }
        }
      })
    }

    # Ortho IgG Quantitative ----
    # Check for if final_result is included as a field
    if ("final_result" %in% names(report) == F) {
      report$final_result = T
    }
    # Check for if cv2gs_study_source is included as a field
    if ("cv2gs_study_source" %in% names(report) == F) {
      report$cv2gs_study_source = 'rdc'
    }
    if (all(fnames_cv2gs %in% names(report))) {
      # Check for invalids and QNS ----
      if ("invalid" %in% unique(report$vitros_quant_s_igg_interpretation)) {
        invalids_igg = report %>%
          filter(vitros_quant_s_igg_interpretation %in% c("qns","invalid"))
        if (9998 %in% unique(invalids_igg$vitros_quant_s_igg_reagent_lot)) {
          stop("stupid invalid bs problem")
        }
      }
      # Pulling the required fields ----
      cv2gs = report %>% select(all_of(fnames_cv2gs)) %>%
        filter(vitros_quant_s_igg_interpretation %in% c('reactive','non-reactive','qns','invalid')) %>%
        mutate(ck = case_when(
          final_result == TRUE ~ paste0(din, "-", 1, "-", cv2gs_study_source),
          final_result == FALSE ~ paste0(din, "-", 0, "-", cv2gs_study_source)
        )) %>%
        distinct()
      # Check if all cv2gs results have been accessioned before
      cv2gs_list = dbReadTable(lc_conn, "cv2gs") %>%
        mutate(ck = case_when(
          final_result == TRUE ~ paste0(din, "-", 1, "-", cv2gs_study_source),
          final_result == FALSE ~ paste0(din, "-", 0, "-", cv2gs_study_source)
        ))
      cv2gs_list = cv2gs_list$ck
      cv2gs_length = length(cv2gs_list)
      # Prepare the data
      if (class(cv2gs$vitros_quant_s_igg_test_date) != "Date") {
        cv2gs$vitros_quant_s_igg_test_date = as.Date(cv2gs$vitros_quant_s_igg_test_date, format = "%Y-%m-%d")
      }

      # Accessioning sample data with transaction ----
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (cv2gs$ck %in% cv2gs_list)) {
          cat("Duplicate IgG results found, subsetting for unique DINs", fill = T)
          cv2gs = subset(cv2gs, cv2gs$ck %in% cv2gs_list == F)
          cv2gs = select(cv2gs, -"ck")
          dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          cv2gs = select(cv2gs, -"ck")
          dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
        }
        # Check for if accessioning was successful
        cv2gs_list = dbReadTable(lc_conn, "cv2gs") %>%
          mutate(ck = case_when(
            final_result == TRUE ~ paste0(din, "-", 1, "-", cv2gs_study_source),
            final_result == FALSE ~ paste0(din, "-", 0, "-", cv2gs_study_source)
          ))
        cv2gs_list = cv2gs_list$ck
        # Rollback in case of error
        if (length(cv2gs_list) - cv2gs_length != nrow(cv2gs)) {
          dsc = length(cv2gs_list) - (cv2gs_length + nrow(cv2gs))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," records failed to be imported for Ortho IgG. Aborting..\n"), fill = T)
          dbBreak()
        } else {
          cat("All VTL Ortho IgG data was successfully imported", fill = T)
          if (nrow(cv2gs) > 0) {
          # post_db_changes(bco = "VTL", table = "cv2gs (Ortho IgG Quantitative)",
          #                 changes_nrow = nrow(cv2gs), action = "created")
          }
        }
      })
    }

    # Total Ig NC ----
    #  Checking for if the nc extra fields are present
    if ("nc_total_ig_test" %in% names(report) == F) {
      if ("nc_test_name" %in% names(report)) {
        names(report)[names(report) == "nc_test_name"] = "nc_total_ig_test"
      } else {
        report$nc_total_ig_test = "ortho"
      }
    }
    if ("nc_dilution_factor" %in% names(report) == F) {
      report$nc_dilution_factor = 1
    }
    if ("nc_final_result" %in% names(report) == F) {
      report$nc_final_result = T
    }
    if ("nc_study_source" %in% names(report) == F) {
      report$nc_study_source = 'rdc'
    }

    #  If all other fields are present, move forward
    if (all(fnames_nc %in% names(report))) {
      # Pulling the required fields and marking the assay name appropriately
      nc_results = report %>%
        select(all_of(fnames_nc)) %>%
        mutate(
          nc_interpretation = tolower(nc_interpretation),
          nc_total_ig_test = case_when(
            grepl("ortho", nc_total_ig_test) == T ~ "ortho",
            grepl("roche", nc_total_ig_test) == T ~ "roche")
          ) %>%
        distinct()

      #  Keeping testing date as a date format
      if (class(nc_results$nc_total_ig_test_date) != "Date") {
        nc_results$nc_total_ig_test_date = as.Date(nc_results$nc_total_ig_test_date, format = "%Y-%m-%d")
      }

      # Prepare the data table ----
      nc_results = nc_results %>%
        filter(nc_interpretation %in% c('reactive','non-reactive','qns','invalid')) %>%
        distinct()
      # Designation of final result based on dilutions
      dins = unique(nc_results$din)
      nc_results$nc_final_result = F
      for (i in 1:length(dins)) {
        nc_dilfactors = nc_results$nc_dilution_factor[nc_results$din == dins[i]]
        nc_results$nc_final_result[nc_results$din == dins[i] & nc_results$nc_dilution_factor == max(nc_dilfactors)] = T
      }
      nc_results = nc_results %>%
        mutate(ck = paste0(din, nc_total_ig_test, nc_dilution_factor, nc_study_source),
               nc_total_ig_test_date = na_if(nc_total_ig_test_date, as.Date("2222-01-01")),
               nc_total_ig_reagent_lot = na_if(nc_total_ig_reagent_lot, 9998)) %>%
        group_by(ck) %>%
        slice(which.max(nc_total_ig_test_date)) %>%
        ungroup() %>%
        distinct()

      # Pull a list of already-accessioned data
      nc_results_db = lc_conn %>%
        tbl("nc_total_ig_results") %>%
        collect() %>%
        mutate(
          ck = paste0(din, nc_total_ig_test, nc_dilution_factor, nc_study_source)
        )
      nc_results_list = nc_results_db$ck
      nc_results_length = length(nc_results_list)

      # Accessioning NC data with transaction ----
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (nc_results$ck %in% nc_results_list)) {
          cat("Duplicate NC results found, subsetting for unique DINs", fill = T)
          nc_results = nc_results %>% filter(ck %in% nc_results_list == F) %>% select(!ck)
          dbWriteTable(lc_conn, "nc_total_ig_results", nc_results, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          nc_results = nc_results %>% select(!ck)
          dbWriteTable(lc_conn, "nc_total_ig_results", nc_results, append = TRUE)
        }
        # Check for if accessioning was successful
        # Pull a list of already-accessioned data
        nc_results_db = lc_conn %>%
          tbl("nc_total_ig_results") %>%
          collect() %>%
          mutate(
            ck = paste0(din, nc_total_ig_test, nc_dilution_factor, nc_study_source)
          )
        nc_results_list = nc_results_db$ck
        # Rollback in case of error
        if (length(nc_results_list) - nc_results_length != nrow(nc_results)) {
          dsc = length(nc_results_list) - (nc_results_length + nrow(nc_results))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," DINs failed to be imported for NC results. Aborting..\n"), fill = T)
          dbBreak()
        } else {
          cat("All VTL NC results were successfully imported", fill = T)
          if (nrow(nc_results) != 0) {
            # post_db_changes(bco = "VTL", table = "nc_total_ig_results (Ortho & Roche NC Results)",
            #                changes_nrow = nrow(nc_results), action = "created")
          }
        }
      })
    }
  } else {
    stop("Can't connect to the Db. Check your connection or contact IT. \n")
  } # Connection invalid
}
