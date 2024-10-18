################################
# ARC MSSQL Samples Accessioning
# v1.1; 06/22/2022
################################

vtl_result_accession = function(report) {
  # Packages and Dependencies ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)

  source("./data_management/helper_scripts/db_connect.R")
  lc_conn = connect_db()

  # If connection can be made, proceed ----
  if (is.null(lc_conn) == F) {
    # Pull a list of the fieldnames
    fnames_cv2gs = dbListFields(lc_conn, "cv2gs")
    fnames_cv2tn = dbListFields(lc_conn, "cv2tn")

    # Ortho IgG Quantitative ----
    if (F %in% (fnames_cv2gs %in% names(report)) == F) {
      # Pulling the required fields
      cv2gs = report %>% select(all_of(fnames_cv2gs))
        # Check if all cv2gs results have been accessioned before
        cv2gs_list = lc_conn %>% tbl("cv2gs") %>% pull("din")
        cv2gs_length = length(cv2gs_list)
        # Prepare the data ----
        cv2gs = na.exclude(cv2gs)
        if (class(cv2gs$vitros_quant_s_igg_test_date) != "Date") {
          cv2gs$vitros_quant_s_igg_test_date = as.Date(cv2gs$vitros_quant_s_igg_test_date, format = "%Y-%m-%d")
        }
        cv2gs = cv2gs %>%
          filter(vitros_quant_s_igg_interpretation %in% c('reactive','non-reactive','qns')) %>%
          group_by(din) %>%
          slice(which.max(vitros_quant_s_igg_test_date))

        # Accessioning sample data with transaction
        dbWithTransaction(lc_conn, {
          # If some samples are already in the Db, we omit those samples
          if (TRUE %in% (cv2gs$din %in% cv2gs_list)) {
            cat("Duplicate IgG results found, subsetting for unique DINs", fill = T)
            cv2gs = subset(cv2gs, cv2gs$din %in% cv2gs_list == F)
            dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
          } else {
            cat("No duplicate records found. Attempting to import all results", fill = T)
            dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
          }
          # Check for if accessioning was successful
          cv2gs_list = lc_conn %>%
            tbl("cv2gs") %>%
            pull("din")
          # Rollback in case of error
          if (length(cv2gs_list) - cv2gs_length != nrow(cv2gs)) {
            dsc = length(cv2gs_list) - (cv2gs_length + nrow(cv2gs))
            if (dsc < 0) {
              dsc = dsc - dsc*2
            }
            cat(paste0(dsc," DINs failed to be imported for Ortho IgG. Aborting.\n"), fill = T)
            dbBreak()
          } else {
            cat("All ARC Ortho IgG data was successfully imported. \n", fill = T)
          }
        })
    }

    # Ortho NC ----
    if (F %in% (fnames_cv2gs %in% names(report)) == F) {
      # Pulling the required fields
      cv2tn = report %>% select(all_of(fnames_cv2tn))
      # Check if all cv2tn results have been accessioned before
      cv2tn_list = lc_conn %>% tbl("cv2tn") %>% pull("din")
      cv2tn_length = length(cv2tn_list)
      # Prepare the data ----
      cv2tn = na.exclude(cv2tn)
      if (class(cv2tn$nc_total_ig_test_date) != "Date") {
        cv2tn$nc_total_ig_test_date = as.Date(cv2tn$nc_total_ig_test_date, format = "%Y-%m-%d")
      }
      cv2tn = cv2tn %>%
        filter(nc_interpretation %in% c('reactive','non-reactive','qns')) %>%
        group_by(din) %>%
        slice(which.max(nc_total_ig_test_date))

      # Accessioning sample data with transaction
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (cv2tn$din %in% cv2tn_list)) {
          cat("Duplicate IgG results found, subsetting for unique DINs", fill = T)
          cv2tn = subset(cv2tn, cv2tn$din %in% cv2tn_list == F)
          dbWriteTable(lc_conn, "cv2tn", cv2tn, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          dbWriteTable(lc_conn, "cv2tn", cv2tn, append = TRUE)
        }
        # Check for if accessioning was successful
        cv2tn_list = lc_conn %>%
          tbl("cv2tn") %>%
          pull("din")
        # Rollback in case of error
        if (length(cv2tn_list) - cv2tn_length != nrow(cv2tn)) {
          dsc = length(cv2tn_list) - (cv2tn_length + nrow(cv2tn))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," DINs failed to be imported for Ortho IgG. Aborting.\n"), fill = T)
          dbBreak()
        } else {
          cat("All ARC Ortho IgG data was successfully imported. \n", fill = T)
        }
      })
    }

  } else {
    stop("Can't connect to the Db. Check your connection or contact IT. \n")
  } # Connection invalid
}

vtl_samples_accession = function(report) {
  # Packages and Dependencies ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)

  source("./data_management/helper_scripts/db_connect.R")
  lc_conn = connect_db()

  # If connection can be made, proceed ----
  if (is.null(lc_conn) == F) {
    # Pull a list of the fieldnames
    fnames_samples = dbListFields(lc_conn, "samples")
    # Pull sample list
    vtl_sample_list = lc_conn %>% tbl("samples") %>% pull("din")
    vtl_samples_length = length(vtl_sample_list)
    if (all(fnames_samples %in% names(report))) {
      # Pulling the required fields
      samples = report %>% select(all_of(fnames_samples))
      if (F %in% (report$din %in% vtl_sample_list) == F) {
        # Accessioning sample data with transaction
        dbWithTransaction(lc_conn, {
          # If some samples are already in the Db, we omit those samples
          if (TRUE %in% (samples$din %in% vtl_sample_list)) {
            cat("Duplicate IgG results found, subsetting for unique DINs", fill = T)
            samples = subset(samples, samples$din %in% vtl_sample_list == F)
            dbWriteTable(lc_conn, "samples", samples, append = TRUE)
          } else {
            cat("No duplicate records found. Attempting to import all results", fill = T)
            dbWriteTable(lc_conn, "samples", samples, append = TRUE)
          }
          # Check for if accessioning was successful
          vtl_sample_list = lc_conn %>%
            tbl("samples") %>%
            pull("din")
          # Rollback in case of error
          if (length(vtl_sample_list) - vtl_samples_length != nrow(samples)) {
            dsc = length(vtl_sample_list) - (vtl_samples_length + nrow(samples))
            if (dsc < 0) {
              dsc = dsc - dsc*2
            }
            cat(paste0(dsc," DINs failed to be imported for Ortho IgG. Aborting.\n"), fill = T)
            dbBreak()
          } else {
            cat("All ARC Ortho IgG data was successfully imported. \n", fill = T)
          }
        })
      } else {
        cat("There are sample(s) in the file that have not been accessioned \n")
      }
    }
    #  Notify if fields are missing
    if (F %in% (fnames_samples %in% names(report))) {
      cat("Incomplete fields for Ortho IgG accessioning. \n", fill = T)
    }
  } else {
    stop("Can't connect to the Db. Check your connection or contact IT. \n")
  } # Connection invalid
}
