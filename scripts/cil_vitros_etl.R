######################################################
# Aggregation of Vitros Results, Core Immunology Lab
# Ver 1.0; 4/29/2022
######################################################
# Instructions for use:
# 1. The script requires 2 supplied variables: dilution factors (as integers) and results directory.
#    When the script is run without these arguments, the coded default values will be used
#    and so users should check the default values below and adjust prior to use
# 2. To use the script using base R, run the following:
#    source("[where the script is saved]/vitros_etl.R")
# 3. When the script is successfully loaded, run the function. The script returns
#    a data frame and so users should save the product as a variable. Example:
#    rdc_ortho = rdc_vitros_pull()
# 4. An example with the two arguments supplied:
#    rdc_ortho = rdc_vitros_pull("~/Desktop/ortho_results", c(1,10,100,1000,10000))

#' @export
rdc_vitros_pull = function(results_dir, dil_factors, n_threads = 16, key_list = NULL, igg = NULL) {
  # Dependencies ----
  require(dplyr)
  require(stringr)
  require(readxl)
  require(glue)
  require(svDialogs)
  require(striprtf)
  require(lubridate)
  library(parallel)
  library(doParallel)
  library(foreach)

  cat("\nOrtho raw result scraping initiated", fill = T)
  cat("-----------------------------------", fill = T)

  # Variables ----
  # Default results_dir
  if (missing(results_dir)) {
    cat("No directory specified, pulling from the default directory:", fill = T)
    # Set the default results directory here
    results_dir = "/Users/315852/Library/CloudStorage/OneDrive-Vitalant/Immunology Core/Vitros Results/"
  } else {
    cat("Directory specified, pulling from the specified directory:", fill = T)
  }
  cat(results_dir, fill = T)

  # Default for dilution factors
  if (missing(dil_factors)) {
    cat("\nNo dilution factors for quant IgG specified, using default set:", fill = T)
    # Set the default accepted dilution factors here
    dil_factors = c(1,20,400)
  } else {
    cat("\nDilution factors for quant IgG specified, using the following set:", fill = T)
  }
  if (length(dil_factors) == 1) {
    cat(dil_factors, fill = T)
  } else {
    cat(dil_factors, sep = ", ", fill = T)
  }

  # Switches
  if (missing(igg)) {
    igg = T
  }

  # Required fields
  required_fields = c("Sample.ID", "Date", "Time", "Assay", "Result",
                      "Result.Text", "Flag", "Dil", "Lot")
  # Acceptable entries
  result_text_vals = c("reactive","non-reactive")
  quantification_vals = c("above_loq","below_loq")

  # Data pull from OneDrive ----
  results_file_list = list.files(results_dir, include.dirs = T,
                                 pattern = "*.csv", recursive = T)
  cat(paste0("\nFound ",length(results_file_list), " files in the directory:"), fill = T)
  results = data.frame()
  for (i in 1:length(results_file_list)) {
    # Setup: Read all files, excel and csv
    if (str_detect(pattern = ".csv", string = results_file_list[i]) == T) {
      cat(sprintf("\r> Reading in file number %.0f of %.0f", i, length(results_file_list)))
      temp = read.csv(paste(results_dir, results_file_list[i], sep = "/"))
    }
    # Field check. Extract pre-defined required fields
    if (F %in% (required_fields %in% names(temp))) {
      stop(errorCondition("Incorrect/missing required fields. Aborting process..."))
    } else {
      temp = temp %>%
        select(all_of(required_fields))
      results = rbind(results, temp)
    }
  }

  # Data QC of results ----
  #  Setup
  cat("\n\nResult scraping complete. Transforming..", fill = T)
  bar = txtProgressBar(min = 0, max = 15, initial = 0, style = 2, char = "|")

  #  Make notation and nchar uniform
  results$Result.Text = tolower(trimws(results$Result.Text))
  results$Sample.ID = substr(trimws(results$Sample.ID),1,13)
  results$Sample.ID = toupper(results$Sample.ID)
  setTxtProgressBar(bar, 1)

  #  Needed for sorting most recently tested
  results$Date = as.Date(results$Date, format("%m/%d/%Y"))
  results$datetime = as_datetime(paste(results$Date, results$Time))
  results$datetime = as.numeric(results$datetime)
  setTxtProgressBar(bar, 2)

  #  Selecting for the required fields
  results = results %>% select(all_of(required_fields), datetime)
  setTxtProgressBar(bar, 3)

  #  Pulling Ortho Total S ----
  if (is.null(key_list)) {
    results_s = results %>%
      filter(Assay == "CoV2T") %>%
      select(din = Sample.ID,
             datetime,
             cv2ts_test_date = Date,
             vitros_s_total_ig_sco = Result,
             vitros_s_total_ig_interpretation = Result.Text)
    setTxtProgressBar(bar, 4)
  } else {
    results_s = results %>%
      filter(Assay == "CoV2T", Sample.ID %in% key_list) %>%
      select(din = Sample.ID,
             datetime,
             cv2ts_test_date = Date,
             vitros_s_total_ig_sco = Result,
             vitros_s_total_ig_interpretation = Result.Text)
    setTxtProgressBar(bar, 4)
  }

  #  Remove duplicates, choosing the most recently tested
  all_results = results_s %>%
    filter(vitros_s_total_ig_interpretation %in% result_text_vals) %>%
    group_by(din) %>%
    slice(which.max(datetime)) %>%
    select(-datetime)
  setTxtProgressBar(bar, 5)

  #  Pulling Ortho IgG ----
  if (igg == T) {
    if (is.null(key_list)) {
      results_igg = results %>%
        filter(Assay == "CVGQN") %>%
        select(din = Sample.ID,
               dtime = datetime,
               vitros_quant_s_igg_test_date = Date,
               vitros_quant_s_igg_quantification = Flag,
               vitros_quant_s_igg_bauml = Result,
               vitros_quant_s_igg_interpretation = Result.Text,
               vitros_quant_s_igg_dilution_factor = Dil,
               vitros_quant_s_igg_reagent_lot = Lot)
      setTxtProgressBar(bar, 6)
    } else {
      results_igg = results %>%
        filter(Assay == "CVGQN", Sample.ID %in% key_list) %>%
        select(din = Sample.ID,
               dtime = datetime,
               vitros_quant_s_igg_test_date = Date,
               vitros_quant_s_igg_quantification = Flag,
               vitros_quant_s_igg_bauml = Result,
               vitros_quant_s_igg_interpretation = Result.Text,
               vitros_quant_s_igg_dilution_factor = Dil,
               vitros_quant_s_igg_reagent_lot = Lot)
      setTxtProgressBar(bar, 6)
    }

    # Transforming IgG values
    # Must have a valid dilution factor. NA as a dilution factor = neat (1)
    results_igg$vitros_quant_s_igg_dilution_factor[is.na(results_igg$vitros_quant_s_igg_dilution_factor)] = 1
    # Subseting for valid dilution factors
    results_igg = subset(results_igg, results_igg$vitros_quant_s_igg_dilution_factor %in% dil_factors)
    class(results_igg$vitros_quant_s_igg_bauml) = "numeric"
    setTxtProgressBar(bar, 7)
    results_igg$bauml_problematic = F
    problematic = results_igg %>%
      mutate(bauml_problematic = case_when(
        results_igg$vitros_quant_s_igg_bauml < results_igg$vitros_quant_s_igg_dilution_factor*2 ~ T,
        results_igg$vitros_quant_s_igg_bauml > results_igg$vitros_quant_s_igg_dilution_factor*200 ~ T)
      ) %>%
      filter(bauml_problematic == T)
    # Build directory into arguments for the function --
    results_igg$rownum = row.names(results_igg)
    if(nrow(problematic) != 0) {
      write.csv(problematic, paste0("~/Desktop/","problematic_igg_",format(Sys.Date(), "%Y%m%d"),".csv"), row.names = F)
      results_igg = results_igg %>%
        filter(rownum %in% problematic$rownum == F) %>%
        select(-rownum)
    }
    setTxtProgressBar(bar, 8)

    # Transforming flag and interpretation depending on certain conditions
    results_igg$vitros_quant_s_igg_quantification[is.na(results_igg$vitros_quant_s_igg_quantification)] = "within_loq"
    results_igg$vitros_quant_s_igg_quantification[results_igg$vitros_quant_s_igg_quantification == ">"] = "above_loq"
    results_igg$vitros_quant_s_igg_bauml[results_igg$vitros_quant_s_igg_quantification == "above_loq"] = results_igg$vitros_quant_s_igg_dilution_factor[results_igg$vitros_quant_s_igg_quantification == "above_loq"] * 200
    results_igg$vitros_quant_s_igg_interpretation[results_igg$vitros_quant_s_igg_quantification == "above_loq"] = "reactive"
    results_igg$vitros_quant_s_igg_quantification[results_igg$vitros_quant_s_igg_quantification == "<"] = "below_loq"
    results_igg$vitros_quant_s_igg_bauml[results_igg$vitros_quant_s_igg_quantification == "below_loq"] = results_igg$vitros_quant_s_igg_dilution_factor[results_igg$vitros_quant_s_igg_quantification == "below_loq"] * 2
    results_igg$vitros_quant_s_igg_interpretation[results_igg$vitros_quant_s_igg_quantification == "below_loq" & results_igg$vitros_quant_s_igg_dilution_factor == 1] = "non-reactive"
    results_igg$vitros_quant_s_igg_interpretation[results_igg$vitros_quant_s_igg_quantification == "below_loq" & results_igg$vitros_quant_s_igg_dilution_factor != 1] = "inconclusive"
    results_igg$vitros_quant_s_igg_quantification[results_igg$vitros_quant_s_igg_quantification %in% quantification_vals == F] = "within_loq"
    setTxtProgressBar(bar, 9)

    # Remove invalid entries and also duplicates, choosing the most recently tested
    results_igg = unique(results_igg)
    results_igg = results_igg %>%
      filter(vitros_quant_s_igg_interpretation %in% c(result_text_vals, "inconclusive") &
               vitros_quant_s_igg_dilution_factor %in% dil_factors)
    results_igg$rownum = row.names(results_igg)
    dins = unique(results_igg$din)
    n_threads = 15

    # The algorithm starts here
    {
      cluster = makeCluster(n_threads)
      registerDoParallel(cluster)

      selected_rows <- foreach(
        i = 1:length(dins),
        .combine = bind_rows,
        .packages = "tidyverse"
      ) %dopar% {
        # subset results for that din, also randomly select 1 result per dilution factor ----
        temp = results_igg %>%
          filter(din == dins[i]) %>%
          group_by(vitros_quant_s_igg_dilution_factor) %>%
          slice_sample(n = 1) %>%
          ungroup()

        # Run the algorithm; check for if the 20 dilution is available ----

        if (20 %in% temp$vitros_quant_s_igg_dilution_factor) {
          if (temp$vitros_quant_s_igg_quantification[temp$vitros_quant_s_igg_dilution_factor == 20] == "within_loq") {
            #cat("hit 1:20 is quantified block", fill = T)
            result = tibble(
              rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 20],
              final = T,
              flag = NA
            )
          } else if (temp$vitros_quant_s_igg_bauml[temp$vitros_quant_s_igg_dilution_factor == 20] == 40 &
                     temp$vitros_quant_s_igg_quantification[temp$vitros_quant_s_igg_dilution_factor == 20] == "below_loq") {
            # Check for 1:1 dilution
            if (1 %in% temp$vitros_quant_s_igg_dilution_factor) {
              #cat("hit found 1:1  block", fill = T)
              result = tibble(
                rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 1],
                final = T,
                flag = NA
              )
            } else {
              #cat("hit 1:1 missing block", fill = T)
              result = tibble(
                rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 20],
                final = T,
                flag = "1:1 missing"
              )
            }
          } else if (temp$vitros_quant_s_igg_bauml[temp$vitros_quant_s_igg_dilution_factor == 20] == 4000 &
                     temp$vitros_quant_s_igg_quantification[temp$vitros_quant_s_igg_dilution_factor == 20] == "above_loq") {
            if (400 %in% temp$vitros_quant_s_igg_dilution_factor) {
              #cat("hit found 1:400 block", fill = T)
              # Use valid 1:400 result. Invalid results if none is available ----
              result = tibble(
                rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 400],
                final = T,
                flag = NA
              )
            } else {
              #cat("hit 1:400 missing block", fill = T)
              result = tibble(
                rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 20],
                final = T,
                flag = "1:400 missing"
              )
            }
          } else {
            stop("the world has ended~")
          }
        } else {
          # flag = "1:20 missing"
          if (nrow(temp) == 1) {
            result = tibble(
              rownum = temp$rownum,
              final = T,
              flag = "1:20 missing"
            )
          } else {
            # Choose
            result = tibble(
              rownum = temp$rownum[temp$vitros_quant_s_igg_dilution_factor == 1],
              final = F,
              flag = "1:20 missing, both 1:1 & 1:400 results present"
            )
          }
        }
        return(result)
      }
      stopCluster(cluster)
    }

    #  Report any samples without a final result, if any
    results_igg = left_join(results_igg, selected_rows, by = "rownum")
    nofinal = results_igg %>%
      filter(is.na(flag) == F)
    if(nrow(nofinal) != 0) {
      tdate = format(Sys.Date(), "%Y%m%d")
      write.csv(nofinal, glue("~/Desktop/ortho_results_report_{tdate}.csv"),
                row.names = F)
    }

    #  Final selection of samples
    results_igg = results_igg %>%
      filter(final == T) %>%
      select(-final, -bauml_problematic, -dtime, -rownum, - flag)
    setTxtProgressBar(bar, 10)

    #  Generate invalid results
    temp_igg = tibble(
      din = subset(key_list, key_list %in% results_igg$din == F),
      vitros_quant_s_igg_test_date = as.Date("2222-01-01"),
      vitros_quant_s_igg_quantification = "not_tested",
      vitros_quant_s_igg_bauml = 9999.88,
      vitros_quant_s_igg_interpretation = "not_tested",
      vitros_quant_s_igg_dilution_factor = 998,
      vitros_quant_s_igg_reagent_lot = 9998
    )
    results_igg = rbind(results_igg, temp_igg)

    #  Joining both S Total and IgG
    all_results = full_join(all_results, results_igg, by = "din")
    setTxtProgressBar(bar, 11)
  }

  #  Pulling Ortho NC ----
  if (is.null(key_list)) {
    results_nc = results %>%
      filter(Assay == "CV2TN") %>%
      select(din = Sample.ID,
             datetime,
             nc_total_ig_test_date = Date,
             nc_sco = Result,
             nc_interpretation = Result.Text,
             nc_total_ig_reagent_lot = Lot)
    setTxtProgressBar(bar, 12)
  } else {
    results_nc = results %>%
      filter(Assay == "CV2TN", Sample.ID %in% key_list) %>%
      select(din = Sample.ID,
             datetime,
             nc_total_ig_test_date = Date,
             nc_sco = Result,
             nc_interpretation = Result.Text,
             nc_total_ig_reagent_lot = Lot)
    setTxtProgressBar(bar, 12)
  }

  # Remove duplicates, choosing the most recently tested
  # QC for assay to doublecheck, drop unneeded columns after
  results_nc = results_nc %>%
    filter(nc_interpretation %in% result_text_vals) %>%
    group_by(din) %>%
    slice(which.max(datetime)) %>%
    select(-datetime)
  setTxtProgressBar(bar, 13)

  #  Joining NC to all results
  temp_nc = tibble(
    din = subset(key_list, key_list %in% results_nc$din == F),
    nc_total_ig_test_date = as.Date("2222-01-01"),
    nc_sco = "9999.88",
    nc_interpretation = "not_tested",
    nc_total_ig_reagent_lot = 9998
  )
  results_nc = rbind(results_nc, temp_nc)
  all_results = full_join(all_results, results_nc, by = "din")
  setTxtProgressBar(bar, 14)
  close(bar)

  # Export
  cat("\nOrtho results data pull complete \n", fill = T)
  return(all_results)
}
