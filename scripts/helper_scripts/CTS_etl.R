###################
# CTS Results ETL
# v1.2; 3/13/2024
###################

#' @export
pull_cts_results = function(nc_dilutions, study_source, recursive) {
  # Dependencies ----
  require(dplyr)
  require(svDialogs)
  require(tidyr)

  # Static variables
  cts_resdir = "/Users/315852/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/CTS Results Files/"
  if (recursive == T) {
    cts_reslist = list.files(cts_resdir, pattern = "*.csv", recursive = T)
  } else {
    cts_reslist = list.files(cts_resdir, pattern = "*.csv")
  }
  cts_fnames_base = c("din","cohort","bco","collection_date","sample_status","aliquot_type",
                      "aliquot_anticoagulant","vitros_quant_s_igg_bauml",
                      "vitros_quant_s_igg_quantification",
                      "vitros_quant_s_igg_dilution_factor",
                      "vitros_quant_s_igg_interpretation",
                      "vitros_quant_s_igg_reagent_lot",
                      "vitros_quant_s_igg_test_date",
                      "nc_test_name")
  cts_fnames_nc_nodil = c("nc_sco", "nc_interpretation",
                          "nc_total_ig_reagent_lot",
                          "nc_total_ig_test_date")
  cts_fnames_nc_dil = c("nc_sco_dil_1",
                        "nc_sco_dil_20",
                        "nc_sco_dil_400",
                        "nc_interpretation",
                        "nc_total_ig_reagent_lot_dil_1",
                        "nc_total_ig_reagent_lot_dil_20",
                        "nc_total_ig_reagent_lot_dil_400",
                        "nc_total_ig_test_date_dil_1",
                        "nc_total_ig_test_date_dil_20",
                        "nc_total_ig_test_date_dil_400")
  cts_fnames_dil_cts_resultsing = c("nc_dilution_factor", "nc_sco",
                               "nc_interpretation", "nc_total_ig_reagent_lot",
                               "nc_total_ig_test_date")

  # Checking function arguments
  if (missing(nc_dilutions)) {
    dil_choice = dlg_list(c("Yes", "No"), preselect = "No", title = "Pull dilutional NC testing results?")
    if (dil_choice$res == "Yes") {
      nc_dilutions = T
    } else {
      nc_dilutions = F
    }
  }

  if (missing(study_source)) {
    study_source_add = F
  } else {
    study_source_add = T
  }

  # Setting up for either processes
  if (nc_dilutions == T) {
    cts_fnames = append(cts_fnames_base, cts_fnames_nc_dil)
  } else {
    cts_fnames = append(cts_fnames_base, cts_fnames_nc_nodil)
  }

  # Read through the list ----
  cts_results = data.frame()
  for (i in 1:length(cts_reslist)) {
    cat(sprintf("\r> Reading in file number %.0f of %.0f", i, length(cts_reslist)))
    temp = read.csv(paste0(cts_resdir, cts_reslist[i]))
    if (all(cts_fnames %in% names(temp))) {
      temp = temp %>% select(all_of(cts_fnames))
      if (grepl("\\/", temp[1,]$vitros_quant_s_igg_test_date)) {
        temp$vitros_quant_s_igg_test_date = as.character(as.Date(temp$vitros_quant_s_igg_test_date, format = "%m/%d/%Y"))
      }
      if (nc_dilutions == F) {
        # Imposing date format for NC testing date
        if (grepl("\\/", temp[1,]$nc_total_ig_test_date)) {
          temp$nc_total_ig_test_date = as.character(as.Date(temp$nc_total_ig_test_date, format = "%m/%d/%Y"))
        }
      } else {
        # Imposing date format for NC testing date variables
        if (grepl("\\/", temp[1,]$nc_total_ig_test_date_dil_1)) {
          temp$nc_total_ig_test_date_dil_1 = as.character(as.Date(temp$nc_total_ig_test_date_dil_1, format = "%m/%d/%Y"))
        }
        if (grepl("\\/", temp[1,]$nc_total_ig_test_date_dil_20)) {
          temp$nc_total_ig_test_date_dil_20 = as.character(as.Date(temp$nc_total_ig_test_date_dil_20, format = "%m/%d/%Y"))
        }
        if (grepl("\\/", temp[1,]$nc_total_ig_test_date_dil_400)) {
          temp$nc_total_ig_test_date_dil_400 = as.character(as.Date(temp$nc_total_ig_test_date_dil_400, format = "%m/%d/%Y"))
        }
      }
      cts_results = rbind(cts_results, temp)
    }
  }

  # Dilutional testing reformatting, where applicable
  if (nc_dilutions == T) {
    # Handle duplicates separately
    cts_results %>% distinct() -> cts_results
    dup_dins = cts_results$din[duplicated(cts_results$din)]
    dups = cts_results %>% filter(din %in% dup_dins)
    cts_results %>% filter(din %in% dup_dins == F) -> cts_results
    dups %>%
      mutate(nc_sco_dil_20 = case_when(
        nc_sco_dil_20 == 9999.88 ~ NA_real_,
        nc_sco_dil_20 == 999999.88 ~ NA_real_,
        nc_sco_dil_20 != 999999.88 & nc_sco_dil_20 != 9999.88 ~ nc_sco_dil_20
      )) %>%
      group_by(din) %>%
      slice_max(order_by = nc_sco_dil_20, n = 1) %>%
      slice_sample(n = 1) %>%
      ungroup() -> dups
      cts_results = rbind(cts_results, dups)

    # Reformat the results, per variable
    #  SCOs
    nc_scos = cts_results %>%
      select(din, nc_sco_dil_1, nc_sco_dil_20, nc_sco_dil_400) %>%
      pivot_longer(cols = c(nc_sco_dil_1, nc_sco_dil_20, nc_sco_dil_400),
                   names_to = "nc_dilution_factor", values_to = "nc_sco") %>%
      mutate(nc_dilution_factor = case_when(
        nc_dilution_factor == "nc_sco_dil_1" ~ 1,
        nc_dilution_factor == "nc_sco_dil_20" ~ 20,
        nc_dilution_factor == "nc_sco_dil_400" ~ 400),
        nc_dilution_factor = as.integer(nc_dilution_factor)
      ) %>%
      distinct()

    #  Testing dates
    nc_tdates = cts_results %>%
      select(din, nc_total_ig_test_date_dil_1, nc_total_ig_test_date_dil_20, nc_total_ig_test_date_dil_400) %>%
      pivot_longer(cols = c(nc_total_ig_test_date_dil_1, nc_total_ig_test_date_dil_20, nc_total_ig_test_date_dil_400),
                   names_to = "nc_dilution_factor", values_to = "nc_total_ig_test_date") %>%
      mutate(nc_dilution_factor = case_when(
        nc_dilution_factor == "nc_total_ig_test_date_dil_1" ~ 1,
        nc_dilution_factor == "nc_total_ig_test_date_dil_20" ~ 20,
        nc_dilution_factor == "nc_total_ig_test_date_dil_400" ~ 400),
        nc_dilution_factor = as.integer(nc_dilution_factor)
      ) %>%
      distinct()

    #  Testing reagent lot numbers
    nc_lots = cts_results %>%
      select(din, nc_total_ig_reagent_lot_dil_1, nc_total_ig_reagent_lot_dil_20, nc_total_ig_reagent_lot_dil_400) %>%
      mutate(nc_total_ig_reagent_lot_dil_1 = as.integer(nc_total_ig_reagent_lot_dil_1),
             nc_total_ig_reagent_lot_dil_20 = as.integer(nc_total_ig_reagent_lot_dil_20),
             nc_total_ig_reagent_lot_dil_400 = as.integer(nc_total_ig_reagent_lot_dil_400)) %>%
      pivot_longer(cols = c(nc_total_ig_reagent_lot_dil_1, nc_total_ig_reagent_lot_dil_20, nc_total_ig_reagent_lot_dil_400),
                   names_to = "nc_dilution_factor", values_to = "nc_total_ig_reagent_lot") %>%
      mutate(nc_dilution_factor = case_when(
        nc_dilution_factor == "nc_total_ig_reagent_lot_dil_1" ~ 1,
        nc_dilution_factor == "nc_total_ig_reagent_lot_dil_20" ~ 20,
        nc_dilution_factor == "nc_total_ig_reagent_lot_dil_400" ~ 400),
        nc_dilution_factor = as.integer(nc_dilution_factor)
      ) %>%
      distinct()

    # Joining the NC results
    nc_results = full_join(nc_scos, nc_tdates, by = c("din","nc_dilution_factor")) %>%
      full_join(., nc_lots, by = c("din","nc_dilution_factor")) %>%
      filter(nc_total_ig_test_date != as.Date("2222-01-01") & nc_sco != 999999.88)

    #  Final joining and reformatting
    cts_results = left_join(cts_results, nc_results, by = "din") %>%
      select(all_of(append(cts_fnames_base, cts_fnames_dil_cts_resultsing))) %>%
      distinct()

    #  Ortho NC formatting
    cts_results$nc_test_name[is.na(cts_results$nc_test_name)] = "not_tested"
    cts_results$nc_interpretation[is.na(cts_results$nc_interpretation)] = "not_tested"
    cts_results$nc_sco[is.na(cts_results$nc_sco)] = 999999.88
    cts_results$nc_dilution_factor[is.na(cts_results$nc_dilution_factor)] = 999999.88
    cts_results$nc_total_ig_reagent_lot[is.na(cts_results$nc_total_ig_reagent_lot)] = 9998
    cts_results$nc_total_ig_test_date[is.na(cts_results$nc_total_ig_test_date)] = as.Date("2222-01-01")
    # 0.00 is not an acceptable entry
    cts_results$nc_sco[cts_results$nc_sco == "0.00"] = "0.01"

    # Study source variables
    if (missing(study_source) == F) {
      cts_results$cv2gs_study_source = study_source
      cts_results$nc_study_source = study_source
    }
  }
  return(cts_results)
}
