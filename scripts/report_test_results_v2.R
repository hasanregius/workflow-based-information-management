##############################
# RDC Test Report Preparation
# v2.2; 3/13/2024
##############################
# Set the default values in the config file prior to use

#' @export
testing_report_prep_ncdil = function(key_list, output_directory, testing_period, bco, results_source) {
  # Dependencies ----
  require(dplyr)
  require(tidyr)
  require(glue)
  require(DBI)
  require(odbc)
  require(striprtf)
  require(dbplyr)
  require(yaml)
  require(svDialogs)

  # Setting variables
  conf = read_yaml("scripts/config.yml")
  conf_treport = conf$reporting_flows$testing_report
  required_scripts = conf_treport$scripts
  treport_fnames = conf_treport$fieldnames

  # Loading helper scripts
  if (all(file.exists(required_scripts))) {
    lapply(required_scripts, source)
  } else {
    stop("missing scripts")
  }

  # Default values ----
  #  key_list ----
  if (missing(key_list)) {
    stop("No list of identifiers supplied..\n")
  } else {
    cat("List of identifiers supplied - pulling data based on the dins provided..\n", fill = T)
  }

  #  output_directory ----
  if (missing(output_directory)) {
    if (is.null(conf$reporting_flows$testing_report$directories$output)) {
      output_directory = dlg_input(message = "Please enter a valid output directory:")
    } else {
      output_directory = conf$reporting_flows$testing_report$directories$output
    }
  }
  # Check for validity
  if (dir.exists(output_directory) == F) {
    stop("invalid output diectory")
  }

  #  testing_period ----
  if (missing(testing_period)) {
    ask_period = dlg_input(
      message = "Please enter the testing period for the report:",
      default = "idk"
      )
    testing_period = ask_period$res
  }

  # results_source ----
  if (missing(results_source)) {
    results_source = "rdc"
  }

  #  blood center ----
  if (missing(bco)) {
    bco = dlgInput(
      message = "Is the report preparation for VTL/ARC?"
    )
    bco = toupper(bco$res)
  }
  if (bco %in% conf$codebook$bco$factors == F) {
    stop("no blood center organization chosen")
  } else {
    if (bco == "ARC") {
      schema = "arc"
    } else {
      schema = "dbo"
    }
  }

  # Connect to the Database, pull, format ----
  lc_conn = connect_db()
  #  Connect and pull the data
  if (!is.null(lc_conn)) {
    # Pulling the raw datasets ----
    #  Metadata ----
    if (bco == "VTL") {
      donors = as.data.frame(dbReadTable(lc_conn, "donors"))
      visits = as.data.frame(dbReadTable(lc_conn, "visits"))
      metadata = right_join(donors, visits, by = "donor_id")
      samples = as.data.frame(dbReadTable(lc_conn, "samples"))
      metadata = left_join(metadata, samples, by = "din")
      merged_ids = dbReadTable(lc_conn, "merged_donor_ids")
    } else {
      metadata = lc_conn %>%
        tbl(in_schema(schema, "specimens")) %>%
        as.data.frame()
    }
    metadata$bco = as.character(bco)
    metadata = metadata %>%
      filter(din %in% key_list)

    #  Ortho IgG ----
    if (bco == "VTL") {
      if (is.null(study_source)) {
        results_igg = lc_conn %>%
          tbl(in_schema(schema,"cv2gs")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   final_result == T) %>%
          distinct()
      } else {
        results_igg = lc_conn %>%
          tbl(in_schema(schema,"cv2gs")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   final_result == T) %>%
          distinct()
      }
    } else {
      results_igg = lc_conn %>%
        tbl(in_schema(schema,"cv2gs")) %>%
        as.data.frame() %>%
        filter(din %in% key_list &
                 final_result == T) %>%
        distinct()
    }

    #  Ortho NC ----
    if (bco == "ARC") {
      results_nc = lc_conn %>%
        tbl(in_schema(schema,"cv2tn")) %>%
        as.data.frame() %>%
        filter(din %in% key_list) %>%
        mutate(nc_test_name = "ortho_total_ig_nc") %>%
        select(-nc_study_source) %>%
        distinct()
      names(results_nc)[names(results_nc) == "nc_quant"] = "nc_sco"
    } else {
      if(is.null(results_source)) {
        results_nc = lc_conn %>%
          tbl(in_schema(schema, "nc_total_ig_results")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   nc_total_ig_test == "ortho") %>%
          mutate(nc_test_name = "ortho_total_ig_nc")
        names(results_nc)[names(results_nc) == "nc_quant"] = "nc_sco"
      } else {
        results_nc = lc_conn %>%
          tbl(in_schema(schema, "nc_total_ig_results")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   nc_total_ig_test == "ortho" &
                   nc_study_source == results_source) %>%
          mutate(nc_test_name = "ortho_total_ig_nc")
        names(results_nc)[names(results_nc) == "nc_quant"] = "nc_sco"
      }
    }

    #  Selecting most recently tested result for each DIN & dilution factor
    results_nc = results_nc %>%
      group_by(din, nc_dilution_factor) %>%
      slice_max(nc_total_ig_test_date, n = 1) %>%
      ungroup()

    # In case none were diluted
    if (TRUE %in% (c(20,400) %in% results_nc$nc_dilution_factor)) {
      #  Reformatting the variables
      nc_sample_level = results_nc %>%
        select(din, nc_test_name, nc_interpretation) %>%
        distinct()

      results_nc_sco = results_nc %>%
        select(din, nc_dilution_factor, nc_sco) %>%
        distinct()

      results_nc_sco = results_nc_sco %>%
        pivot_wider(names_from = nc_dilution_factor,
                    names_prefix = "nc_sco_dil_",
                    id_cols = din,
                    values_from = nc_sco) %>%
        mutate(nc_sco_dil_1 = replace_na(nc_sco_dil_1, 999999.88),
               nc_sco_dil_20 = replace_na(nc_sco_dil_20, 999999.88),
               nc_sco_dil_400 = replace_na(nc_sco_dil_400, 999999.88))

      results_nc_lot = results_nc %>%
        select(din, nc_dilution_factor, nc_total_ig_reagent_lot) %>%
        distinct()

      results_nc_lot = results_nc_lot %>%
        pivot_wider(names_from = nc_dilution_factor,
                    names_prefix = "nc_total_ig_reagent_lot_dil_",
                    id_cols = din,
                    values_from = nc_total_ig_reagent_lot) %>%
        mutate(nc_total_ig_reagent_lot_dil_1 = replace_na(nc_total_ig_reagent_lot_dil_1, 9998),
               nc_total_ig_reagent_lot_dil_20 = replace_na(nc_total_ig_reagent_lot_dil_20, 9998),
               nc_total_ig_reagent_lot_dil_400 = replace_na(nc_total_ig_reagent_lot_dil_400, 9998))

      results_nc_date = results_nc %>%
        select(din, nc_dilution_factor, nc_total_ig_test_date) %>%
        distinct()

      results_nc_date = results_nc_date %>%
        pivot_wider(names_from = nc_dilution_factor,
                    names_prefix = "nc_total_ig_test_date_dil_",
                    id_cols = din,
                    values_from = nc_total_ig_test_date) %>%
        mutate(nc_total_ig_test_date_dil_1 = replace_na(nc_total_ig_test_date_dil_1, as.Date("2222-01-01")),
               nc_total_ig_test_date_dil_20 = replace_na(nc_total_ig_test_date_dil_20, as.Date("2222-01-01")),
               nc_total_ig_test_date_dil_400 = replace_na(nc_total_ig_test_date_dil_400, as.Date("2222-01-01")))

      results_nc_findil = results_nc %>%
        filter(nc_final_result == T) %>%
        group_by(din) %>%
        slice_max(nc_dilution_factor) %>%
        ungroup() %>%
        mutate(nc_dilution_factor_final_result = nc_dilution_factor) %>%
        select(din, nc_dilution_factor_final_result)

      # Generating final table to report
      nc_fnames = append(c("din"), conf_treport$fieldnames[grepl("nc_",conf_treport$fieldnames)])
      results_nc = left_join(nc_sample_level, results_nc_sco, by = "din") %>%
        left_join(., results_nc_lot, by = "din") %>%
        left_join(., results_nc_date, by = "din") %>%
        left_join(., results_nc_findil, by = "din") %>%
        select(all_of(nc_fnames)) %>%
        distinct()
    } else {
      # Generating final table to report
      nc_fnames = append(c("din"), conf_treport$fieldnames[grepl("nc_",conf_treport$fieldnames)])
      results_nc = results_nc %>%
        mutate(nc_sco_dil_1 = nc_sco,
               nc_sco_dil_20 = 999999.88,
               nc_sco_dil_400 = 999999.88,
               nc_total_ig_reagent_lot_dil_1 = nc_total_ig_reagent_lot,
               nc_total_ig_reagent_lot_dil_20 = 9998,
               nc_total_ig_reagent_lot_dil_400 = 9998,
               nc_total_ig_test_date_dil_1 = nc_total_ig_test_date,
               nc_total_ig_test_date_dil_20 = as.Date("2222-01-01"),
               nc_total_ig_test_date_dil_400 = as.Date("2222-01-01"),
               nc_dilution_factor_final_result = 1) %>%
        select(all_of(nc_fnames)) %>%
        distinct()
    }

    # Subset for the testing report ----
    treport = metadata %>%
      filter(din %in% key_list) %>%
      as.data.frame() %>%
      distinct()
    names(treport)[names(treport) == "donorid"] = "donor_id"

    # Join the tables and format ----
    treport = left_join(treport, results_igg, by = "din") %>% distinct()
    treport = left_join(treport, results_nc, by = "din") %>% distinct()
    treport = treport %>% select(all_of(treport_fnames)) %>% distinct()

    # Handling NA values for the fields ----
    for (i in 1:nrow(treport)) {
      # Ortho IgG Quant
      if (is.na(treport[i,]$vitros_quant_s_igg_bauml) |
          treport[i,]$vitros_quant_s_igg_bauml == "NA") {
        treport[i,]$vitros_quant_s_igg_quantification = "not_tested"
        treport[i,]$vitros_quant_s_igg_dilution_factor = 998
        treport[i,]$vitros_quant_s_igg_interpretation = "not_tested"
        treport[i,]$vitros_quant_s_igg_reagent_lot = 9998
        treport[i,]$vitros_quant_s_igg_test_date = as.Date("2222-01-01")
        treport[i,]$vitros_quant_s_igg_bauml = 999999.88
      }
      # Ortho Total Ig NC
      if (is.na(treport[i,]$nc_dilution_factor_final_result)) {
        treport[i,]$nc_test_name = "not_tested"
        treport[i,]$nc_interpretation = "not_tested"
        treport[i,]$nc_sco_dil_1 = 999999.88
        treport[i,]$nc_sco_dil_20 = 999999.88
        treport[i,]$nc_sco_dil_400 = 999999.88
        treport[i,]$nc_total_ig_reagent_lot_dil_1 = 9998
        treport[i,]$nc_total_ig_reagent_lot_dil_20 = 9998
        treport[i,]$nc_total_ig_reagent_lot_dil_400 = 9998
        treport[i,]$nc_total_ig_test_date_dil_1 = as.Date("2222-01-01")
        treport[i,]$nc_total_ig_test_date_dil_20 = as.Date("2222-01-01")
        treport[i,]$nc_total_ig_test_date_dil_400 = as.Date("2222-01-01")
        treport[i,]$nc_dilution_factor_final_result = 998
      }
      # Aliquot Type
      if (is.na(treport[i,]$aliquot_type) |
          treport[i,]$aliquot_type == "NA") {
        treport[i,]$aliquot_type = "unavailable"
      }
      # Aliquot Anticoagulant
      if (is.na(treport[i,]$aliquot_anticoagulant) |
          treport[i,]$aliquot_anticoagulant == "NA") {
        treport[i,]$aliquot_anticoagulant = "unavailable"
      }
    }

    # Standardizing the donor ID prefixes ----
    treport$donor_id[nchar(treport$donor_id) == 7 & substr(treport$donor_id,1,3) != bco] = paste0(bco,treport$donor_id[nchar(treport$donor_id) == 7])

    # Ortho NC formatting ----
    treport$nc_sco_dil_1 = formatC(treport$nc_sco_dil_1, format = "f", digits = 2)
    treport$nc_sco_dil_20 = formatC(treport$nc_sco_dil_2, format = "f", digits = 2)
    treport$nc_sco_dil_400 = formatC(treport$nc_sco_dil_400, format = "f", digits = 2)
    # 0.00 is not an acceptable entry
    treport$nc_sco_dil_1[treport$nc_sco_dil_1 == "0.00"] = "0.01"
    treport$nc_sco_dil_20[treport$nc_sco_dil_20 == "0.00"] = "0.01"
    treport$nc_sco_dil_400[treport$nc_sco_dil_400 == "0.00"] = "0.01"
    treport$vitros_quant_s_igg_interpretation[treport$vitros_quant_s_igg_interpretation == "qns" &
                                                treport$vitros_quant_s_igg_quantification == "not_tested"] = "not_tested"

    # Ortho S IgG decimal count ----
    treport$vitros_quant_s_igg_bauml = formatC(treport$vitros_quant_s_igg_bauml, format = "f", digits = 2)
    treport$vitros_quant_s_igg_bauml[treport$vitros_quant_s_igg_bauml == "9999.88"] = "999999.88"
    treport$vitros_quant_s_igg_bauml[treport$vitros_quant_s_igg_bauml == "0.00"] = "0.01"

    # Handle merged donor IDs ----
    if (bco == "VTL") {
      need_reversion = treport$donor_id[treport$donor_id %in% merged_ids$current_donor_id]
      if (length(need_reversion) > 0) {
        for (i in 1:length(need_reversion)) {
          current_donid = need_reversion[i]
          prev_donid = merged_ids$previous_donor_id[merged_ids$current_donor_id == current_donid]
          treport$donor_id[treport$donor_id == current_donid] = prev_donid
        }
      }
    }

    # Save the output ----
    today_date = format(Sys.Date(), format = "%Y%m%d")
    file_name = glue("{bco}_testing_report_{testing_period}_{today_date}.csv")
    write.csv(treport, glue("{output_directory}/{file_name}"), row.names = F)
    return(treport)
  } else {
    stop("can't connect to the Db")
  }
}

