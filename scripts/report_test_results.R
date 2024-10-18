##############################
# RDC Test Report Preparation
# v1.1; 3/13/2024
##############################
# Set the default values in the config file prior to use

#' @export
testing_report_prep = function(key_list, output_directory, testing_period, bco, results_source) {
  # Dependencies ----
  require(dplyr)
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
      } else {
        metadata = lc_conn %>%
          tbl(in_schema(schema,"samples")) %>%
          as.data.frame()
      }
      metadata$bco = as.character(bco)
      metadata = metadata %>%
        filter(din %in% key_list)

      #  Ortho IgG ----
      if (bco == "VTL") {
        results_igg = lc_conn %>%
          tbl(in_schema(schema,"cv2gs")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   final_result == T &
                   cv2gs_study_source == results_source) %>%
          distinct()
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
          mutate(nc_test_name = "ortho_total_ig_nc")
        names(results_nc)[names(results_nc) == "nc_quant"] = "nc_sco"
      } else {
        results_nc = lc_conn %>%
          tbl(in_schema(schema, "nc_total_ig_results")) %>%
          as.data.frame() %>%
          filter(din %in% key_list &
                   nc_total_ig_test == "ortho" &
                   nc_final_result == T &
                   nc_study_source == results_source) %>%
          mutate(nc_test_name = "ortho_total_ig_nc")
        names(results_nc)[names(results_nc) == "nc_quant"] = "nc_sco"
      }

      # Subset for the testing report ----
      treport = metadata %>%
        filter(din %in% key_list) %>%
        as.data.frame()
      names(treport)[names(treport) == "donorid"] = "donor_id"

      # Join the tables and format ----
      treport = left_join(treport, results_igg, by = "din")
      treport = left_join(treport, results_nc, by = "din")
      treport = treport %>% select(all_of(treport_fnames))

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
        if (is.na(treport[i,]$nc_sco) |
            treport[i,]$nc_sco == "NA") {
          treport[i,]$nc_test_name = "ortho_total_ig_nc"
          treport[i,]$nc_interpretation = "not_tested"
          treport[i,]$nc_total_ig_reagent_lot = 9998
          treport[i,]$nc_total_ig_test_date = as.Date("2222-01-01")
          treport[i,]$nc_sco = 9999.88
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
      treport$nc_sco = formatC(treport$nc_sco, format = "f", digits = 2)
      # 0.00 is not an acceptable entry
      treport$nc_sco[treport$nc_sco == "0.00"] = "0.01"
      treport$vitros_quant_s_igg_interpretation[treport$vitros_quant_s_igg_interpretation == "qns" & treport$vitros_quant_s_igg_quantification == "not_tested"] = "not_tested"
      # dilution factor
      treport$nc_dilution_factor[is.na(treport$nc_dilution_factor)] = 998
      treport$nc_dilution_factor[treport$nc_dilution_factor == ""] = 998

      # Ortho S IgG decimal count ----
      treport$vitros_quant_s_igg_bauml = formatC(treport$vitros_quant_s_igg_bauml,
                                                 format = "f", digits = 2)
      treport$vitros_quant_s_igg_bauml[treport$vitros_quant_s_igg_bauml == "0.00"] = "0.01"

      # Save the output ----
      today_date = format(Sys.Date(), format = "%Y%m%d")
      file_name = glue("{bco}_testing_report_{testing_period}_{today_date}.csv")
      write.csv(treport, glue("{output_directory}/{file_name}"), row.names = F)
      return(treport)
    } else {
      stop("can't connect to the Db")
    }
}
