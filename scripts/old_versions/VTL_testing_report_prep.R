##################################
# VTL Testing Report Preparation
# v1.0; 08/09/2022
##################################

prep_vtl_testing_report = function(savedir, start_date, end_date) {
# Dependencies ----
require(dplyr)
require(odbc)
require(DBI)
require(svDialogs)
require(glue)
require(yaml)

# Configuration and helper scripts
#  Config file
conf = read_yaml("data_management/config.yml")
#  Helper scripts
if (file.exists("data_management/helper_scripts/db_connect.R")) {
  source("data_management/helper_scripts/db_connect.R")
} else {
  stop("missing database connection script")
}
if (file.exists("data_management/helper_scripts/CTS_etl.R")) {
  source("data_management/helper_scripts/CTS_etl.R")
} else {
  stop("missing CTS ETL script")
}
if (file.exists("data_management/vtl_result_mssql_accessioning.R")) {
  source("data_management/vtl_result_mssql_accessioning.R")
} else {
  stop("missing results accessioning script")
}

# Variables and default values
treport_fnames = conf$reporting_flows$testing_report$fieldnames
today_date = format(Sys.Date(), format = "%Y%m%d")
#  Local saving directory
if (missing(savedir)) {
  savedir = "~/Desktop"
}
#  Start date
if (missing(start_date)) {
  stop("Missing start date")
} else {
  start_date = as.Date(start_date)
  if (class(start_date) == "character") {
    stop("Invalid start date")
  }
}
#  End date
if (missing(end_date)) {
  stop("Missing end date")
} else {
  end_date = as.Date(end_date)
  if (class(end_date) == "character") {
    stop("Invalid end date")
  }
}

# Accession, Query, and report ----
#  Connect to the Db
lc_conn = connect_db()
#  Check for if a connection can be made
if (is.null(lc_conn) == F) {
  # Query the donation and sample tables from the Db ----
  #  Visits table
  visits = dbReadTable(lc_conn, "visits")
  #  Samples table
  samples = dbReadTable(lc_conn, "samples")

  # Pull the CTS-reported results ----
  cts_results = pull_cts_results() %>%
    select(-collection_date) %>%
    filter(din %in% visits$din)
  names(cts_results)[names(cts_results) == "nc_quant"] = "nc_sco"
  cts_results$vitros_quant_s_igg_test_date[cts_results$vitros_quant_s_igg_test_date == ""] = "2222-01-01"
  cts_results$nc_total_ig_test_date[cts_results$nc_total_ig_test_date == ""] = "2222-01-01"

  # Join on the results ----
  dondat = left_join(visits, samples, by = "din") %>%
      select(-aliquot_type, -aliquot_anticoagulant)
  treport = inner_join(dondat, cts_results, by = "din")

  # Report what did match ----
  if ("nc_test_name" %in% names(treport) == F) {
    treport$nc_test_name = "ortho_total_ig_nc"
  }
  treport = unique(treport) %>%
    select(all_of(treport_fnames)) %>%
    filter(between(collection_date, start_date, end_date))
  treport$rownum = row.names(treport)

  # Omitting duplicates
  dups = treport %>% filter(din %in% treport$din[duplicated(treport$din)])
  din_list = unique(dups$din)
  for (i in 1:length(din_list)) {
    temp = subset(dups, dups$din == din_list[i])
    if (length(unique(temp$vitros_quant_s_igg_quantification)) > 1) {
      if ("within_loq" %in% unique(temp$vitros_quant_s_igg_quantification)) {
        dups = subset(dups, dups$rownum != temp$rownum[temp$vitros_quant_s_igg_quantification == "within_loq"])
      }
    } else {
      max_date = max(temp$vitros_quant_s_igg_test_date)
      dups = subset(dups, dups$rownum %in% temp$rownum[temp$vitros_quant_s_igg_test_date != max_date] == F)
    }
  }
  treport = treport %>%
    filter(rownum %in% dups$rownum == F) %>%
    select(-rownum)

  write.csv(treport, glue("{savedir}/VTL_testing_report_{today_date}.csv"), row.names = F)
  vtl_result_accession(treport)
  return(treport)
} else {
  stop("can't connect to the Db")
}
}
