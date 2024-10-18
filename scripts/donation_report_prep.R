################################
# Prepare RDC Donation Report
# v1.0; 3/13/2024
################################

#' @export
prep_donation_report = function(start_date, end_date, period_label) {
  # Dependencies and Setup ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)
  require(yaml)
  require(stringr)

  #  Configuration and helper scripts ----
  cat("\nDonation report preparation initiated", fill = T)
  cat("-------------------------------------", fill = T)
  conf = read_yaml("scripts/config.yml")
  don_report_conf = conf$reporting_flows$donation_report
  source("scripts/helper_scripts/db_connect.R")

  #  Variable check ----
  cat("Checking for required function variables..", fill = T)
  if (missing(start_date)) {
    stop("missing start date")
  } else {
    start_date = as.Date(start_date, format = "%Y-%m-%d")
    if (class(start_date) != "Date") {
      stop("invalid start date")
    }
  }
  if (missing(end_date)) {
    stop("missing end date")
  } else {
    end_date = as.Date(end_date, format = "%Y-%m-%d")
    if (class(start_date) != "Date") {
      stop("invalid end date")
    }
  }
  cat("> Variable check complete\n", fill = T)

  # Data pull ----
  cat("Checking for database connection..", fill = T)
  lc_conn = connect_db()
  if (is.null(lc_conn) == F) {
    cat("> Connected to the database. Performing data pull..", fill = T)
    donors = dbReadTable(lc_conn, "donors")
    visits = dbReadTable(lc_conn, "visits")
    visits$edw_eth_code = str_pad(visits$edw_eth_code, width = 3, pad = "0", side = "left")
    visits$edw_eth_code[visits$edw_eth_code == '000'] = ''
    raceref = dbReadTable(lc_conn, "race_ref")
    merged_ids = dbReadTable(lc_conn, "merged_donor_ids")
    cat("> Data pull complete\n", fill = T)
  } else {
    stop("failed to connect to the study database")
  }

  # Merging the tables
  cat("Formatting the data per codebook specifications..", fill = T)
  vtlv = visits %>%
    filter(between(collection_date, start_date, end_date),
           substr(din,1,1) == "W")
  vtlv = vtlv %>%
    right_join(donors, ., by = "donor_id") %>%
    left_join(., raceref, by = c("edw_eth_code", "edw_race_desc")) %>%
    mutate(bco = "VTL",
           age_in_months = interval(mob, Sys.Date()) %/% months(1),
           sample_status = case_when(sample_in_repository == T ~ "available",
                                     sample_in_repository == F ~ "unavailable"),
           abo_blood_group = toupper(abo_group),
           abo_blood_group = case_when(
             is.na(abo_blood_group) ~ "unavailable",
             abo_blood_group == "A" ~ "A",
             abo_blood_group == "B" ~ "B",
             abo_blood_group == "AB" ~ "AB",
             abo_blood_group == "O" ~ "O"),
           rh_factor = rh_group,
           rh_factor = case_when(
             is.na(rh_factor) ~ "unavailable",
             rh_factor == "positive" ~ "positive",
             rh_factor == "negative" ~ "negative"),
           ethnicity_hispanic = tolower(ethnicity_hispanic),
           dhq_vaccination_status_donation = case_when(
             is.na(dhq_vaccination_status_donation) ~ "unavailable",
             dhq_vaccination_status_donation == T ~ "vaccinated",
             dhq_vaccination_status_donation == F ~ "not_vaccinated"),
           dhq_vaccination_status_ever = case_when(
             is.na(dhq_vaccination_status_ever) ~ "unavailable",
             dhq_vaccination_status_ever == T ~ "vaccinated_ever",
             dhq_vaccination_status_ever == F ~ "not_vaccinated_ever"),
           phlebotomy_status = case_when(
             phlebotomy_status == T ~ "successful_phlebotomy",
             phlebotomy_status == F ~ "unsuccessful_phlebotomy")) %>%
    filter(rdc_2023 == T) %>%
    select(all_of(don_report_conf$fieldnames))
  # Handle NAs
  vtlv$donation_procedure[is.na(vtlv$donation_procedure)] = 'other'
  # Handle merged donor ids
  need_reversion = vtlv$donor_id[vtlv$donor_id %in% merged_ids$current_donor_id]
  if (length(need_reversion) > 0) {
    for (i in 1:length(need_reversion)) {
      current_donid = need_reversion[i]
      prev_donid = merged_ids$previous_donor_id[merged_ids$current_donor_id == current_donid]
      vtlv$donor_id[vtlv$donor_id == current_donid] = prev_donid
    }
  }
  # Produce report
  cat("> report preparation complete\n", fill = T)
  date = format(Sys.Date(), format = "%Y%m%d")
  write.csv(vtlv, paste0("~/Desktop/VTL_donation_report_",period_label,"_",date,".csv"), row.names = F)
  return(vtlv)
}
