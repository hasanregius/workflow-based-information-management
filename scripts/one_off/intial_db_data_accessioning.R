# Setup ----
require(DBI)
require(odbc)
require(striprtf)
require(arrow)
require(dplyr)
require(glue)
require(lubridate)
source("data_management/vitros_etl.R")

# Connection string for the Db
lc_conn = dbConnect(odbc(), Driver = "freetds",
                    Server = "bsiwsqlp016.bloodsystems.org",
                    Database = "longitudinal_cohort",
                    Port = 1433,
                    uid = "BSI\\C02715",
                    pwd = gsub("die", "cide",readChar(
                      "~/Desktop/creds.txt", nchar = 30)))

# Pull fieldnames
donors_fnames = dbListFields(lc_conn, "donors")
visits_fnames = dbListFields(lc_conn, "visits")
samples_fnames = dbListFields(lc_conn, "samples")

# Reading in parquet file from Eduard ----
parq = read_parquet("~/Downloads/rdc/vrdc_data_toMar22.parquet", as_data_frame = T)
#  Transform
parq = parq %>%
  select(
    din = DONATION_NUMBER,
    donor_id = MA_ID,
    collection_date = donation_date,
    qualtrics_did = DID,
    vx_dhq_response = YesVacc,
    cohort = cohort,
    gender = DONOR_GENDER,
    zip_code = DONOR_ZIP,
    dob = DONOR_DATE_OF_BIRTH
  ) %>%
  mutate(
    dob = as.Date(dob, format = "%d-%B-%y"),
    mob = floor_date(dob, unit = "month"),
    # Please review the mutation, Eduard
    vx_dhq_response = case_when(vx_dhq_response %in% "(0)No" ~ 0,
                                vx_dhq_response %in% "(1)indexYes" ~ 1,
                                vx_dhq_response %in% "(2)priorYes" ~ 1,
                                vx_dhq_response %in% "(3)NoWithPriorYes" ~ 0),
    vx_dhq_response = na_if(vx_dhq_response, "(4)BlankwithPriorYes")
  ) %>%
  select(-dob)

# Reading in donor baseline report and QC-ing against Eduard's file----
donrep = read.csv("~/Downloads/rdc/VTL_donor_report_20220411.csv")
donrep$donor_id = paste0("VTL", donrep$donor_id)

#  Check for cohort mismatch
diff_cohort = select(donrep, c("donor_id","cohort"))
names(diff_cohort)[2] = "cohort_report"
eg = select(parq, "donor_id", "cohort")
names(eg)[2] = "cohort_egrebe"
diff_cohort = left_join(diff_cohort, eg)
diff_cohort[,2:3] = trimws(diff_cohort[,2:3], "both")
summary(diff_cohort$cohort_report != diff_cohort$cohort_egrebe)

# Creating the master dataset ----
parq = subset(parq, parq$donor_id %in% donrep$donor_id)
master = left_join(parq, donrep, by = "donor_id")

#  Formatting
master = master %>%
  select(-"cohort.x", -"zip_code.y", -"gender.y",
         -"age_in_months", -"survey_response") %>%
  # 6 donations and 2 donors didn't have blood type. Omitted.
  filter(abo_blood_group != "unavailable", rh_factor != "unavailable") %>%
  mutate(abo_blood_group = tolower(abo_blood_group))

names(master)[names(master) == "gender.x"] = "gender"
names(master)[names(master) == "cohort.y"] = "cohort"
names(master)[names(master) == "zip_code.x"] = "zip_code"
names(master)[names(master) == "abo_blood_group"] = "abo_group"
names(master)[names(master) == "rh_factor"] = "rh_group"
names(master)[names(master) == "ethnicity_hispanic"] = "hispanic_eth"

# QC and accession for donor level ----
donors = select(master, all_of(donors_fnames))

# QC
donors = unique(donors)
donors1 = subset(donors, duplicated(donors$donor_id) == T)
donors1 = subset(donors, donors$donor_id %in% donors1$donor_id)
# VTL836SZ8D and VTL836SZAU have different dobs in the parquet file. Omitted.
donors = subset(donors, donors$donor_id %in% donors1$donor_id == F)

#  Checking for the state in the Db first
donors_list = lc_conn %>%
  tbl("donors") %>%
  pull("donor_id")
d_length = length(donors_list)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (donors$donor_id %in% donors_list)) {
    cat("Duplicate donor ID found, subsetting for unique IDs", fill = T)
    dons = subset(donors, donors$donor_id %in% donors_list == F)
    dbWriteTable(lc_conn, "donors", donors, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "donors", donors, append = TRUE)
  }
  # Check for if accessioning was successful
  donors_list = lc_conn %>%
    tbl("donors") %>%
    pull("donor_id")
  # Rollback in case of error
  if (length(donors_list) - d_length != nrow(donors)) {
    diff = length(donors_list) - d_length + nrow(donors)
    cat(paste(diff,"Donor ID's failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All donor data was successfully imported", fill = T)
  }
})

# QC and accession for visit level ----
## Missing 11 observations, as they belong to the omitted donors
visits = select(master, all_of(visits_fnames))
visits$gender[visits$gender == "M"] = "male"
visits$gender[visits$gender == "F"] = "female"
visits$hispanic_eth[visits$hispanic_eth == "false"] = 0
visits$hispanic_eth[visits$hispanic_eth == "true"] = 1
visits$hispanic_eth[visits$hispanic_eth == "unavailable"] = NA
visits$race[visits$race == ""] = NA
visits$race[visits$race == "unavailable"] = NA

#  QC
visits = unique(visits) # same as nrow so we'll just move on

#  Checking for the state in the Db first
din_list = lc_conn %>%
  tbl("visits") %>%
  pull("din")
din_length = length(din_list)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (visits$din %in% din_list)) {
    cat("Duplicate donor ID found, subsetting for unique IDs", fill = T)
    visits = subset(visits, visits$din %in% din_list == F)
    dbWriteTable(lc_conn, "visits", visits, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "visits", visits, append = TRUE)
  }
  # Check for if accessioning was successful
  din_list = lc_conn %>%
    tbl("visits") %>%
    pull("din")
  # Rollback in case of error
  if (length(din_list) - din_length != nrow(visits)) {
    diff = length(din_list) - (din_length + nrow(visits))
    cat(paste(diff,"DINs failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All visit data was successfully imported", fill = T)
  }
})

# QC and accession for sample level ----
master$list_date = Sys.Date()
samples = select(master, all_of(samples_fnames))

#  QC
samples = unique(samples) # same as nrow so we'll just move on
samples = subset(samples, samples$din %in% din_list)

#  Checking for the state in the Db first
din_list_samples = lc_conn %>%
  tbl("samples") %>%
  pull("din")
din_length_samples = length(din_list_samples)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (samples$din %in% din_list_samples)) {
    cat("Duplicate donor ID found, subsetting for unique IDs", fill = T)
    samples = subset(samples, samples$din %in% din_list_samples == F)
    dbWriteTable(lc_conn, "samples", samples, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "samples", samples, append = TRUE)
  }
  # Check for if accessioning was successful
  din_list_samples = lc_conn %>%
    tbl("samples") %>%
    pull("din")
  # Rollback in case of error
  if (length(din_list_samples) - din_length_samples != nrow(samples)) {
    diff = length(din_list_samples) - (din_length_samples + nrow(samples))
    cat(paste(diff,"DINs failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All sample data was successfully imported", fill = T)
  }
})

# Accessioning testing data from both EG's file and Clara ----
## Sorry, Eduard, I can only pull Total S because the others ones are missing fields
parq_results = read_parquet("~/Downloads/rdc/vrdc_data_toMar22.parquet", as_data_frame = T)
eg_results = parq_results %>%
  filter(is.na(E_st_interpretation) == F & is.na(din) == F) %>%
  select(
    din = CTS_DIN,
    vitros_s_total_ig_sco = E_st_sco,
    vitros_s_total_ig_interpretation = E_st_interpretation
  ) %>%
  mutate (
    vitros_s_total_ig_interpretation = case_when(
      vitros_s_total_ig_interpretation %in% "R" ~ "reactive",
      vitros_s_total_ig_interpretation %in% "NR" ~ "non-reactive"
    )
  )

# none of S results in EG's file is valid
cv2ts = eg_results

cv2ts_list = lc_conn %>%
  tbl("cv2ts") %>%
  pull("din")
cv2ts_length = length(cv2ts_list)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (cv2ts$din %in% cv2ts_list)) {
    cat("Duplicate donor ID found, subsetting for unique IDs", fill = T)
    cv2ts = subset(cv2ts, cv2ts$din %in% cv2ts_list == F)
    dbWriteTable(lc_conn, "cv2ts", cv2ts, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "cv2ts", cv2ts[1,], append = TRUE)
  }
  # Check for if accessioning was successful
  cv2ts_list = lc_conn %>%
    tbl("cv2ts") %>%
    pull("din")
  # Rollback in case of error
  if (length(cv2ts_list) - cv2ts_length != nrow(cv2ts)) {
    diff = length(cv2ts_list) - (cv2ts_length + nrow(cv2ts))
    cat(paste(diff,"Ortho S results failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All Total Ig S results were successfully imported", fill = T)
  }
})

## Clara's Ortho Results
## Only 1 result for Ortho S? Skipping for now
cil = rdc_vitros_pull("~/Vitalant/Research Operations Core - vitros raw data/")
cil$din = substr(cil$din, 1, 13)
cil = subset(cil, cil$din %in% din_list)

### CV2GS
cv2gs = subset(cil, is.na(cil$vitros_quant_s_igg_interpretation) == F)
cv2gs = select(cv2gs, all_of(dbListFields(lc_conn, "cv2gs")))
class(cv2gs$vitros_quant_s_igg_bauml) = "numeric"
cv2gs$vitros_quant_s_igg_bauml = formatC(cv2gs$vitros_quant_s_igg_bauml,
                                         digits = 2, format = "f")
cv2gs_list = lc_conn %>%
  tbl("cv2gs") %>%
  pull("din")
cv2gs_length = length(cv2gs_list)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (cv2gs$din %in% cv2gs_list)) {
    cat("Duplicate DIN found, subsetting for unique DINs", fill = T)
    cv2gs = subset(cv2gs, cv2gs$din %in% cv2gs_list == F)
    dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "cv2gs", cv2gs, append = TRUE)
  }
  # Check for if accessioning was successful
  cv2gs_list = lc_conn %>%
    tbl("cv2gs") %>%
    pull("din")
  # Rollback in case of error
  if (length(cv2gs_list) - cv2gs_length != nrow(cv2gs)) {
    diff = length(cv2gs_list) - (cv2gs_length + nrow(cv2gs))
    cat(paste(diff,"Ortho S results failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All Total Ig S results were successfully imported", fill = T)
  }
})

### CV2TN
cv2tn = subset(cil, is.na(cil$nc_interpretation) == F)
cv2tn = select(cv2tn, all_of(dbListFields(lc_conn, "cv2tn")))
class(cv2tn$nc_quant) = "numeric"
cv2tn$nc_quant = formatC(cv2tn$nc_quant, digits = 2, format = "f")
cv2tn_list = lc_conn %>%
  tbl("cv2tn") %>%
  pull("din")
cv2tn_length = length(cv2tn_list)

#  Accessioning the data with transaction
dbWithTransaction(lc_conn, {
  if (TRUE %in% (cv2tn$din %in% cv2tn_list)) {
    cat("Duplicate DIN found, subsetting for unique DINs", fill = T)
    cv2tn = subset(cv2tn, cv2tn$din %in% cv2tn_list == F)
    dbWriteTable(lc_conn, "cv2tn", cv2tn, append = TRUE)
  } else {
    cat("No duplicate records found. Attempting to import all results..", fill = T)
    dbWriteTable(lc_conn, "cv2tn", cv2tn[1,], append = TRUE)
  }
  # Check for if accessioning was successful
  cv2tn_list = lc_conn %>%
    tbl("cv2tn") %>%
    pull("din")
  # Rollback in case of error
  if (length(cv2tn_list) - cv2tn_length != nrow(cv2tn)) {
    diff = length(cv2tn_list) - (cv2tn_length + nrow(cv2tn))
    cat(paste(diff,"Ortho S results failed to be imported. Aborting..\n", sep = " "), fill = T)
    dbBreak()
  } else {
    cat("All Total Ig S results were successfully imported", fill = T)
  }
})

