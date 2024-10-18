# Dependencies ----
require(haven)
require(glue)
require(lubridate)
require(readxl)
require(tidyverse)

# Connections
lc_conn = connect_db()
sb_conn = connect_db(db_name = "VRI_Sandbox")

# Donor and VTL survey linkage tables ----
# ARC Donors
donors_tbl_fnames = dbListFields(sb_conn, "donors")
rdc_arc_donors = dbReadTable(lc_conn, Id(schema = "arc", table = "donors"))
donor_file = read_sas("~/Downloads/NBDC_Final_Data_2024-05-03/1_Individual_Datasets/1_Donor_File/donor_file_05012024.sas7bdat")
donor_file = read_sas("~/Downloads/NBDC_Final_Data_2024-05-09/1_Individual_Datasets/1_Donor_File/Donor_File_05072024.sas7bdat")
donor_file = read_sas("~/Downloads/RDC_2023_Final_Data_2024-06-12/1_Individual_Datasets/1_Donor_File/Donor_File_05092024.sas7bdat")

arc_2023_rdc = read_xlsx("~/Desktop/ARC 2023_COVIDCohort_DID_List_01112023.xlsx") %>%
  mutate(donor_id = paste0("ARC", DonorID))
donor_reformat = donor_file %>%
  mutate(mob = as.Date("2021-07-01") %m-% months(age_in_months),
         donor_did = NA_real_,
         udp_2022 = 0,
         vaxplas = 0,
         vrdc_2022 = 1,
         rdc_2023 = case_when(
           donor_id %in% arc_2023_rdc$donor_id ~ 1,
           donor_id %in% arc_2023_rdc$donor_id == F ~ 0),
         abo_group = tolower(abo_blood_group),
         arc_pilot = case_when(
           donor_id %in% rdc_arc_donors$donor_id[rdc_arc_donors$pilot_study == T] ~ 1,
           donor_id %in% rdc_arc_donors$donor_id[rdc_arc_donors$pilot_study == T] == F ~ 0),
         rh_group = rh_factor
         ) %>%
  select(all_of(append(donors_tbl_fnames, "donor_did")))

# VTL Donors
vtl_donors = dbReadTable(lc_conn, "donors") %>%
  mutate(bco = "VTL",
         arc_pilot = 0) %>%
  select(all_of(append(donors_tbl_fnames, "donor_did")))

# Merge the two tables
merged_donors = rbind(vtl_donors, donor_reformat) %>%
  distinct()

# Create VTL survey linkage table
vtl_survey_linkage = merged_donors %>%
  filter(bco == "VTL" & !is.na(donor_did)) %>%
  select(donor_id, donor_did)

# Accession donors to the donors table
merged_donors = merged_donors %>%
  mutate(
    abo_group = na_if(abo_group, "unavailable"),
    rh_group = na_if(rh_group, "unavailable"),
    rh_group = na_if(rh_group, "unknown"),
  ) %>%
  select(-donor_did)

donors_db = dbReadTable(sb_conn, "donors")
merged_donors %>% filter(donor_id %in% donors_db$donor_id == F) -> merged_donors
dbWriteTable(sb_conn, "donors", merged_donors, append = T)

# Accession VTL survey linkage table
dbWriteTable(sb_conn, "vtl_survey_donor_linkage", vtl_survey_linkage, append = T)
vtl_survey_linkage_db = dbReadTable(sb_conn, "vtl_survey_donor_linkage")
vtl_survey_linkage = vtl_survey_linkage %>% filter(donor_id %in% vtl_survey_linkage_db$donor_id == F)

# Race reference table ----
new_raceref = read.csv("~/Desktop/ARC_edw_raceth_definitions.csv") %>%
  select(-edw_eth_desc)
dbWriteTable(sb_conn, "race_ref", new_raceref, append = T)

# Visits table ----
visits_fnames = dbListFields(sb_conn, "visits")

# ARC Visits
visits_dir = "~/Downloads/NBDC_Final_Data_2024-05-03/1_Individual_Datasets/2_Donation_File/donation_file_05012024.sas7bdat"
visits_file = read_sas(visits_dir) %>%
  mutate(map_division = NA,
         location = NA,
         map_region = NA,
         age_at_donation = age_in_months,
         sample_in_repository = sample_status,
         race = case_when(
           race == "hispanic_ethnicity" ~ "unavailable",
           race != "hispanic_ethnicity" ~ race),
         edw_eth_code = case_when(
           race == "asian" ~ '1001',
           race == "american_indian" ~ '1002',
           race == "black" ~ '1003',
           race == "white" ~ '1004',
           race == "other" ~ '1005',
           race == "more_than_one" ~ '1006',
           race == "unavailable" ~ '1007'),
         edw_race_desc = case_when(
           ethnicity_hispanic == "unavailable" ~ "UNAVAILABLE",
           ethnicity_hispanic == "true" ~ "HISPANIC",
           ethnicity_hispanic == "false" ~ "NON-HISPANIC"),
         phlebotomy_status = 1,
         zip_code = str_pad(zip_code, width = 5, pad = "0", side = "left"),
         zip_code = na_if(zip_code, '99999')) %>%
  select(all_of(visits_fnames)) %>%
  filter(substr(donor_id,1,3) == "ARC")

visits_dir2 = "~/Downloads/RDC_2023_Final_Data_2024-06-12/1_Individual_Datasets/2_Donation_File/donation_file_2023_05132024.sas7bdat"
visits_file2 = read_sas(visits_dir2) %>%
  mutate(map_division = NA,
         location = NA,
         map_region = NA,
         age_at_donation = age_in_months,
         sample_in_repository = sample_status,
         race = case_when(
           race == "hispanic_ethnicity" ~ "unavailable",
           race != "hispanic_ethnicity" ~ race),
         edw_eth_code = case_when(
           race == "asian" ~ '1001',
           race == "american_indian" ~ '1002',
           race == "black" ~ '1003',
           race == "white" ~ '1004',
           race == "other" ~ '1005',
           race == "more_than_one" ~ '1006',
           race == "unavailable" ~ '1007'),
         edw_race_desc = case_when(
           ethnicity_hispanic == "unavailable" ~ "UNAVAILABLE",
           ethnicity_hispanic == "true" ~ "HISPANIC",
           ethnicity_hispanic == "false" ~ "NON-HISPANIC"),
         phlebotomy_status = 1,
         zip_code = str_pad(zip_code, width = 5, pad = "0", side = "left"),
         zip_code = na_if(zip_code, '99999')) %>%
  select(all_of(visits_fnames)) %>%
  filter(substr(donor_id,1,3) == "ARC")

visits_file = rbind(visits_file, visits_file2) %>%
  distinct()

# VTL Visits
ori_raceref = dbReadTable(lc_conn, "race_ref")
vtl_visits = dbReadTable(lc_conn, "visits") %>%
  left_join(., ori_raceref, by = c("edw_eth_code", "edw_race_desc")) %>%
  select(all_of(visits_fnames))

# Merge both tables
merged_visits = rbind(vtl_visits, visits_file)
merged_visits = merged_visits %>%
  mutate(
    dhq_vaccination_status_donation = case_when(
      dhq_vaccination_status_donation %in% c("TRUE", "vaccinated") ~  T,
      dhq_vaccination_status_donation %in% c("FALSE", "not_vaccinated") ~ F),
    dhq_vaccination_status_ever = case_when(
      dhq_vaccination_status_ever %in% c("TRUE", "vaccinated") ~ T,
      dhq_vaccination_status_ever %in% c("FALSE", "not_vaccinated") ~ F),
    zip_code = str_pad(zip_code, width = 5, pad = "0", side = "left"),
    ethnicity_hispanic = case_when(
      toupper(ethnicity_hispanic) == "TRUE" ~ T,
      toupper(ethnicity_hispanic) == "FALSE" ~ F),
    sample_in_repository = case_when(
      sample_in_repository %in% c("available", "TRUE") ~ T,
      sample_in_repository %in% c("unavailable", "FALSE") ~ F)
  ) %>% filter(donor_id %in% donors_db$donor_id)

visits_db = dbReadTable(sb_conn, "visits")
merged_visits = merged_visits %>%
  filter(din %in% visits_db$din == F)

# ARC input file
visits_input = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/2_Donation_Files/ARC/ARC_donation_linelist_20230215_Updated.csv"
input_arc = read.csv(visits_input)

# Accession data into the database
dbWriteTable(sb_conn, "visits", merged_visits, append = T)
dbWriteTable(sb_conn, "visits", merged_visits[1000001:2000000,], append = T)
dbWriteTable(sb_conn, "visits", merged_visits[2000001:3000000,], append = T)
dbWriteTable(sb_conn, "visits", merged_visits[3000001:nrow(merged_visits),], append = T)

# Samples table ----
samples_fnames = dbListFields(sb_conn, "samples")
arc_samples = dbReadTable(lc_conn, Id(schema = "arc", table = "specimens")) %>%
  left_join(., visits_file, by = "din") %>%
  select(din, sample_status = sample_in_repository, aliquot_type, aliquot_anticoagulant)
vtl_samples = dbReadTable(lc_conn, "samples") %>%
  select(all_of(samples_fnames))
merged_samples = rbind(vtl_samples, arc_samples)
dbWriteTable(sb_conn, "samples", merged_samples, append = T)
samples_db = dbReadTable(sb_conn, "samples")
merged_samples = merged_samples %>%
  filter(din %in% samples_db$din == F)

samples_file = read_sas(visits_dir2) %>%
  filter(din %in% merged_samples$din == F &
           din %in% merged_samples$din == F &
           din %in% visits_db$din) %>%
  select(din, sample_status) %>%
  mutate(aliquot_type = NA,
         aliquot_anticoagulant = NA)

dbWriteTable(sb_conn, "samples", samples_file, append = T)

samples_db = dbReadTable(sb_conn, "samples")
samples_file %>% filter(din %in% samples_db$din == F) -> samples_file

arc_summ = arc_check %>%
  group_by(year(collection_date)) %>%
  summarize(n = n())

# Ortho Total Ig S table ----
cv2ts = dbReadTable(lc_conn, "cv2ts")
dbWriteTable(sb_conn, "cv2ts", cv2ts, append = T)

# Ortho IgG S table ----
arc_igg = dbReadTable(lc_conn, Id(schema = "arc", table = "cv2gs")) %>%
  mutate(cv2gs_study_source = "rdc")
vtl_igg = dbReadTable(lc_conn, "cv2gs")

igg_db = dbReadTable(sb_conn, "cv2gs")
merged_igg = rbind(arc_igg, vtl_igg) %>%
  filter(din %in% igg_db$din == F & din %in% samples_db$din &
           vitros_quant_s_igg_quantification %in% c("invalid","not_tested") == F) %>%
  mutate(vitros_quant_s_igg_quantification = tolower(vitros_quant_s_igg_quantification))
dbWriteTable(sb_conn, "cv2gs", merged_igg, append = T)

# Ortho NC table ----
arc_nc = dbReadTable(lc_conn, Id(schema = "arc", table = "cv2tn")) %>%
  mutate(nc_total_ig_test = "ortho") %>%
  select(din, nc_total_ig_test, nc_total_ig_test_date, nc_sco = nc_quant, nc_interpretation,
         nc_total_ig_reagent_lot, nc_dilution_factor, nc_final_result, nc_study_source)
vtl_nc = dbReadTable(lc_conn, "nc_total_ig_results")

nc_db = dbReadTable(sb_conn, "nc_total_ig_results")
merged_nc = rbind(arc_nc, vtl_nc) %>%
  filter(din %in% nc_db$din == F & din %in% samples_db$din) %>%
  distinct()
dbWriteTable(sb_conn, "nc_total_ig_results", merged_nc, append = T)



# Survey questions ----
survey_q = dbReadTable(lc_conn, "survey_questions")
