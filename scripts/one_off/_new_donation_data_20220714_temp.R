#library(haven)
library(arrow)
library(tidyverse)
library(lubridate)
library(readxl)
projectdir = "~/dev/rdc_data_management/"
datadir = "~/data/vtl_rd_cohorts/"
setwd(projectdir)

# cohortdat = read_stata(paste0(datadir, "DatasetVRDC_uptoJune2022_forWestatplusothers.dta")) %>%
#   haven::as_factor()
#
# results_nc = read_parquet(paste0(datadir, "20220624_nc_results.parquet"))
# results_qigg = read_parquet(paste0(datadir, "20220624_quant_igg_results.parquet"))
#
# cohortdat %>%
#   mutate(din_original = din,
#          din = case_when(
#            str_length(din_original) == 7 ~ NA_character_,
#            str_length(din_original) != 7 ~ str_sub(din_original, 1, 13) # Should I be handling other potential lengths?
#          )
#   ) %>%
#   left_join(results_nc) %>%
#   left_join(results_qigg) -> cohortdat
#
# cohortdat %>%
#   write_parquet(paste0(datadir, "cohort_testing_data_20220715.parquet"))

# cohortdat = read_parquet(paste0(datadir, "cohort_testing_data_20220715.parquet"))
#
# # Consolidate all NC testing data
#
# cohortdat %>%
#   group_by(nc_interpretation) %>%
#   summarise(n = n())
#
cohortdat %>%
  group_by(COVID_8300, CVDA_RESULTS) %>%
  summarise(n = n()) %>%
  knitr::kable()

cohortdat %>%
  mutate(
    s_total_interpretation = case_when(
      # Prioritise CTS result
      CVDA_RESULTS == "N" | CVDA_RESULTS == "Non-reactive" ~ "non-reactive",
      CVDA_RESULTS == "P" | CVDA_RESULTS == "Reactive" ~ "reactive",
      COVID_8300 == "NEGATIVE" & (CVDA_RESULTS == "" | is.na(CVDA_RESULTS))~ "non-reactive",
      COVID_8300 == "POSITIVE" & (CVDA_RESULTS == "" | is.na(CVDA_RESULTS))~ "reactive"
    )
  ) -> cohortdat

cohortdat %>%
  mutate(
    s_total_sco = case_when(
      !is.na(CVDA_SCO) ~ as.numeric(CVDA_SCO),
      is.na(CVDA_SCO) ~ NA_real_
    )
  ) -> cohortdat

cohortdat %>%
  group_by(COVID_8310, ECOV_RESULTS, ASCV_RESULTS) %>%
  summarise(n = n()) %>%
  View(.)

cohortdat %>%
  mutate(
    nc_total_interpretation = case_when(
      ECOV_RESULTS == "POS" ~ "reactive",
      ECOV_RESULTS == "NEG" ~ "non-reactive",
      ASCV_RESULTS == "P" | ASCV_RESULTS == "HP" | ASCV_RESULTS == "LP" ~ "reactive",
      ASCV_RESULTS == "N" ~ "non-reactive",
      COVID_8310 == "POSITIVE" ~ "reactive",
      COVID_8310 == "NEGATIVE" ~ "non-reactive"
    ),
    nc_total_sco = case_when(
      !is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have only ecov
      !is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have both
      is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ASCV_SCO), # have only ascv
      is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ NA_real_ # have neither
    )
  ) -> cohortdat

# # Handle cases where both universal and cohort testing is present
#
# cohortdat %>%
#   rename(
#     nc_total_us_interpretation = nc_total_interpretation,
#     nc_total_us_sco = nc_total_sco,
#     nc_total_rdc_interpretation = nc_interpretation,
#     nc_total_rdc_sco = nc_quant
#     ) %>%
#   mutate(
#     nc_total_interpretation = case_when(
#       !is.na(nc_total_rdc_interpretation) ~ nc_total_rdc_interpretation,
#       is.na(nc_total_rdc_interpretation) & !is.na(nc_total_us_interpretation) ~ nc_total_us_interpretation,
#       is.na(nc_total_rdc_interpretation) & is.na(nc_total_us_interpretation) ~ NA_character_
#     ),
#     nc_total_sco = case_when(
#       !is.na(nc_total_rdc_sco) ~ nc_total_rdc_sco,
#       is.na(nc_total_rdc_sco) & !is.na(nc_total_us_sco) ~ nc_total_us_sco,
#       is.na(nc_total_rdc_sco) & is.na(nc_total_us_sco) ~ NA_real_
#     ),
#     nc_total_assay = case_when(
#       !is.na(nc_total_rdc_interpretation) | !is.na(nc_total_rdc_sco) ~ nc_test_name,
#       (is.na(nc_total_rdc_interpretation) & !is.na(nc_total_us_interpretation)) | ( is.na(nc_total_rdc_sco) & !is.na(nc_total_us_sco)) ~ "roche_total_ig_nc",
#       is.na(nc_total_rdc_interpretation) & is.na(nc_total_us_interpretation) & is.na(nc_total_rdc_sco) & is.na(nc_total_us_sco) ~ NA_character_
#     )
#   ) -> cohortdat
#
# cohortdat %>%
#   group_by(nc_total_us_interpretation, nc_total_rdc_interpretation, nc_total_interpretation) %>%
#   summarise(n = n())
#
# cohortdat %>%
#   write_parquet(paste0(datadir, "cohort_testing_data_20220722.parquet"))

cohortdat = read_parquet(paste0(datadir, "cohort_testing_data_20220722.parquet"))

# Look at mismatches between US and RDC testing results

cohortdat %>%
  filter(
    (nc_total_us_interpretation == "non-reactive" & nc_total_rdc_interpretation == "reactive") |
      (nc_total_us_interpretation == "reactive" & nc_total_rdc_interpretation == "non-reactive")
  ) %>%
  select(din, collection_date, nc_total_us_interpretation, nc_total_us_sco, nc_total_rdc_interpretation, nc_total_rdc_sco) %>%
  View(.)


### Check race and ethnicity

glimpse(cohortdat)

cohortdat %>%
  group_by(donor_id) %>%
  summarise(n_eth_codes = n_distinct(DONOR_ETHNICITY_CODE)) %>%
  View(.)

donors_w_mult_ethcodes = cohortdat %>%
  group_by(donor_id) %>%
  summarise(n_eth_codes = n_distinct(DONOR_ETHNICITY_CODE)) %>%
  filter(n_eth_codes > 1) %>%
  pull(donor_id)

cohortdat %>%
  filter(donor_id %in% donors_w_mult_ethcodes) %>%
  select(donor_id, din, collection_date, DONOR_ETHNICITY_CODE, DONOR_ETHNICITY_DESC, RACE_DESCRIPTION) %>%
  arrange(donor_id, collection_date) %>%
  View(.)


cohortdat %>%
  group_by(donor_id) %>%
  arrange(collection_date) %>%
  slice(n()) %>%
  ungroup() -> cohort_donors

cohort_donors %>%
  group_by(DONOR_ETHNICITY_CODE, DONOR_ETHNICITY_DESC, RACE_DESCRIPTION) %>%
  summarise(n = n()) %>%
  View(.)

cohort_donors %>%
  mutate(
    eth_code = case_when(
      is.na(DONOR_ETHNICITY_CODE) | DONOR_ETHNICITY_CODE == "" ~ NA_character_,
      !is.na(DONOR_ETHNICITY_CODE) & DONOR_ETHNICITY_CODE != "" ~ DONOR_ETHNICITY_CODE
    ),
    eth_desc = case_when(
      is.na(DONOR_ETHNICITY_DESC) | DONOR_ETHNICITY_DESC == "" ~ NA_character_,
      !is.na(DONOR_ETHNICITY_DESC) & DONOR_ETHNICITY_DESC != "" ~ DONOR_ETHNICITY_DESC
    ),
    race_desc = case_when(
      is.na(RACE_DESCRIPTION) | RACE_DESCRIPTION == "" ~ NA_character_,
      !is.na(RACE_DESCRIPTION) & RACE_DESCRIPTION != "" ~ RACE_DESCRIPTION
    ),
    eth_desc_tosplit = eth_desc
  ) %>%
  separate(col = eth_desc_tosplit, into = c("eth_desc1", "eth_desc2"), sep = " - ") -> cohort_donors

cohort_donors %>%
  group_by(eth_code, eth_desc1, eth_desc2, race_desc) %>%
  summarise(n = n()) -> unique_combinations

unique_combinations %>%
  write_csv("unique_ethnicity_combinations.csv")


cohort_donors %>%
  group_by(eth_code, eth_desc1, eth_desc2, race_desc) %>%
  summarise(n = n()) %>%
  group_by(eth_code) %>%
  summarise(n_unique_combinations = n())




### Selection of Q2 samples for testing

cts_inventory_old = read_csv(paste0(datadir, "VRI SF_CTS RDC Inventory_FW export_042822.csv")) %>%
  mutate(
    sample_label = din,
    din = str_sub(sample_label, 2, 14),
    collection_date = mdy(`Collection Date`),
    site = Client,
    n_aliquots = as.integer(`Aliquot #`),
    sample_type = `Aliquot Sample Type`,
    volume = `Current Volume`
  ) %>%
  select(sample_label, din, collection_date, site, n_aliquots, sample_type, volume)

cts_inventory_new = read_csv(paste0(datadir, "VRI SF_CTS RDC Inventory_FW export_010122 to 071222.csv")) %>%
  mutate(
    sample_label = `ISBT DIN`,
    din = str_sub(sample_label, 2, 14),
    collection_date = mdy(`Collection Date`),
    site = Site,
    n_aliquots = as.integer(`Aliquot #`),
    sample_type = `Sample Type Aliquot`,
    volume = `Current Volume`
  ) %>%
  select(sample_label, din, collection_date, site, n_aliquots, sample_type, volume)

cts_inventory = cts_inventory_old %>%
  filter( !(din %in% cts_inventory_new$din) ) %>%
  bind_rows(cts_inventory_new)

cts_inventory_sum = cts_inventory %>%
  mutate(captured = TRUE) %>%
  select(din, captured) %>%
  distinct()

cohortdat %>%
  left_join(cts_inventory_sum) %>%
  mutate(
    in_repository = case_when(
      is.na(captured) ~ FALSE,
      !is.na(captured) & !captured ~ FALSE, #Won't happen
      !is.na(captured) & captured ~ TRUE
    )
  ) -> cohortdat

cohortdat %>%
  filter(
    collection_date >= ymd("2022-04-01"),
    collection_date <= ymd("2022-06-30"),
    !is.na(din), # new way
    str_length(din) == 13, # old way
    in_repository == TRUE,
    !is.na(cohort)
  ) -> selectionset

selectionset %>%
  group_by(donor_id) %>%
  summarise(n_donations = n()) %>%
  group_by(n_donations) %>%
  summarise(n_donors = n()) %>%
  knitr::kable()

set.seed(32775325)
selectionset %>%
  group_by(donor_id) %>%
  sample_n(1, replace = FALSE) %>%
  ungroup() -> selected

selected %>%
  group_by(donor_id) %>%
  summarise(n_donations = n()) %>%
  group_by(n_donations) %>%
  summarise(n_donors = n())

selected %>%
  mutate(
    Ortho_Quant_IgG = "YES",
    Ortho_NC = "YES",
    donation_date_str = format(collection_date, "%m/%d/%Y")
  ) %>%
  select(
    din,
    cohort,
    donation_date = donation_date_str,
    Ortho_Quant_IgG,
    Ortho_NC
  ) -> test_order

test_order %>%
  write_csv(paste0(datadir, "20220716_VTL_Q2_2022_CTS_testing_order.csv"))

## End Q2 Vitalant Testing

## Request NC testing on Q1 samples ##

q1_testing_order = read_csv(paste0(datadir, "20220429_VTL_Q12022_CTS_testing_order.csv"))

q1_supplemental_order = q1_testing_order %>%
  mutate(
    Ortho_NC = case_when(
      Ortho_NC == "YES" ~ "",
      is.na(Ortho_NC) | Ortho_NC == "" ~ "YES"
    ),
    Ortho_Quant_IgG = ""
  )

q1_supplemental_order %>%
  write_csv(paste0(datadir, "20220718_VTL_Q12022_CTS_supplemental_testing_order.csv"))










# Request additional testing on vaxplasma donors

q1_order = read_csv("~/data/vtl_rd_cohorts/20220429_VTL_Q12022_CTS_testing_order.csv")
q1_supp = read_csv("~/data/vtl_rd_cohorts/20220718_VTL_Q12022_CTS_supplemental_testing_order.csv")
q2_order = read_csv("~/data/vtl_rd_cohorts/20220716_VTL_Q2_2022_CTS_testing_order.csv")
vaxplas1 = read_excel("~/Vitalant/Research Operations Core - Vaxplasma study/VaxPlasGrp1_n30_RAW results  061722.xlsx", sheet = 1)
vaxplas2asymp = read_excel("~/Vitalant/Research Operations Core - Vaxplasma study/VaxPlasGroup 2 Asmptomatic for CTS select MSD RVPN 061722.xlsx", sheet = 1)
vaxplas2symp = read_excel("~/Vitalant/Research Operations Core - Vaxplasma study/VaxPlasGroup 2 Symptomatic_RAWselected MSD RVPN 061722.xlsx", sheet = 1)
vaxplas3 = read_excel("~/Vitalant/Research Operations Core - Vaxplasma study/VaxPlasGrp3_n30_RAW results 061722.xlsx", sheet = 1)

glimpse(vaxplas1)

vaxplas1 %>%
  select(donor_id = DONOR_NUMBER,
         donation_date) %>%
  mutate(group = "Group 1",
         donation_date = as_date(donation_date)) %>%
  distinct() %>%
  bind_rows(
    vaxplas2asymp %>%
      select(
        donor_id = DONOR_NUMBER,
        donation_date
      ) %>%
      mutate(
        group = "Group 2 Asymptomatic",
        donation_date = as_date(donation_date)
      )
  ) %>%
  bind_rows(
    vaxplas2symp %>%
      select(
        donor_id = DONOR_NUMBER,
        donation_date
      ) %>%
      mutate(
        group = "Group 2 Symptomatic",
        donation_date = as_date(donation_date)
      )
  ) %>%
  bind_rows(
    vaxplas3 %>%
      select(
        donor_id = DONOR_NUMBER,
        donation_date
      ) %>%
      mutate(
        group = "Group 3",
        donation_date = as_date(donation_date)
      )
  ) -> vaxplas_donations








# Bizarre source fields!
cohortdat %>%
  group_by(COVID_8300) %>%
  summarise(n = n())
cohortdat %>%
  group_by(COVID_8310) %>%
  summarise(n = n())
cohortdat %>%
  group_by(COVID_8315) %>%
  summarise(n = n())
cohortdat %>%
  group_by(COVID_8305) %>%
  summarise(n = n())
cohortdat %>%
  group_by(COVID_8320) %>%
  summarise(n = n())
cohortdat %>%
  group_by(CVDA_RESULTS) %>%
  summarise(n = n())
cohortdat %>%
  group_by(CVDA_SCO) %>%
  summarise(n = n())
cohortdat %>%
  group_by(CVDG_RESULTS) %>%
  summarise(n = n())
cohortdat %>%
  group_by(CVDG_SCO) %>%
  summarise(n = n())
cohortdat %>%
  group_by(CVDG_RESULTS) %>%
  summarise(n = n())
cohortdat %>%
  group_by(ECOV_RESULTS) %>%
  summarise(n = n())
cohortdat %>%
  group_by(ECOV_SCO) %>%
  summarise(n = n())
cohortdat %>%
  group_by(ASCV_RESULTS) %>%
  summarise(n = n())
cohortdat %>%
  group_by(ASCV_SCO) %>%
  summarise(n = n())
