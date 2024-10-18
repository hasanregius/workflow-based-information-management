# IV & INV Q3, Q4 2021 Selection

lc_conn = connect_db()
donors = dbReadTable(lc_conn, "donors")
visits = dbReadTable(lc_conn, "visits")
nc = dbReadTable(lc_conn, "nc_total_ig_results")

s_igg = dbReadTable(lc_conn, "cv2gs")

set.seed = 18274358263

# Subsetting
donations = donors %>%
  left_join(., visits, by = "donor_id") %>%
  filter(cohort %in% c("IV", "INV") &
          vrdc_2022 == T &
           between(collection_date, as.Date("2021-07-01"), as.Date("2021-12-31")) &
           sample_in_repository == T) %>%
  mutate(quarter = case_when(
    between(collection_date, as.Date("2021-07-01"), as.Date("2021-9-30")) ~ "q3",
    between(collection_date, as.Date("2021-10-01"), as.Date("2021-12-31")) ~ "q4"
  )) %>%
  group_by(donor_id, quarter) %>%
  slice_sample(n = 1)

nc %>% filter(nc_total_ig_test == "ortho") -> nc
s_igg %>% filter(final_result == T) -> s_igg

pull_list = donations %>%
  left_join(., nc, by = "din") %>%
  left_join(., s_igg, by = "din") %>%
  mutate(
    nc_testing = case_when(
      is.na(nc_interpretation) ~ T,
      nc_interpretation == "reactive" & nc_sco >= 100 ~ T,
      nc_interpretation == "reactive" & nc_sco < 100 ~ F,
      nc_interpretation == "non-reactive" ~ F),
    nc_dilutional = case_when(
      is.na(nc_interpretation) ~ "full",
      nc_interpretation == "reactive" & nc_sco >= 100 ~ "some",
      nc_testing == F ~ "none"),
    igg_testing = case_when(
      is.na(vitros_quant_s_igg_interpretation) ~ T,
      vitros_quant_s_igg_interpretation %in% c("reactive", "non-reactive") ~ F),
    igg_dilutions = case_when(
      is.na(vitros_quant_s_igg_quantification) ~ "full",
      vitros_quant_s_igg_quantification == "within_loq" ~ "none",
      vitros_quant_s_igg_quantification %in% c("below_loq", "above_loq") ~ "some"),
    ortho_tested = case_when(
      din %in% ortho_flag$din ~ T,
      din %in% ortho_flag$din == F ~ F)
  ) %>%
  select(donor_id, din, cohort, quarter, collection_date,
         nc_testing, nc_dilutional, igg_testing, igg_dilutions, ortho_tested)

summ = pull_list %>%
  group_by(quarter, cohort, nc_dilutional) %>%
  summarize(n = n()) %>%
  pivot_wider(names_from = nc_dilutional, names_prefix = "nc_test_", values_from = n)

summ2 = pull_list %>%
  group_by(quarter, cohort, igg_dilutions) %>%
  summarize(n = n()) %>%
  pivot_wider(names_from = igg_dilutions, names_prefix = "s_igg_test_", values_from = n)

summ = left_join(summ, summ2)


init_plist = read.csv(file.choose())
init_plist2 = read.csv(file.choose())
init_plist = rbind(init_plist, init_plist2)

init_plist %>%
  mutate(din = findin) %>%
  left_join(., visits, by = "din") %>%
  select(donor_id, cohort) %>%
  distinct() -> init_plist_donors

pull_list %>%
  ungroup() %>%
  select(donor_id, cohort) %>%
  distinct() -> new_plist_donors

plist_donors = rbind(init_plist_donors, new_plist_donors)

donations = donors %>%
  right_join(., visits, by = "donor_id") %>%
  filter(between(collection_date, as.Date("2021-07-01"), as.Date("2021-12-31")) &
           vrdc_2022 == T &
           donor_id %in% plist_donors$donor_id == F &
           sample_in_repository == T) %>%
  mutate(quarter = case_when(
    between(collection_date, as.Date("2021-07-01"), as.Date("2021-9-30")) ~ "q3",
    between(collection_date, as.Date("2021-10-01"), as.Date("2021-12-31")) ~ "q4"
  )) %>%
  group_by(donor_id, quarter) %>%
  slice_sample(n = 1)

pull_list2 = donations %>%
  left_join(., nc, by = "din") %>%
  left_join(., s_igg, by = "din") %>%
  mutate(
    nc_testing = case_when(
      is.na(nc_interpretation) ~ T,
      nc_interpretation == "reactive" & nc_sco >= 100 ~ T,
      nc_interpretation == "reactive" & nc_sco < 100 ~ F,
      nc_interpretation == "non-reactive" ~ F),
    nc_dilutional = case_when(
      is.na(nc_interpretation) ~ "full",
      nc_interpretation == "reactive" & nc_sco >= 100 ~ "some",
      nc_testing == F ~ "none"),
    igg_testing = case_when(
      is.na(vitros_quant_s_igg_interpretation) ~ T,
      vitros_quant_s_igg_interpretation %in% c("reactive", "non-reactive") ~ F),
    igg_dilutions = case_when(
      is.na(vitros_quant_s_igg_quantification) ~ "full",
      vitros_quant_s_igg_quantification == "within_loq" ~ "none",
      vitros_quant_s_igg_quantification %in% c("below_loq", "above_loq") ~ "some"),
    ortho_tested = case_when(
      din %in% ortho_flag$din ~ T,
      din %in% ortho_flag$din == F ~ F)
  ) %>%
  select(donor_id, din, cohort, quarter, collection_date,
         nc_testing, nc_dilutional, igg_testing, igg_dilutions)

summ_a = pull_list2 %>%
  group_by(quarter, cohort, nc_dilutional) %>%
  summarize(n = n()) %>%
  pivot_wider(names_from = nc_dilutional, names_prefix = "nc_test_", values_from = n)

summ2_a = pull_list2 %>%
  group_by(quarter, cohort, igg_dilutions) %>%
  summarize(n = n()) %>%
  pivot_wider(names_from = igg_dilutions, names_prefix = "s_igg_test_", values_from = n)

summ_a = left_join(summ_a, summ2_a)

write.csv("~/Vitalant - Vitalant Research Institute (VRI) - Vitalant Repeat Donor Cohorts - Vitalant Repeat Donor Cohorts/")
