visits <- as_tibble(DBI::dbReadTable(lc_conn, "vrdc_2022_visits"))

nc_results <- as_tibble(DBI::dbReadTable(lc_conn, "vrdc_2022_nc_total_ig_results"))

diag_tbl$pos_date = as.Date(glue("{diag_tbl$YYYY}-{diag_tbl$MM}-{diag_tbl$DD}"))

diag_tbl %>%
  filter(!is.na(test_date) & !is.na(test_type) &
           test_type %in% c('physician','antigen','naat/pcr','unsure')) %>%
  group_by(donor_did) %>%
  arrange(test_date) %>%
  slice(1) %>%
  ungroup() -> first_diagnoses

diag_tbl %>%
  filter(donor_did %in% first_diagnoses$donor_did &
           !is.na(test_date) &
           !is.na(test_type) &
           test_type %in% c('physician','antigen','naat/pcr','unsure')) %>%
  left_join(select(first_diagnoses, donor_did, first_diagnosis_date = test_date)) %>%
  group_by(donor_did) %>%
  arrange(test_date) %>%
  mutate(days_since_first = interval(first_diagnosis_date, test_date)/ddays(1)) %>%
  filter(days_since_first >= 90) %>%
  slice(1) %>%
  ungroup() -> second_diagnoses

first_diagnoses %>%
  filter(test_date >= ymd("2022-01-01")) %>%
  select(
    donor_did,
    first_sci_date = test_date,
    first_sci_test = test_type
  ) %>%
  left_join(
    second_diagnoses %>%
      filter(test_date >= ymd("2022-01-01")) %>%
      select(
        donor_did,
        second_sci_date = test_date,
        second_sci_test = test_type
      )
  ) -> diag_summary_postomicron


first_diagnoses %>%
  filter(test_date <= ymd("2021-11-30")) %>%
  select(
    donor_did,
    first_sci_date = test_date,
    first_sci_test = test_type
  ) %>%
  left_join(
    second_diagnoses %>%
      filter(test_date >= ymd("2022-01-01")) %>%
      select(
        donor_did,
        second_sci_date = test_date,
        second_sci_test = test_type
      )
  ) -> diag_summary_firstinf_preomicron


diag_summary_postomicron %>%
  filter(!is.na(second_sci_date)) %>%
  mutate(ri_flag = "both infections post-Omicron") -> ri_summary_both_postomicron

diag_summary_firstinf_preomicron %>%
  filter(!is.na(second_sci_date)) %>%
  mutate(ri_flag = "first infection pre-Omicron") -> ri_summary_firstinf_preomicron


donors <- as_tibble(DBI::dbReadTable(lc_conn, "vrdc_2022_donors"))
donors = dbReadTable(lc_conn, "donors") %>%
  select(
    donor_did,
    donor_id,
    udp_2022,
    vrdc_2022,
    vrdc_2023 = rdc_2023
  )

rbind(ri_summary_both_postomicron, ri_summary_firstinf_preomicron) %>%
  left_join(donors, by = "donor_did") -> ri_summary

ri_summary %>%
  group_by(udp_2022,
           vrdc_2022,
           vrdc_2023) %>%
  summarise(n = n())

vax_doses <- vacc_tbl

vax_doses %>%
  filter(!is.na(vacc_date)) %>%
  group_by(donor_did) %>%
  slice(1:2) %>%
  summarise(
    first_dose_date = first(vacc_date),
    fully_vaxed_date = case_when(
      !is.na(first(vacc_manufacturer)) & first(vacc_manufacturer) == "Johnson & Johnson" ~ first(vacc_date) + ddays(14),
      (is.na(first(vacc_manufacturer)) | first(vacc_manufacturer) != "Johnson & Johnson") & n() == 1 ~ NA_Date_,
      (is.na(first(vacc_manufacturer)) | first(vacc_manufacturer) != "Johnson & Johnson") & n() > 1 ~ nth(vacc_date, 2) + ddays(14)
    )
  ) -> vax_summary


# vax_summary %>%
# filter(!is.na(fully_vaxed_date)) %>%
# filter(fully_vaxed_date >= ymd("2020-12-15")) -> vaxed_after_midDec

ri_summary %>% left_join(vax_summary) -> ri_summary

ri_summary %>%
  filter(
    fully_vaxed_date >= ymd("2020-12-15") &
      first_sci_date >= fully_vaxed_date &
      fully_vaxed_date <= ymd("2023-01-01")
  ) %>%
  group_by(udp_2022,
           vrdc_2022,
           vrdc_2023) %>%
  summarise(n = n())


#Temp
ri_summary %>%
  filter(
    ri_flag == "both infections post-Omicron" &
      fully_vaxed_date >= ymd("2020-12-15") &
      fully_vaxed_date < first_sci_date &
      fully_vaxed_date <= ymd("2023-01-01")
  ) %>%
  mutate(
    priming_group = "Ancestral Vax Primed + Omicron first + Omicron RI"
  ) %>%
  select(donor_did, priming_group) -> grp1


ri_summary %>%
  left_join(grp1) -> ri_summary


ri_summary %>%
  filter(
    ri_flag == "both infections post-Omicron" & (
      is.na(first_dose_date) | (
        !is.na(first_dose_date) & first_sci_date < first_dose_date)
    )
  ) %>%
  mutate(
    priming_group = "Not vax primed + Omicron first + Omicron RI"
  ) %>%
  select(donor_did, priming_group) -> grp3

ri_summary %>%
  mutate(
    priming_group = case_when(
      !is.na(priming_group) ~ priming_group,
      is.na(priming_group) & donor_did %in% grp3$donor_did ~ "Not vax primed + Omicron first + Omicron RI",
      is.na(priming_group) & !(donor_did %in% grp3$donor_did) ~ NA_character_
    )
  ) -> ri_summary

ri_summary %>%
  group_by(priming_group,
           udp_2022,
           vrdc_2022,
           vrdc_2023) %>%
  summarise(n = n())


# ri_summary_preOmicron %>%
#   left_join(
#     donors %>%
#       select(
#         donor_did,
#         donor_id,
#         udp_2022,
#         vrdc_2022,
#         vrdc_2023 = rdc_2023
#       )
#   ) %>%
#   left_join(vax_summary) -> ri_summary_preOmicron

ri_summary %>%
  mutate(
    priming_group = case_when(
      !is.na(priming_group) ~ priming_group,
      is.na(priming_group) & ri_flag == "first infection pre-Omicron" &
        (is.na(first_dose_date) | first_dose_date > second_sci_date) ~ "Not vax primed + pre-Omicron first + Omicron RI"
    )
  ) -> ri_summary

ri_summary %>%
  group_by(priming_group,
           udp_2022,
           vrdc_2022,
           vrdc_2023) %>%
  summarise(n = n())

# ri_summary %>%
#   left_join(grp) -> ri_summary_preOmicron


ri_summary %>%
  mutate(
    priming_group = case_when(
      !is.na(priming_group) ~ priming_group,
      is.na(priming_group) ~ "Donor does not qualify"
    )
  ) -> ri_summary

ri_summary %>%
  group_by(priming_group,
           udp_2022,
           vrdc_2022,
           vrdc_2023) %>%
  summarise(n = n())
