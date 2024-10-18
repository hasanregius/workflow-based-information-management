# Infection following vaccination case control matching
sb_conn = connect_db(db_name = "VRI_Sandbox")

# All donations with NC results
all_dons_w_nc = dbReadTable(sb_conn, "donors") %>%
  right_join(., dbReadTable(sb_conn, "visits"), by = "donor_id") %>%
  left_join(., dbReadTable(sb_conn, "nc_total_ig_results")) %>%
  select(-udp_2022, -vaxplas, -arc_pilot)

# Donations of interest all occurred between 1/1/21 and 12/31/22
donations = all_dons_w_nc %>%
  filter(between(collection_date, ymd("2021-01-01"), ymd("2022-12-31")))

# Pulling the donor survey data
survey = dbReadTable(sb_conn, "donor_survey_results")

# Generating the flag for whether they had negative NC value before 2021
pre_2021_nc = all_dons_w_nc %>%
  filter(between(collection_date, ymd("2020-07-01"), ymd("2021-06-30")))

# Date of first NC+ for each donor
donors = dbReadTable(sb_conn, "donors") %>% pull(donor_id)

# Date of infection from survey
survey_inf = survey %>%
  filter(str_detect(question, "inf"))
