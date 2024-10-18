# Substudy table
require(haven)

# CoP Case-Controls ----
# VI CoP case-control
vi_cop_cc_path = "~/Downloads/NBDC_Final_Data_2024-05-03/3_Analytic_Datasets/1_VI_Case_Control/vi_case_control_05022024.sas7bdat"
vi_cop_cc = read_sas(vi_cop_cc_path) %>%
  select(donor_id, din = DIN) %>%
  mutate(vi_cop_case_control = T) %>%
  distinct()

# RI CoP case-control
ri_cop_cc_path = "~/Downloads/NBDC_Final_Data_2024-05-03/3_Analytic_Datasets/2_RI_Case_Control/ri_case_control_05022024.sas7bdat"
ri_cop_cc = read_sas(ri_cop_cc_path) %>%
  select(donor_id, din = DIN) %>%
  mutate(ri_cop_case_control = T) %>%
  distinct()

# Marry the two
cop_cc = full_join(vi_cop_cc, ri_cop_cc, by = c("donor_id","din")) %>%
  mutate(
    ri_cop_case_control = case_when(
      is.na(ri_cop_case_control) ~ F,
      !is.na(ri_cop_case_control) ~ ri_cop_case_control),
    vi_cop_case_control = case_when(
      is.na(vi_cop_case_control) ~ F,
      !is.na(vi_cop_case_control) ~ vi_cop_case_control)
    )

# CoP 4.1
cop_ri_41 = read_sas(ri_cop_cc_path) %>%
  filter(msd_sample != 0) %>%
  select(donor_id, din = DIN) %>%
  mutate(cop_4.1 = T) %>%
  distinct()

cop_vi_41 = read_sas(vi_cop_cc_path) %>%
  filter(msd_sample != 0) %>%
  select(donor_id, din = DIN) %>%
  mutate(cop_4.1 = T) %>%
  distinct()

cop_4.1 = rbind(cop_ri_41, cop_vi_41) %>%
  distinct()

cop_4.2 = cop %>%
  left_join(., visits, by = "din") %>%
  select(donor_id, din) %>%
  mutate(cop_4.2 = T)

cop = full_join(cop_4.1, cop_4.2, by = c("donor_id", "din")) %>%
  mutate(
    cop_4.1 = case_when(
      is.na(cop_4.1) ~ F,
      !is.na(cop_4.1) ~ cop_4.1),
    cop_4.2 = case_when(
      is.na(cop_4.2) ~ F,
      !is.na(cop_4.2) ~ cop_4.2)
  )


# Longitudinal Case Series ----
# VI longitudinal case series
vi_long_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/3_Analytic_Datasets/VI_Cases_Longitudinal_Dynamics/vi_case_dynamics_06052024.sas7bdat"
vi_long_series = read_sas(vi_long_path) %>%
  select(donor_id, din) %>%
  mutate(vi_longitudinal_case_series = T) %>%
  distinct()

# RI longitudinal case series
ri_long_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/3_Analytic_Datasets/RI_Cases_Longitudinal_Dynamics/ri_case_dynamics_06052024.sas7bdat"
ri_long_series = read_sas(ri_long_path) %>%
  select(donor_id, din) %>%
  mutate(ri_longitudinal_case_series = T) %>%
  distinct()

long_case_series = full_join(vi_long_series, ri_long_series, by = c("donor_id","din")) %>%
  mutate(
    ri_longitudinal_case_series = case_when(
      is.na(ri_longitudinal_case_series) ~ F,
      !is.na(ri_longitudinal_case_series) ~ ri_longitudinal_case_series),
    vi_longitudinal_case_series = case_when(
      is.na(vi_longitudinal_case_series) ~ F,
      !is.na(vi_longitudinal_case_series) ~ vi_longitudinal_case_series)
  ) %>%
  distinct()

# Merge them all ---
substudy_dins = full_join(cop_cc, cop, by = c("donor_id", "din")) %>%
  full_join(., long_case_series, by = c("donor_id", "din")) %>%
  mutate(
    vi_cop_case_control = replace_na(vi_cop_case_control, F),
    ri_cop_case_control = replace_na(ri_cop_case_control, F),
    cop_4_1 = replace_na(cop_4.1, F),
    cop_4_2 = replace_na(cop_4.2, F),
    vi_longitudinal_case_series = replace_na(vi_longitudinal_case_series, F),
    ri_longitudinal_case_series = replace_na(ri_longitudinal_case_series, F)
  )

sb_conn = connect_db(db_name = "VRI_Sandbox")
sub_flist = dbListFields(sb_conn, "substudy_din_list")

substudy_dins = substudy_dins %>% select(all_of(sub_flist))

dbWriteTable(sb_conn, "substudy_din_list", substudy_dins, append = T)


# Check Vivian's donor IDs for missing MSD results
vivian = left_join(vivian, visits, by = "din")
vivian = vivian %>% select(donor_id, din_vivian = din, collection_date_vivian = collection_date)

msd = dbReadTable(sb_conn, "msd_multiplex") %>%
  left_join(., visits, by = "din") %>%
  filter(donor_id %in% vivian$donor_id) %>%
  select(donor_id, din, collection_date) %>%
  mutate(msd_tested = T) %>%
  distinct()

rvpn = dbReadTable(sb_conn, "vri_rvpn") %>%
  left_join(., visits, by = "din") %>%
  filter(donor_id %in% vivian$donor_id) %>%
  select(donor_id, din, collection_date) %>%
  mutate(rvpn_tested = T) %>%
  distinct()

msd_testing = full_join(msd, rvpn, by = c("donor_id","din","collection_date"))

vivian_feedback = vivian %>%
  left_join(., msd_testing, by = "donor_id")

