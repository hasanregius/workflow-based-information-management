# Sample retention
fw = read_csv("~/Desktop/rdc_sample_retention_20240827.csv")
vaxplas = readxl::read_xlsx("~/Desktop/vaxplas.xlsx", sheet = 1)

# Retain samples only from 2023 cohort donors who donated (at least once) in 2023
sb_conn = connect_db()
donors_db = dbReadTable(sb_conn, "donors")
visits_db = dbReadTable(sb_conn, "visits")

donor_list = donors_db %>%
  right_join(., visits_db, by = "donor_id") %>%
  filter(rdc_2023 == T & between(collection_date, ymd("2023-01-01"), ymd("2023-12-31"))) %>%
  pull(donor_id) %>%
  unique(.)

fw$donated_in_2023 = F
fw$donated_in_2023[fw$donor_id %in% donor_list] = T

# Retain the following samples:
# - 2023 quarterly tested donations
# - 2022 quarterly tested donations
# - 2021 Q3 and Q4 tested samples
nc_db = dbReadTable(sb_conn, "nc_total_ig_results")
igg_db = dbReadTable(sb_conn, "cv2gs")

tested = visits_db %>%
  inner_join(., igg_db, by = "din") %>%
  left_join(., nc_db, by = "din") %>%
  filter(donor_id %in% donor_list) %>%
  pull(din) %>%
  unique(.)

fw$tested = F
fw$tested[fw$din %in% tested] = T
fw$rdc_2023_cohort = F
fw$rdc_2023_cohort[fw$donor_id %in% donors_db$donor_id[donors_db$rdc_2023]] = T

# if donor has no Q3 and Q4 2021 tested samples -> one sample from Q4 or if none from Q3 for retention
# Check for donor with tested 2021 Q3/Q4
tested_q4_2021 = visits_db %>%
  inner_join(., igg_db, by = "din") %>%
  left_join(., nc_db, by = "din") %>%
  filter(din %in% tested & between(collection_date, ymd("2021-10-01"), ymd("2021-12-31")))

tested_q3_2021 = visits_db %>%
  inner_join(., igg_db, by = "din") %>%
  left_join(., nc_db, by = "din") %>%
  filter(din %in% tested & between(collection_date, ymd("2021-07-01"), ymd("2021-09-30")))

# For untested samples, if performing sample selection and donor has > 1 sample in a quarter, retain the higher volume sample

untested_q4_2021 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
         donor_id %in% tested_q4_2021$donor_id == F &
         (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5) &
         between(collection_date, ymd("2021-10-01"), ymd("2021-12-31"))) %>%
  group_by(donor_id) %>%
  slice_max(collection_date, n = 1) %>%
  ungroup()

untested_q3_2021 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           donor_id %in% tested_q3_2021$donor_id == F &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5) &
           between(collection_date, ymd("2021-07-01"), ymd("2021-09-30"))) %>%
  group_by(donor_id) %>%
  slice_max(collection_date, n = 1) %>%
  ungroup()

untested_q2_2021 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
         din %in% tested == F &
         between(collection_date, ymd("2021-04-01"), ymd("2021-06-30")) &
         (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() %>%
  filter(!duplicated(donor_id))

untested_q1_2021 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           din %in% tested == F &
           between(collection_date, ymd("2021-01-01"), ymd("2021-03-31")) &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() %>%
  filter(!duplicated(donor_id))

untested_q4_2020 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           din %in% tested == F &
           between(collection_date, ymd("2020-10-01"), ymd("2020-12-31")) &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() %>%
  filter(!duplicated(donor_id))

untested_q3_2020 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           din %in% tested == F &
           between(collection_date, ymd("2020-07-01"), ymd("2020-09-30")) &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() %>%
  filter(!duplicated(donor_id))

untested_q2_2020 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           din %in% tested == F &
           between(collection_date, ymd("2020-04-01"), ymd("2020-06-30")) &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() %>%
  filter(!duplicated(donor_id))

untested_q1_2020 = visits_db %>%
  inner_join(., fw, by = c("din", "donor_id", "collection_date")) %>%
  filter(donor_id %in% donor_list &
           din %in% tested == F &
           between(collection_date, ymd("2020-01-01"), ymd("2020-03-31")) &
           (total_serum_volume_ml >= 0.5 | total_plasma_volume_ml >= 0.5)) %>%
  mutate(total_volume = total_serum_volume_ml + total_plasma_volume_ml) %>%
  group_by(donor_id) %>%
  slice_max(total_volume, n = 1) %>%
  ungroup() # NONE

fw$selected = F
# Add definition of tested with any test result, Roche NC and S total Ig included
fw$selected[fw$din %in% tested] = T
fw$selected[fw$din %in% tested_q3_2021$din] = T
fw$selected[fw$din %in% tested_q4_2021$din] = T
fw$selected[fw$din %in% untested_q3_2021$din] = T
fw$selected[fw$din %in% untested_q4_2021$din] = T
fw$selected[fw$din %in% untested_q2_2021$din] = T
fw$selected[fw$din %in% untested_q1_2021$din] = T
fw$selected[fw$din %in% untested_q4_2020$din] = T
fw$selected[fw$din %in% untested_q3_2020$din] = T
fw$selected[fw$din %in% untested_q2_2020$din] = T
fw$selected[fw$din %in% untested_q1_2020$din] = T
fw$selected[fw$din %in% mispa$din] = T
fw$selected[fw$din %in% vaxplas$din] = T
fw$selected[fw$in_substudy] = T
fw$in_substudy[fw$din %in% mispa$din] = T
fw$in_substudy[fw$din %in% vaxplas$din] = T
# Fix the flags, before printing order by donor id and collection date

# Find the donor ids for those in a substudy
donor_list_2 = unique(fw$donor_id[fw$in_substudy == T])
fw$in_substudy[fw$donor_id %in% donor_list_2] = T


# Prefer serum aliquots
# if < 0.5 mL serum, keep both plasma and serum aliquots
fw$aliquot_type_selected = NA
fw$aliquot_type_selected[fw$total_serum_volume_ml >= 0.5] = "serum"
fw$aliquot_type_selected[fw$total_serum_volume_ml < 0.5] = "serum and plasma"
fw$aliquot_type_selected[fw$selected == F] = "none"

fw_final = fw %>%
  select(din, donor_id, collection_date, plasma_aliquot_count, total_plasma_volume_ml,
         serum_aliquot_count, total_serum_volume_ml, rdc_2023_cohort, donated_in_2023, tested, in_substudy,
         selected, aliquot_type_selected) %>%
  arrange(donor_id, collection_date)

write.csv(fw_final, "~/Desktop/rdc_sample_retention_vri_20240903_2.csv", row.names = F)

fw_cts = fw %>%
  filter(selected == T) %>%
  select(din, aliquot_type_selected)

write.csv(fw_cts, "~/Desktop/rdc_sample_retention_cts_20240903_2.csv", row.names = F)
