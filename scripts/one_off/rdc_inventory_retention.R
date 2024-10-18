# Open the FW exports
inventory = readxl::read_xlsx("~/Downloads/cts_fw/RDC VTL 2022 FW Export.xlsx", sheet = "Available") %>%
  select(din = `LT Sample ID`,
         aliquot_id = `Globally Unique Aliquot ID`,
         aliquot_type = `Sample Type Aliquots`,
         volume = `Current Amount`) %>%
  mutate(aliquot_type = tolower(aliquot_type),
         aliquot_type = case_when(
           str_detect(aliquot_type, "edta") ~ "plasma",
           str_detect(aliquot_type, "serum") ~ "serum"))

inventory_2021 = readxl::read_xlsx("~/Downloads/cts_fw/VTL RDC 2021 FW Export w Sample Status.xlsx") %>%
  select(din = `LT Sample ID`,
         aliquot_id = `Globally Unique Aliquot ID`,
         aliquot_type = `Sample Type Aliquots`,
         volume = `Current Amount`) %>%
  mutate(aliquot_type = tolower(aliquot_type),
         aliquot_type = case_when(
           str_detect(aliquot_type, "edta") ~ "plasma",
           str_detect(aliquot_type, "serum") ~ "serum"))

inventory_2020 = readxl::read_xlsx("~/Downloads/cts_fw/VTL RDC 2020 FW Export.xlsx") %>%
  select(din = `LT Sample ID`,
         aliquot_id = `Globally Unique Aliquot ID`,
         aliquot_type = `Sample Type Aliquots`,
         volume = `Current Amount`) %>%
  mutate(aliquot_type = tolower(aliquot_type),
         aliquot_type = case_when(
           str_detect(aliquot_type, "edta") ~ "plasma",
           str_detect(aliquot_type, "serum") ~ "serum"))

inventory = rbind(inventory_2020, inventory_2021) %>%
  rbind(., inventory)

# Get a tally of plasma and serum per din
summ = inventory %>%
  group_by(din, aliquot_type) %>%
  summarize(n = n(),
            total_volume_ml = sum(as.numeric(volume)))

summ_count = summ %>%
  pivot_wider(id_cols = din,
              names_from = aliquot_type,
              values_from = n) %>%
  rename(plasma_aliquot_count = plasma,
         serum_aliquot_count = serum) %>%
  mutate(plasma_aliquot_count = replace_na(plasma_aliquot_count, 0),
         serum_aliquot_count = replace_na(serum_aliquot_count, 0))

summ_volume = summ %>%
  pivot_wider(id_cols = din,
              names_from = aliquot_type,
              values_from = total_volume_ml) %>%
  rename(total_plasma_volume_ml = plasma,
         total_serum_volume_ml = serum) %>%
  mutate(total_plasma_volume_ml = replace_na(total_plasma_volume_ml, 0),
         total_serum_volume_ml = replace_na(total_serum_volume_ml, 0))

summ = left_join(summ_count, summ_volume, by = "din") %>%
  select(din, plasma_aliquot_count, total_plasma_volume_ml, serum_aliquot_count, total_serum_volume_ml)

# Restrict to the quarterly tested donation DINs, which only applies to 2022 and earlier samples
sb_conn = connect_db(db_name = "VRI_Sandbox")
donors = dbReadTable(sb_conn, "donors")
visits = dbReadTable(sb_conn, "visits")
nc = dbReadTable(sb_conn, "nc_total_ig_results")
nc_qtr = nc %>% filter(nc_study_source == "rdc")
igg = dbReadTable(sb_conn, "cv2gs")
igg_qtr = igg %>% filter(cv2gs_study_source == "rdc")
substudy_dins = dbReadTable(sb_conn, "substudy_din_list")
survey_respondent_2023_donors = dbReadTable(sb_conn, "survey_results") %>%
  filter(grepl("cy*", round)) %>%
  pull(donor_id)

# Get the collection dates and check if tested
inventory = summ %>% inner_join(., visits, by = "din")
inventory$tested = NA
inventory$tested[inventory$din %in% nc$din & inventory$din %in% igg$din] = T
inventory$tested[is.na(inventory$tested)] = F
inventory$in_substudy = F
inventory$in_substudy[inventory$din %in% substudy_dins$din] = T

inventory = inventory %>%
  select(din,
         donor_id,
         collection_date,
         plasma_aliquot_count,
         total_plasma_volume_ml,
         serum_aliquot_count,
         total_serum_volume_ml,
         tested,
         in_substudy)

inventory$survey_respondent_2023 = NA
inventory$survey_respondent_2023[inventory$donor_id %in% survey_respondent_2023_donors] = T
inventory$survey_respondent_2023[inventory$donor_id %in% survey_respondent_2023_donors == F] = F

# Get the complicated selection for 2021
inventory$selected = NA
dnrs = unique(inventory$donor_id)
for (i in 1:length(dnrs)) {
  cat(i, fill = T)
  sbst_22 = subset(inventory, inventory$donor_id == dnrs[i] & inventory$tested == T & year(inventory$collection_date) == 2022)
  if (nrow(sbst_22) > 0) {
    if (T %in% sbst_22$tested) {
      sbst_21 = subset(inventory, inventory$donor_id == dnrs[i] & inventory$tested == F & year(inventory$collection_date) == 2021)
      if (nrow(sbst_21) > 0) {
        selected_din = sbst_21 %>% slice_max(total_serum_volume_ml, n = 1) %>% pull(din)
        if (length(selected_din) == 0) {
          selected_din = sbst_21 %>% slice_max(total_serum_volume_ml, n = 1) %>% pull(din)
        }
        inventory$selected[inventory$din == selected_din] = T
      }
    }
  }
}

inventory_rdc_23 = inventory %>% filter(survey_respondent_2023 & donated_in_2023)
inventory$selected[inventory$donor_id %in% inventory_rdc_23$donor_id | inventory$in_substudy] = F

visits_2023 = visits %>%
  filter(year(collection_date) == "2023")

inventory$donated_in_2023 = NA
inventory$donated_in_2023[inventory$donor_id %in% visits_2023$donor_id] = T
inventory$donated_in_2023[is.na(inventory$donated_in_2023)] = F

for (i in 165763:nrow(inventory)) {
  cat(i, fill = T)
  if (is.na(inventory[i,]$selected)) {
    if (inventory[i,]$tested & inventory[i,]$survey_respondent_2023 & inventory[i,]$donated_in_2023) {
      inventory[i,]$selected = T
    }
  }
}


inventory$selected[is.na(inventory$selected)] = F

inventory = inventory %>%
  select(din, donor_id, collection_date, plasma_aliquot_count, total_plasma_volume_ml, serum_aliquot_count,
         total_serum_volume_ml, tested, in_substudy, donated_in_2023, survey_respondent_2023, selected)

# Tested or in substudy(ies)
summary(inventory$tested[year(inventory$collection_date) == 2022] | inventory$in_substudy [year(inventory$collection_date) == 2022])
sum(inventory$plasma_aliquot_count[(inventory$tested | inventory$in_substudy) & year(inventory$collection_date) == 2022])  +
  sum(inventory$serum_aliquot_count[(inventory$tested | inventory$in_substudy) & year(inventory$collection_date) == 2022])
# 159,220 aliquots

sum(inventory$plasma_aliquot_count) + sum(inventory$serum_aliquot_count)

# Survey responses
survey_responses = dbReadTable(sb_conn, "vtl_survey_responses")
survey_linkage = dbReadTable(sb_conn, "vtl_survey_donor_linkage")
survey_responses = left_join(survey_responses, survey_linkage, by = "donor_did")

inventory$survey_responder = F
inventory$survey_responder[inventory$donor_id %in% survey_responses$donor_id] = T

summary(inventory$tested[(inventory$tested | inventory$in_substudy) & inventory$survey_responder])
sum(inventory$plasma_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder]) +
  sum(inventory$serum_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder])

summary(inventory$tested[(inventory$tested | inventory$in_substudy) & inventory$survey_responder & year(inventory$collection_date) == 2022])
sum(inventory$plasma_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder & year(inventory$collection_date) == 2022]) +
  sum(inventory$serum_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder & year(inventory$collection_date) == 2022])

inventory$survey_responder_2022 = F
inventory$survey_responder_2022[inventory$donor_id %in% survey_responses$donor_id[year(survey_responses$response_date) >= "2022"]] = T

summary(inventory$tested[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022])
sum(inventory$plasma_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022]) +
  sum(inventory$serum_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022])

summary(inventory$tested[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022 & year(inventory$collection_date) == 2022])
sum(inventory$plasma_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022 & year(inventory$collection_date) == 2022]) +
  sum(inventory$serum_aliquot_count[(inventory$tested | inventory$in_substudy) & inventory$survey_responder_2022 & year(inventory$collection_date) == 2022])

# Only retain samples from the original NBDC if the donors were in the 2023 RDC cohort and had tested donations in 2023
donors_inventory = dbReadTable(sb_conn, "donors") %>% filter(bco == "VTL" & rdc_2023)
rdc_2023_tested = dbReadTable(sb_conn, "visits") %>% filter(din %in% nc$din &
                                                               din %in% igg$din &
                                                               year(collection_date) == "2023" &
                                                               donor_id %in% donors_inventory$donor_id)

inventory$rdc_2023_and_tested = F
inventory$rdc_2023_and_tested[inventory$donor_id %in% rdc_2023_tested$donor_id] = T

summary(inventory$tested[inventory$rdc_2023_and_tested | inventory$in_substudy])
sum(inventory$plasma_aliquot_count[inventory$rdc_2023_and_tested | inventory$in_substudy]) +
  sum(inventory$serum_aliquot_count[inventory$rdc_2023_and_tested | inventory$in_substudy])

# Only retain samples from the original NBDC if the donors were in the 2023 RDC cohort and responded to post 2023 survey
inventory$survey_responder_2023 = F
inventory$survey_responder_2023[inventory$donor_id %in% survey_responses$donor_id[year(survey_responses$response_date) == "2023"]] = T

summary(inventory$tested[(inventory$rdc_2023_and_tested | inventory$in_substudy) & inventory$survey_responder_2023])
sum(inventory$plasma_aliquot_count[(inventory$rdc_2023_and_tested | inventory$in_substudy) & inventory$survey_responder_2023]) +
  sum(inventory$serum_aliquot_count[(inventory$rdc_2023_and_tested | inventory$in_substudy) & inventory$survey_responder_2023])

