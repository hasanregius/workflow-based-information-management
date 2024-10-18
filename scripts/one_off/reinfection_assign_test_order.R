# Check the files Tunde sent

# Multiple RI
m_vri_dins = read.csv(file.choose())
m_vri_dids = read.csv(file.choose())

# Single RI
s_vri_dins = read.csv(file.choose())
s_vri_dids = read.csv(file.choose())

length(unique(s_vri_dins$smp_din))

dids = unique(s_vri_dids$donor_id)

# Db pull
lc_conn = connect_db()

donors = dbReadTable(lc_conn, "donors")
visits = dbReadTable(lc_conn, "visits")

donations = left_join(donors, visits, by = "donor_id")

# NC Total IG
nc = dbReadTable(lc_conn, "nc_total_ig_results") %>%
  filter(nc_total_ig_test == "ortho" & nc_final_result == T)

# S Quant IgG
s_igg = dbReadTable(lc_conn, "cv2gs") %>%
  filter(final_result == T)

# Ortho testing
ortho_1 = readxl::read_xlsx("~/Downloads/VTL Single Multi and ARC Multi reinfection data.xlsx", sheet = 1)
ortho_2 = readxl::read_xlsx("~/Downloads/VTL Single Multi and ARC Multi reinfection data.xlsx", sheet = 2)
ortho_1 = ortho_1[,c(1,2,3,9,15,16)]
ortho_2 = ortho_2[,c(1,2,3,9,15,16,17)]
ortho = full_join(ortho_1, ortho_2, by = c("donor_id", "din"))
ortho$ortho_tested = T
ortho_flag = ortho[,c(2,12)]

# Multiple RI ----
mri = donations %>%
  right_join(., m_vri_dids, by = "donor_id") %>%
  filter(collection_date >= as.Date(first_dt, format = "%m/%d/%Y") &
         sample_in_repository == T) %>%
  mutate(ri_dt1 = as.Date(ri_dt1, format = "%m/%d/%Y"),
         post_infection = T,
         post_reinfection = case_when(
           collection_date >= ri_dt1 ~ T,
           collection_date < ri_dt1 ~ F)
         )

mri_list = mri %>%
  left_join(., s_igg, by = "din") %>%
  left_join(., nc, by = "din") %>%
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
  select(donor_id, din, cohort, collection_date, post_infection,
         post_reinfection, nc_testing, nc_dilutional, igg_testing, igg_dilutions, ortho_tested) %>%
  distinct()

write.csv(mri_list, "/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/VTL_multiple_RIs_testing_list_internal_20231107.csv", row.names = F)
mri_list %>% select(-ortho_tested) -> mri_list
write.csv(mri_list, "/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/VTL_multiple_RIs_testing_list_20231107.csv", row.names = F)


# Single RI ----
sri = donations %>%
  right_join(., s_vri_dids, by = "donor_id") %>%
  filter(collection_date >= as.Date(first_dt, format = "%m/%d/%Y") &
           sample_in_repository == T) %>%
  mutate(ri_dt1 = as.Date(ri_dt1, format = "%m/%d/%Y"),
         post_infection = T,
         post_reinfection = case_when(
           collection_date >= ri_dt1 ~ T,
           collection_date < ri_dt1 ~ F)
  )

sri_list = sri %>%
  left_join(., s_igg, by = "din") %>%
  left_join(., nc, by = "din") %>%
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
  select(donor_id, din, cohort, collection_date, post_infection,
         post_reinfection, nc_testing, nc_dilutional, igg_testing, igg_dilutions, ortho_tested) %>%
  distinct()

write.csv(sri_list, "/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/VTL_single_RIs_testing_list_internal_20231107.csv", row.names = F)
sri_list %>% select(-ortho_tested) -> sri_list
write.csv(sri_list, "/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/VTL_single_RIs_testing_list_20231107.csv", row.names = F)

# Generate new lists (11/07/23)
#  Designate flagging for donors
m_vri_dids %>%
  mutate(reinfection_group = "multiple") %>%
  select(donor_id, reinfection_group) -> m_vri_did_simp
s_vri_dids %>%
  mutate(reinfection_group = "single") %>%
  select(donor_id, reinfection_group) -> s_vri_did_simp
donor_flag = rbind(m_vri_did_simp, s_vri_did_simp) %>% distinct()

#  Pull in the original list
ori_list = read.csv("/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/Archive/20230515_VTL_RI_cases_pull_list.csv")

#  New list
new_list = rbind(mri_list, sri_list)

#  Check for differences
summary(ori_list$din %in% new_list$din)
dropped_samples = ori_list %>%
  filter(din %in% new_list$din == F)
write.csv(dropped_samples, "~/Desktop/dropped_samples_RI_testing.csv", row.names = F)

# 318 DINs in the new list that is not on the old list
# Of the 318, 2 doesn't require any testing so they weren't included
difference = new_list %>%
  filter(din %in% ori_list$din == F)
difference_a = difference %>%
  filter(nc_testing == F & igg_testing == F)
difference %>% filter(din %in% difference_a$din == F) -> difference
write.csv(difference, "~/Desktop/VTL_additional_RI_test_list_20231107.csv", row.names = F)

# Save the new list
new_list %>%
  left_join(., donor_flag, by = "donor_id") -> new_list

write.csv(new_list, "~/Desktop/VTL_RI_testing_list_20231107.csv", row.names = F)


# Update ARC list
arc_ri = read.csv("/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/ARC_multiple_RIs_testing_list_20230704.csv")
arc_samples = lc_conn %>%
  tbl(in_schema("arc","samples")) %>%
  collect() %>%
  filter(donor_id %in% arc_ri$donor_id) %>%
  select(donor_id, cohort) %>%
  distinct()
arc_ri %>%
  left_join(., arc_samples, by = "donor_id") %>%
  select(donor_id, cohort, din, collection_date) -> arc_ri
write.csv(arc_ri, "/Users/c02715/Library/CloudStorage/OneDrive-Vitalant/Vitalant Repeat Donor Cohorts/DIN Testing Lists/RI Testing Lists/ARC_multiple_RIs_testing_list_20231107.csv", row.names = F)

#
