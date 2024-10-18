# MSD Accessioning
sb_conn = connect_db(db_name = "VRI_Sandbox")
fieldnames = dbListFields(sb_conn, "msd_multiplex")

msd_results = readxl::read_xlsx(file, sheet = 1) %>%
  filter(!is.na(`SARS-Cov2 Nucleocapsid`)) %>%
  mutate(
    msd_scov2_nc_quantification = case_when(
      `SARS-Cov2 Nucleocapsid` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Nucleocapsid` != "<LOQ" ~ "within_loq"),
    msd_scov2_nc_bauml = case_when(
      `SARS-Cov2 Nucleocapsid` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Nucleocapsid` != "<LOQ" ~ as.numeric(`SARS-Cov2 Nucleocapsid`)),
    msd_scov2_s_ancestral_quantification = case_when(
      `SARS-CoV-2 Spike` == "<LOQ" ~ "below_loq",
      `SARS-CoV-2 Spike` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ancestral_bauml = case_when(
      `SARS-CoV-2 Spike` == "<LOQ" ~ NA_real_,
      `SARS-CoV-2 Spike` != "<LOQ" ~ as.numeric(`SARS-CoV-2 Spike`)),
    msd_scov2_s_b1_1_7_quantification = case_when(
      `SARS-Cov2 Spike B1.1.7` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Spike B1.1.7` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_b1_1_7_bauml = case_when(
      `SARS-Cov2 Spike B1.1.7` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Spike B1.1.7` != "<LOQ" ~ as.numeric(`SARS-Cov2 Spike B1.1.7`)),
    msd_scov2_s_b1_351_quantification = case_when(
      `SARS-Cov2 Spike B1.351` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Spike B1.351` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_b1_351_bauml = case_when(
      `SARS-Cov2 Spike B1.351` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Spike B1.351` != "<LOQ" ~ as.numeric(`SARS-Cov2 Spike B1.351`)),
    msd_scov2_s_ba2_to_ba2_12_quantification = case_when(
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` == "<LOQ" ~ "below_loq",
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_to_ba2_12_bauml = case_when(
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` == "<LOQ" ~ NA_real_,
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` != "<LOQ" ~ as.numeric(`Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12`)),
    msd_scov2_s_ba2_12_1_quantification = case_when(
      `Spike (BA.2.12.1)` == "<LOQ" ~ "below_loq",
      `Spike (BA.2.12.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_12_1_bauml = case_when(
      `Spike (BA.2.12.1)` == "<LOQ" ~ NA_real_,
      `Spike (BA.2.12.1)` != "<LOQ" ~ as.numeric(`Spike (BA.2.12.1)`)),
    msd_scov2_s_ba1_quantification = case_when(
      `Spike (B.1.1.529; BA.1)` == "<LOQ" ~ "below_loq",
      `Spike (B.1.1.529; BA.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba1_bauml = case_when(
      `Spike (B.1.1.529; BA.1)` == "<LOQ" ~ NA_real_,
      `Spike (B.1.1.529; BA.1)` != "<LOQ" ~ as.numeric(`Spike (B.1.1.529; BA.1)`)),
    msd_scov2_s_ay4_quantification = case_when(
      `Spike (B.1.617.2; AY 4)` == "<LOQ" ~ "below_loq",
      `Spike (B.1.617.2; AY 4)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ay4_bauml = case_when(
      `Spike (B.1.617.2; AY 4)` == "<LOQ" ~ NA_real_,
      `Spike (B.1.617.2; AY 4)` != "<LOQ" ~ as.numeric(`Spike (B.1.617.2; AY 4)`)),
    msd_scov2_s_ba2_75_quantification = case_when(
      `Spike (BA.2.75)` == "<LOQ" ~ "below_loq",
      `Spike (BA.2.75)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_75_bauml = case_when(
      `Spike (BA.2.75)` == "<LOQ" ~ NA_real_,
      `Spike (BA.2.75)` != "<LOQ" ~ as.numeric(`Spike (BA.2.75)`)),
    msd_scov2_s_ba2_5_quantification = case_when(
      `Spike (BA.5)` == "<LOQ" ~ "below_loq",
      `Spike (BA.5)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_5_bauml = case_when(
      `Spike (BA.5)` == "<LOQ" ~ NA_real_,
      `Spike (BA.5)` != "<LOQ" ~ as.numeric(`Spike (BA.5)`))
  ) %>%
  select(all_of(fieldnames))

# Other plate
msd_results = readxl::read_xlsx(file, sheet = 1) %>%
  filter(!is.na(`SARS-Cov2 Nucleocapsid...44`)) %>%
  mutate(
    msd_scov2_nc_quantification = case_when(
      `SARS-Cov2 Nucleocapsid...44` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Nucleocapsid...44` != "<LOQ" ~ "within_loq"),
    msd_scov2_nc_bauml = case_when(
      `SARS-Cov2 Nucleocapsid...44` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Nucleocapsid...44` != "<LOQ" ~ as.numeric(`SARS-Cov2 Nucleocapsid...44`)),
    msd_scov2_s_ancestral_quantification = case_when(
      `SARS-CoV-2 Spike...45` == "<LOQ" ~ "below_loq",
      `SARS-CoV-2 Spike...45` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ancestral_bauml = case_when(
      `SARS-CoV-2 Spike...45` == "<LOQ" ~ NA_real_,
      `SARS-CoV-2 Spike...45` != "<LOQ" ~ as.numeric(`SARS-CoV-2 Spike...45`)),
    msd_scov2_s_b1_1_7_quantification = case_when(
      `SARS-Cov2 Spike B1.1.7` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Spike B1.1.7` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_b1_1_7_bauml = case_when(
      `SARS-Cov2 Spike B1.1.7` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Spike B1.1.7` != "<LOQ" ~ as.numeric(`SARS-Cov2 Spike B1.1.7`)),
    msd_scov2_s_b1_351_quantification = case_when(
      `SARS-Cov2 Spike B1.351` == "<LOQ" ~ "below_loq",
      `SARS-Cov2 Spike B1.351` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_b1_351_bauml = case_when(
      `SARS-Cov2 Spike B1.351` == "<LOQ" ~ NA_real_,
      `SARS-Cov2 Spike B1.351` != "<LOQ" ~ as.numeric(`SARS-Cov2 Spike B1.351`)),
    msd_scov2_s_ba2_to_ba2_12_quantification = case_when(
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` == "<LOQ" ~ "below_loq",
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_to_ba2_12_bauml = case_when(
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` == "<LOQ" ~ NA_real_,
      `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12` != "<LOQ" ~ as.numeric(`Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12`)),
    msd_scov2_s_ba2_12_1_quantification = case_when(
      `Spike (BA.2.12.1)` == "<LOQ" ~ "below_loq",
      `Spike (BA.2.12.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_12_1_bauml = case_when(
      `Spike (BA.2.12.1)` == "<LOQ" ~ NA_real_,
      `Spike (BA.2.12.1)` != "<LOQ" ~ as.numeric(`Spike (BA.2.12.1)`)),
    msd_scov2_s_ba1_quantification = case_when(
      `Spike (B.1.1.529; BA.1)` == "<LOQ" ~ "below_loq",
      `Spike (B.1.1.529; BA.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba1_bauml = case_when(
      `Spike (B.1.1.529; BA.1)` == "<LOQ" ~ NA_real_,
      `Spike (B.1.1.529; BA.1)` != "<LOQ" ~ as.numeric(`Spike (B.1.1.529; BA.1)`)),
    msd_scov2_s_ay4_quantification = case_when(
      `Spike (B.1.617.2; AY 4)` == "<LOQ" ~ "below_loq",
      `Spike (B.1.617.2; AY 4)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ay4_bauml = case_when(
      `Spike (B.1.617.2; AY 4)` == "<LOQ" ~ NA_real_,
      `Spike (B.1.617.2; AY 4)` != "<LOQ" ~ as.numeric(`Spike (B.1.617.2; AY 4)`)),
    msd_scov2_s_ba2_75_quantification = case_when(
      `Spike (BA.2.75)` == "<LOQ" ~ "below_loq",
      `Spike (BA.2.75)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba2_75_bauml = case_when(
      `Spike (BA.2.75)` == "<LOQ" ~ NA_real_,
      `Spike (BA.2.75)` != "<LOQ" ~ as.numeric(`Spike (BA.2.75)`)),
    msd_scov2_s_ba5_quantification = case_when(
      `Spike (BA.5)...53` == "<LOQ" ~ "below_loq",
      `Spike (BA.5)...53` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_ba5_bauml = case_when(
      `Spike (BA.5)...53` == "<LOQ" ~ NA_real_,
      `Spike (BA.5)...53` != "<LOQ" ~ as.numeric(`Spike (BA.5)...53`)),
    msd_scov2_s_eg5_1_quantification = case_when(
      `Spike (EG.5.1)` == "<LOQ" ~ "below_loq",
      `Spike (EG.5.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_eg5_1_bauml = case_when(
      `Spike (EG.5.1)` == "<LOQ" ~ NA_real_,
      `Spike (EG.5.1)` != "<LOQ" ~ as.numeric(`Spike (EG.5.1)`)),
    msd_scov2_s_fl1_5_1_quantification = case_when(
      `Spike (FL.1.5.1)` == "<LOQ" ~ "below_loq",
      `Spike (FL.1.5.1)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_fl1_5_1_bauml = case_when(
      `Spike (FL.1.5.1)` == "<LOQ" ~ NA_real_,
      `Spike (FL.1.5.1)` != "<LOQ" ~ as.numeric(`Spike (FL.1.5.1)`)),
    msd_scov2_s_xbb1_16_quantification = case_when(
      `Spike (XBB.1.16)` == "<LOQ" ~ "below_loq",
      `Spike (XBB.1.16)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_xbb1_16_bauml = case_when(
      `Spike (XBB.1.16)` == "<LOQ" ~ NA_real_,
      `Spike (XBB.1.16)` != "<LOQ" ~ as.numeric(`Spike (XBB.1.16)`)),
    msd_scov2_s_xbb1_16_6_quantification = case_when(
      `Spike (XBB.1.16.6)` == "<LOQ" ~ "below_loq",
      `Spike (XBB.1.16.6)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_xbb1_16_6_bauml = case_when(
      `Spike (XBB.1.16.6)` == "<LOQ" ~ NA_real_,
      `Spike (XBB.1.16.6)` != "<LOQ" ~ as.numeric(`Spike (XBB.1.16.6)`)),
    msd_scov2_s_xbb1_5_quantification = case_when(
      `Spike (XBB.1.5)` == "<LOQ" ~ "below_loq",
      `Spike (XBB.1.5)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_xbb1_5_bauml = case_when(
      `Spike (XBB.1.5)` == "<LOQ" ~ NA_real_,
      `Spike (XBB.1.5)` != "<LOQ" ~ as.numeric(`Spike (XBB.1.5)`)),
    msd_scov2_s_xbb2_3_quantification = case_when(
      `Spike (XBB.2.3)` == "<LOQ" ~ "below_loq",
      `Spike (XBB.2.3)` != "<LOQ" ~ "within_loq"),
    msd_scov2_s_xbb2_3_bauml = case_when(
      `Spike (XBB.2.3)` == "<LOQ" ~ NA_real_,
      `Spike (XBB.2.3)` != "<LOQ" ~ as.numeric(`Spike (XBB.2.3)`)),
  ) %>%
  select(all_of(fieldnames))

dbWriteTable(sb_conn, name = "msd_multiplex", value = msd_results, append = T)

msd_db = dbReadTable(sb_conn, "msd_multiplex")
msd_results %>% filter(msd_results$din %in% msd_db$din == F) -> msd_results

bs = visits_db %>%
  filter(din %in% msd_results$din) %>%
  select(din, sample_status = sample_in_repository) %>%
  mutate(aliquot_type = NA,
         aliquot_anticoagulant = NA)

bs$sample_status = "available"
dbWriteTable(sb_conn, "samples", bs, append = T)

# New MSD data incoming 8/27/24
readxl::excel_sheets("~/Downloads/RDC VRI Panel 2 Seroplex MSD VPlex CoV RV IgG Manifest results 082724.xlsx")
new_msd = readxl::read_xlsx("~/Downloads/RDC VRI Panel 2 Seroplex MSD VPlex CoV RV IgG Manifest results 082724.xlsx")

# File 1
file_1_path = "~/Desktop/Consolidated Manifest CDC aim4.1 results 050224.xlsx"
file_1 = readxl::read_xlsx(file_1_path, sheet = 1, col_types = "text")
file_1_processed = file_1 %>%
  select(-GUAID, -aliquot_type, -aliquot_anticoagulant, -current_amount, -box_id,
         -row, -column, -`Case or Control`) %>%
  pivot_longer(cols = 2:11, names_to = "fieldname", values_to = "auml") %>%
  mutate(
    quantification = case_when(
      grepl("<", auml) ~ "below_loq",
      grepl(">", auml) ~ "above_loq",
      !grepl("<", auml) | !grepl(">", auml) ~ "within_loq"),
    pathogen = "sars_cov_2",
    antigen = case_when(
      grepl("spike", fieldname, ignore.case = T) ~ "spike",
      grepl("nucleocapsid", fieldname, ignore.case = T) ~ "nucleocapsid"),
    strain = case_when(
      fieldname %in% c("SARS-Cov2 Nucleocapsid", "SARS-CoV-2 Spike") ~ "ancestral",
      grepl("B1.1.7", fieldname) ~ "B1.1.7",
      grepl("B1.351", fieldname) ~ "B1.351",
      grepl("BA.2", fieldname) & grepl("BA.2.12", fieldname) ~ "B2:B2.12",
      grepl("B.1.1.529; BA.1", fieldname) ~ "B.1.1.529; BA.1",
      grepl("B.1.617.2; AY 4", fieldname) ~ "B.1.617.2; AY 4",
      grepl("BA.2.12.1", fieldname) ~ "BA.2.12.1",
      grepl("BA.2.75", fieldname) ~ "BA.2.75",
      grepl("BA.5", fieldname) ~ "BA.5"),
    auml = as.numeric(auml)) %>%
  select(din, pathogen, strain, antigen, quantification, auml)

# File 2
file_2_path = "~/Desktop/CoP_4.2_din_selection_20240501_results 051024.xlsx"
file_2_processed = readxl::read_xlsx(file_2_path, sheet = 1, col_types = "text") %>%
  select(din,
         `SARS-CoV-2 Spike` = `SARS-CoV-2 Spike...45`,
         `SARS-Cov2 Nucleocapsid` = `SARS-Cov2 Nucleocapsid...44`,
         `SARS-Cov2 Spike B1.1.7`,
         `SARS-Cov2 Spike B1.351`,
         `Spike ( BA.2, BA.2.1, BA.2.2, BA.2.3, BA.2.5,...to BA.2.12`,
         `Spike (B.1.1.529; BA.1)`,
         `Spike (B.1.617.2; AY 4)`,
         `Spike (BA.2.12.1)`,
         `Spike (BA.2.75)`,
         `Spike (BA.5)` = `Spike (BA.5)...53`,
         `Spike (BA.2.86)`,
         `Spike (EG.5.1)`,
         `Spike (FL.1.5.1)`,
         `Spike (XBB.1.16)`,
         `Spike (XBB.1.16.6)`,
         `Spike (XBB.1.5)`,
         `Spike (XBB.2.3)`
         ) %>%
  pivot_longer(cols = 2:11, names_to = "fieldname", values_to = "auml") %>%
  mutate(
    quantification = case_when(
      grepl("<", auml) ~ "below_loq",
      grepl(">", auml) ~ "above_loq",
      !grepl("<", auml) | !grepl(">", auml) ~ "within_loq"),
    pathogen = "sars_cov_2",
    antigen = case_when(
      grepl("spike", fieldname, ignore.case = T) ~ "spike",
      grepl("nucleocapsid", fieldname, ignore.case = T) ~ "nucleocapsid"),
    strain = case_when(
      fieldname %in% c("SARS-Cov2 Nucleocapsid", "SARS-CoV-2 Spike") ~ "ancestral",
      grepl("B1.1.7", fieldname) ~ "B1.1.7",
      grepl("B1.351", fieldname) ~ "B1.351",
      grepl("BA.2", fieldname) & grepl("BA.2.12", fieldname) ~ "B2:B2.12",
      grepl("B.1.1.529; BA.1", fieldname) ~ "B.1.1.529; BA.1",
      grepl("B.1.617.2; AY 4", fieldname) ~ "B.1.617.2; AY 4",
      grepl("BA.2.12.1", fieldname) ~ "BA.2.12.1",
      grepl("BA.2.75", fieldname) ~ "BA.2.75",
      grepl("BA.5", fieldname) ~ "BA.5",
      grepl("BA.2.86", fieldname) ~ "BA.2.86",
      grepl("EG.5.1", fieldname) ~ "EG.5.1",
      grepl("FL.1.5.1", fieldname) ~ "FL.1.5.1",
      grepl("XBB.1.16", fieldname) ~ "XBB.1.16",
      grepl("XBB.1.16.6", fieldname) ~ "XBB.1.16.6",
      grepl("XBB.1.5", fieldname) ~ "XBB.1.5",
      grepl("XBB.2.3", fieldname) ~ "XBB.2.3"),
    auml = as.numeric(auml)) %>%
  select(din, pathogen, strain, antigen, quantification, auml)

msd_cop = rbind(file_1_processed, file_2_processed) %>%
  distinct()

# New values
new_msd = readxl::read_xlsx("~/Downloads/RDC VRI Panel 2 Seroplex MSD VPlex CoV RV IgG Manifest results 082724.xlsx", col_types = "text") %>%
  select(din = DIN,
         names(.)[grepl("Flu", names(.))],
         names(.)[grepl("RSV", names(.))],
         names(.)[grepl("SARS-CoV-2", names(.))]) %>%
  pivot_longer(cols = 2:ncol(.), names_to = "fieldname", values_to = "auml") %>%
  mutate(
    quantification = case_when(
      grepl("<", auml) ~ "below_loq",
      grepl(">", auml) ~ "above_loq",
      !grepl("<", auml) | !grepl(">", auml) ~ "within_loq"),
    pathogen = case_when(
      grepl("Flu", fieldname) ~ "influenza",
      grepl("RSV", fieldname) ~ "rsv",
      grepl("SARS-CoV-2", fieldname) ~ "sars_cov_2"),
    antigen = case_when(
      pathogen == "sars_cov_2" & grepl("spike", fieldname, ignore.case = T) ~ "spike",
      pathogen == "sars_cov_2" & grepl("nucleocapsid", fieldname, ignore.case = T) ~ "nucleocapsid",
      pathogen == "influenza" & grepl(" H1", fieldname) ~ "H1",
      pathogen == "influenza" & grepl(" H3", fieldname) ~ "H3",
      pathogen == "influenza" & grepl(" H7", fieldname) ~ "H7",
      pathogen == "influenza" & grepl(" HA", fieldname) ~ "HA",
      pathogen == "rsv" & grepl(" F", fieldname) ~ "F"),
    strain = case_when(
      pathogen == "sars_cov_2" ~ "ancestral",
      pathogen == "influenza" & grepl("A/Darwin/2021", fieldname) ~ "A/Darwin/2021",
      pathogen == "influenza" & grepl("A/Wisconsin/2019", fieldname) ~ "A/Wisconsin/2019",
      pathogen == "influenza" & grepl("B/Austria/2021", fieldname) ~ "B/Austria/2021",
      pathogen == "influenza" & grepl("B/Phuket/2013", fieldname) ~ "B/Phuket/2013",
      pathogen == "influenza" & grepl("A/Shangai/2013", fieldname) ~ "A/Shangai/2013",
      pathogen == "rsv" ~ "A and B"),
    auml = as.numeric(auml)
  )  %>%
  select(din, pathogen, strain, antigen, quantification, auml)

all_msd = rbind(msd_cop, new_msd)
dbWriteTable(sb_conn, "msd_multiplex", all_msd, append = T)

msd_db = dbReadTable(sb_conn, "msd_multiplex") %>%
  mutate(ck = paste0(din, pathogen, strain, antigen))
msd_acc = all_msd %>%
  mutate(ck = paste0(din, pathogen, strain, antigen)) %>%
  filter(ck %in% msd_db$ck == F & !is.na(din)) %>%
  select(-ck)
dbWriteTable(sb_conn, "msd_multiplex", msd_acc, append = T)
