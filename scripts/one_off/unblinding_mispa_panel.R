# MISPA assay
require(readxl)
manifest = read_csv("~/Desktop/ASU MISPA manifest.csv") %>%
  select(din = DIN,
         lims_id = `Freezerworks ID`,
         sample_id = `Primary Sample ID`,
         sample_group = Study_Name,
         donor_id = `Donor ID`,
         collection_date = `Collection Date`,
         aliquot_id = `Aliquot ID`,
         Sample_Name = `Unique Aliquot ID`,
         panel_id = `Panel ID`,
         box = `Box ID`,
         row = `Position 4`,
         column = `Position 5`) %>%
  mutate(collection_date = as.Date(collection_date, format = "%m/%d/%y"),
         Sample_Name = str_pad(Sample_Name, width = 7, side = "left", pad = "0"))


results_1 = read_xlsx("~/Desktop/1. Vitalant Run01_Data_20240621.xlsx", sheet = 1, skip = 7)
names(results_1)[3:length(names(results_1))] = paste0("SINR01_",names(results_1)[3:length(names(results_1))])

results_2 = read_xlsx("~/Desktop/1. Vitalant Run01_Data_20240621.xlsx", sheet = 2, skip = 15)
names(results_2)[3:length(names(results_2))] = paste0("RaHaloCal_",names(results_2)[3:length(names(results_2))])

results_3 = read_xlsx("~/Desktop/1. Vitalant Run01_Data_20240621.xlsx", sheet = 3, skip = 15)
names(results_3)[3:length(names(results_3))] = paste0("STDCal_",names(results_3)[3:length(names(results_3))])

sb_conn = connect_db(db_name = "VRI_Sandbox")

# NC total IG results
nc_db = dbReadTable(sb_conn, "nc_total_ig_results") %>%
  filter(din %in% manifest$din & nc_final_result) %>%
  group_by(din) %>%
  slice_max(nc_dilution_factor) %>%
  ungroup() %>%
  select(din, nc_total_ig_test, nc_dilution_factor, nc_sco, nc_interpretation) %>%
  mutate(nc_total_ig_test = "ortho_total_ig") %>%
  distinct()

# Ortho IgG S results
s_db = dbReadTable(sb_conn, "cv2gs") %>%
  filter(din %in% manifest$din & final_result) %>%
  group_by(din) %>%
  slice_max(vitros_quant_s_igg_dilution_factor, n = 1) %>%
  ungroup() %>%
  select(din,
         vitros_quant_s_igg_bauml,
         vitros_quant_s_igg_dilution_factor,
         vitros_quant_s_igg_quantification,
         vitros_quant_s_igg_bauml,
         vitros_quant_s_igg_interpretation) %>%
  distinct()

results = full_join(s_db, nc_db, by = "din")

mansults = left_join(manifest, results, by = "din") %>%
  select(-din, -lims_id, -sample_id, -donor_id) %>%
  group_by(Sample_Name) %>%
  slice_sample(n = 1) %>%
  ungroup() %>%
  select(Sample_Name,
         sample_group,
         collection_date,
         vitros_quant_s_igg_bauml,
         vitros_quant_s_igg_dilution_factor,
         vitros_quant_s_igg_quantification,
         vitros_quant_s_igg_interpretation,
         nc_total_ig_test,
         nc_dilution_factor,
         nc_sco,
         nc_interpretation)

results_1$Sample_Name = str_pad(results_1$Sample_Name, width = 7, side = "left", pad = "0")
results_2$Sample_Name = str_pad(results_2$Sample_Name, width = 7, side = "left", pad = "0")
results_3$Sample_Name = str_pad(results_3$Sample_Name, width = 7, side = "left", pad = "0")

unblinding = mansults %>%
  left_join(., results_1, by = "Sample_Name") %>%
  left_join(., results_2, by = "Sample_Name") %>%
  left_join(., results_3, by = "Sample_Name") %>%
  select(-Sample_Group.y, -Sample_Group.x)

write.csv(unblinding, "~/Desktop/asu_mispa_panel_unblinding.csv", row.names = F)








