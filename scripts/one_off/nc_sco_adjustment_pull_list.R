# NC SCO < 100 selection
cop_list = readxl::read_xlsx("~/Desktop/CoP_4.2_din_selection_20240501_corrected.xlsx") %>%
  pull(din) %>%
  append(., c("W042522032406","W042522034133","W042522039278","W117021024465","W117021024593","W117021063323"))

candidates = read.csv("~/Desktop/Q1 2023 sample list for dilutional NC testing_050624.csv") %>%
  filter(din %in% cop_list == F) %>%
  mutate(sco_group = case_when(
    between(nc_sco_dil_1, 1, 5) ~ "1-5",
    between(nc_sco_dil_1, 6, 10) ~ "6-10",
    between(nc_sco_dil_1, 11, 15) ~ "11-15",
    between(nc_sco_dil_1, 16, 20) ~ "16-20",
    between(nc_sco_dil_1, 21, 25) ~ "21-25",
    between(nc_sco_dil_1, 26, 30) ~ "26-30",
    between(nc_sco_dil_1, 31, 35) ~ "31-35",
    between(nc_sco_dil_1, 36, 40) ~ "36-40",
    between(nc_sco_dil_1, 41, 45) ~ "41-45",
    between(nc_sco_dil_1, 46, 50) ~ "46-50",
    between(nc_sco_dil_1, 51, 55) ~ "51-55",
    between(nc_sco_dil_1, 56, 60) ~ "56-60",
    between(nc_sco_dil_1, 61, 65) ~ "61-65",
    between(nc_sco_dil_1, 66, 70) ~ "66-70",
    between(nc_sco_dil_1, 71, 75) ~ "71-75",
    between(nc_sco_dil_1, 76, 80) ~ "76-80",
    between(nc_sco_dil_1, 81, 85) ~ "81-85",
    between(nc_sco_dil_1, 86, 90) ~ "86-90",
    between(nc_sco_dil_1, 91, 95) ~ "91-95",
    between(nc_sco_dil_1, 96, 99) ~ "96-99"
  )) %>%
  select(din, cohort, bco, collection_date, neat_sco = nc_sco_dil_1, sco_group)

# Selection
selected = candidates %>%
  filter(!is.na(sco_group)) %>%
  group_by(sco_group) %>%
  slice_sample(n = 10) %>%
  ungroup()

manifest = selected %>%
  mutate(nc_neat_testing = "No",
         nc_dilutional_testing = "Yes",
         s_igg_full_testing = "No",
         s_igg_400_only_testing = "No",
         collection_date = as.Date(collection_date, format = "%m/%d/%Y")) %>%
  select(din, cohort, bco, collection_date, nc_neat_testing, nc_dilutional_testing, s_igg_full_testing, s_igg_400_only_testing)

write.csv(manifest, "~/Desktop/nc_sco_adjustment_dilutional_testing_pull_list_20240506.csv", row.names = F)
write.csv(selected, "~/Desktop/nc_sco_adjustment_neat")

