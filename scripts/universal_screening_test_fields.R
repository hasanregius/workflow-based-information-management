rdc_basefile = arrow::read_parquet("~/Desktop/analytic_dataset_20230202.parquet")
roche_fields = subset(names(rdc_basefile), grepl("ECOV",names(rdc_basefile)) |
                        grepl("ASCV",names(rdc_basefile)) |
                        grepl("COVID_8310", names(rdc_basefile)))
ortho_fields = subset(names(rdc_basefile), grepl("COVID_8300", names(rdc_basefile)) |
                        grepl("CVDA", names(rdc_basefile)))
rdc_basefile$E_nc_assay[rdc_basefile$E_nc_assay == ""] = NA
rdc_basefile$ECOV_ASCV_8310[rdc_basefile$ECOV_ASCV_8310 %in% c("","TECHNICAL PROBLEM")] = NA

univ_testing = rdc_basefile %>%
  mutate(
    cv2ts_sco = case_when(
      !is.na(CVDA_SCO) & is.na(E_st_sco) ~ as.numeric(CVDA_SCO),
      is.na(CVDA_SCO) & !is.na(E_st_sco) ~ as.numeric(E_st_sco),
      is.na(CVDA_SCO) & is.na(E_st_sco) ~ NA_real_),
    cv2ts_interpretation = case_when(
      CVDA_8300 == "NEGATIVE" | E_st_interpretation == "NR" ~ "non-reactive",
      CVDA_8300 == "POSITIVE" | E_st_interpretation == "R" ~ "reactive"),
    nc_sco = case_when(
      !is.na(ECOV_ASCV_SCO) & is.na(E_nc_sco) ~ as.numeric(ECOV_ASCV_SCO),
      is.na(ECOV_ASCV_SCO) & !is.na(E_nc_sco) ~ as.numeric(E_nc_sco),
      is.na(ECOV_ASCV_SCO) & is.na(ECOV_ASCV_SCO) ~ NA_real_),
    nc_interpretation = case_when(
      ECOV_ASCV_8310 == "POSITIVE" ~ "reactive",
      E_nc_interpretation == "R" ~ "reactive",
      E_nc_interpretation == "NR" ~ "non-reactive",
      ECOV_ASCV_8310 == "NEGATIVE" ~ "non-reactive"),
    nc_assay = case_when(
      E_nc_assay == "Vitros Total Ig NC" ~ "ortho",
      E_nc_assay == "Ortho Total Ig NC" ~ "ortho",
      E_nc_assay == "Roche Total Ig NC" ~ "roche",
      is.na(E_nc_assay) & !is.na(ECOV_ASCV_8310) ~ "roche"),
    din = substr(DONATION_NUMBER, 1, 13)) %>%
  select(din, cv2ts_sco, cv2ts_interpretation, nc_assay, nc_sco, nc_interpretation) %>%
  filter(nchar(din) == 13)

stot = rdc_basefile %>%
  select(din,
         vitros_s_total_ig_sco = s_total_sco,
         vitros_s_total_ig_interpretation = s_total_interpretation) %>%
  filter(!is.na(vitros_s_total_ig_sco) & din %in% db_s$din == F)
dbWriteTable(lc_conn, "cv2ts", stot, append = T)

s_fnames = dbListFields(lc_conn, "cv2ts")

ntot = select(univ_testing, c("din","nc_assay","nc_sco","nc_interpretation")) %>%
  filter(!is.na(nc_sco) & !is.na(nc_assay) & din %in% rdc_dins)
ntot$nc_interpretation[ntot$nc_sco >= 1] = "reactive"
summary(as.factor(ntot$nc_interpretation[ntot$nc_sco >= 1]))

rdc_nc = rdc_basefile %>%
  filter(is.na(nc_total_interpretation) == F) %>%
  mutate(ck = paste0(din,nc_test_name))

db_s = dbReadTable(lc_conn, "cv2ts")

dumb_linkage_table_add = stot %>% filter(din %in% samples_din == F)
dumb_linkage_table_add2 = ntot %>% filter(din %in% samples_din == F)
dumb_linkage_table_add = rbind(dumb_linkage_table_add[,1], dumb_linkage_table_add2[,1]) %>%
  mutate(sample_status = NA,
         aliquot_type = NA,
         aliquot_anticoagulant = NA)
dumb_linkage_table_add = dumb_linkage_table_add %>%
  filter(din %in% samples_din == F)

ntot = rdc_nc %>%
  mutate(nc_total_ig_test_date = NA,
         nc_total_ig_reagent_lot = NA,
         nc_test_name = case_when(nc_total_assay == "Ortho VITROS Total Ig NC" ~ "ortho",
                                  nc_total_assay == "Roche ELECSYS Total Ig NC" ~ "roche"),
         ck = paste0(din, nc_test_name)) %>%
  filter(ck %in% nc_dins$ck == F & !is.na(nc_test_name)) %>%
  select(din, nc_total_ig_test = nc_test_name,
         nc_total_ig_test_date,
         nc_sco = nc_total_sco,
         nc_interpretation = nc_total_interpretation,
         nc_total_ig_reagent_lot)

dbWriteTable(lc_conn, "nc_total_ig_results", ntot, append = T)

nc_table1 = dbReadTable(lc_conn, "nc_total_ig_results")

nc_fnames = dbListFields(lc_conn, "nc_total_ig_results")
nc_dins = dbReadTable(lc_conn, "nc_total_ig_results") %>%
  mutate(ck = paste0(din, nc_total_ig_test))
cohortdat = rdc_basefile %>%
  mutate(cv2ts_interpretation = case_when(
    CVDA_RESULTS == "N" | CVDA_RESULTS == "Non-reactive" ~ "non-reactive",
    CVDA_RESULTS == "P" | CVDA_RESULTS == "Reactive" ~ "reactive",
    COVID_8300 == "NEGATIVE" & is.na(CVDA_RESULTS) ~ "non-reactive",
    COVID_8300 == "POSITIVE" & is.na(CVDA_RESULTS) ~ "reactive"),
    cv2ts_sco = case_when(
      !is.na(CVDA_SCO) ~ as.numeric(CVDA_SCO),
      is.na(CVDA_SCO) ~ NA_real_),
    nc_interpretation = case_when(
      ECOV_RESULTS == "POS" ~ "reactive",
      ECOV_RESULTS == "NEG" ~ "non-reactive",
      ASCV_RESULTS == "P" | ASCV_RESULTS == "HP" | ASCV_RESULTS == "LP" ~ "reactive",
      ASCV_RESULTS == "N" ~ "non-reactive",
      COVID_8310 == "POSITIVE" ~ "reactive",
      COVID_8310 == "NEGATIVE" ~ "non-reactive"),
    nc_total_sco = case_when(
      !is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have only ecov
      !is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have both
      is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ASCV_SCO), # have only ascv
      is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ NA_real_))


# Ortho S: covid_8300, cvda_results, cvda_sco
# Roche : ecov_sco, ecov_results, ascv_sco, ascv_results, covid_8310

dil_vtl = read.csv("~/Desktop/RDC_VTL_dilutional_testing_list_20230127.csv")
dil_q1 = dil_vtl %>%
  filter(between(as.Date(dil_vtl$collection_date), as.Date("2022-01-01"), as.Date("2022-03-31"))) %>%
  group_by(cohort) %>%
  summarize(n = n())
