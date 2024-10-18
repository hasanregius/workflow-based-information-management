rdc_basefile = read.csv("~/Desktop/UDPVRDC_testresults_asof20230413edw.csv")
rdc_basefile = arrow::read_parquet("~/Desktop/analytic_dataset_20230202.parquet")
roche_fields = subset(names(rdc_basefile),
                      grepl("ECOV_SCO",names(rdc_basefile)) |
                        grepl("ASCV_SCO",names(rdc_basefile)))
ortho_sco_fnames = subset(names(rdc_basefile),
                          grepl("COVID_8300", names(rdc_basefile)) |
                            grepl("CVDA_SCO", names(rdc_basefile)))
ortho_int_fnames = subset(names(rdc_basefile),
                          grepl("COVID_8300", names(rdc_basefile)) |
                            grepl("CVDA_SCO", names(rdc_basefile)))
rdc_basefile$E_nc_assay[rdc_basefile$E_nc_assay == ""] = NA
rdc_basefile$ECOV_ASCV_8310[rdc_basefile$ECOV_ASCV_8310 %in% c("","TECHNICAL PROBLEM")] = NA

# Standardize the data and nulls
for (field in names(rdc_basefile)) {
  if (class(rdc_basefile[,field]) == "character") {
    rdc_basefile[,field] = trimws(rdc_basefile[,field])
  }
  rdc_basefile[,field][rdc_basefile[,field] == ""] = NA
}

univ_testing = rdc_basefile %>%
  mutate(
    vitros_total_ig_s_interpretation = case_when(CVDA_SCO >= 1 ~ "reactive",
                                                 CVDA_SCO < 1 ~ "non-reactive"),
    nc_sco_covid8305 = as.numeric(COVID_8305),
    nc_sco = case_when(
      !is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO),
      is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ASCV_SCO)),
    nc_interpretation = case_when(
      nc_sco >= 1 ~ "reactive",
      nc_sco < 1 ~ "non-reactive"),
    din = substr(din, 1, 13)) %>%
  select(din,
         vitros_total_ig_s_sco = CVDA_SCO, vitros_total_ig_s_interpretation,
         nc_sco_covid8305, nc_sco, nc_interpretation) %>%
  filter(nchar(din) == 13)

univ_testing$nc_sco[!is.na(univ_testing$nc_sco_covid8305)] = univ_testing$nc_sco_covid8305[!is.na(univ_testing$nc_sco_covid8305)]
univ_testing$nc_interpretation[univ_testing$nc_sco >= 1] = "reactive"
univ_testing %>% select(-nc_sco_covid8305) -> univ_testing

# S total Ig ----
lc_conn = connect_db()
stot_db = lc_conn %>% tbl("cv2ts") %>% pull(din)
stot_db_l = length(stot_db)

stot = univ_testing %>%
  select(din,
         vitros_s_total_ig_sco = vitros_total_ig_s_sco,
         vitros_s_total_ig_interpretation = vitros_total_ig_s_interpretation) %>%
  filter(!is.na(vitros_s_total_ig_sco) &
           din %in% stot_db == F)

samples_fnames = dbListFields(lc_conn, "samples")
visits = dbReadTable(lc_conn, "visits")

samples_db = lc_conn %>% tbl("samples") %>% pull(din)
samples_db_l = length(samples_db)
samples = left_join(stot, visits, by = "din") %>%
  select(din, sample_status = sample_in_repository) %>%
  mutate(sample_status = case_when(
    sample_status == T ~ "available",
    sample_status == F ~ "unavailable"),
    aliquot_type = NA,
    aliquot_anticoagulant = NA) %>%
  filter(din %in% samples_db == F)

dbBegin(lc_conn)
dbWriteTable(lc_conn, "samples", samples, append = T)
samples_db = lc_conn %>% tbl("samples") %>% pull(din)
if (length(samples_db) - samples_db_l == nrow(samples)) {
  dbCommit(lc_conn)
  post_db_changes("VTL","samples",nrow(samples),"appended")
} else {
  dbRollback(lc_conn)
}

dbBegin(lc_conn)
dbWriteTable(lc_conn, "cv2ts", stot, append = T)
stot_db = lc_conn %>% tbl("cv2ts") %>% pull(din)
if (length(stot_db) - stot_db_l == nrow(stot)) {
  dbCommit(lc_conn)
  post_db_changes("VTL","cv2ts",nrow(stot),"appended")
} else {
  dbRollback(lc_conn)
}

# NC Total Ig ----
ntot_db = lc_conn %>% tbl("nc_total_ig_results") %>% collect()
ntot_db_l = nrow(ntot_db)
ntot_db$nid = paste0(ntot_db$din, ntot_db$nc_total_ig_test)

ntot = univ_testing %>%
  select(din, nc_sco, nc_interpretation) %>%
  mutate(nc_total_ig_test = "roche",
         nid = paste0(din, nc_total_ig_test),
         nc_total_ig_test_date = NA,
         nc_total_ig_reagent_lot = NA) %>%
  filter(!is.na(nc_sco) & nid %in% ntot_db$nid == F)

samples_fnames = dbListFields(lc_conn, "samples")
visits = dbReadTable(lc_conn, "visits")

samples_db = lc_conn %>% tbl("samples") %>% pull(din)
samples_db_l = length(samples_db)
samples = left_join(ntot, visits, by = "din") %>%
  select(din, sample_status = sample_in_repository) %>%
  mutate(sample_status = case_when(
    sample_status == T ~ "available",
    sample_status == F ~ "unavailable"),
    aliquot_type = NA,
    aliquot_anticoagulant = NA) %>%
  filter(din %in% samples_db == F)

dbBegin(lc_conn)
dbWriteTable(lc_conn, "samples", samples, append = T)
samples_db = lc_conn %>% tbl("samples") %>% pull(din)
if (length(samples_db) - samples_db_l == nrow(samples)) {
  dbCommit(lc_conn)
  post_db_changes("VTL","samples",nrow(samples),"appended")
} else {
  dbRollback(lc_conn)
}

ntot %>% select(all_of(dbListFields(lc_conn, "nc_total_ig_results"))) -> ntot
dbBegin(lc_conn)
dbWriteTable(lc_conn, "nc_total_ig_results", ntot, append = T)
ntot_db = lc_conn %>% tbl("nc_total_ig_results") %>% pull(din)
if (length(stot_db) - stot_db_l == nrow(stot)) {
  dbCommit(lc_conn)
  post_db_changes("VTL","nc_total_ig_results",nrow(ntot),"appended")
} else {
  dbRollback(lc_conn)
}


