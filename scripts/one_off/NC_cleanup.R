historical_subset = historical %>%
  select(
    din = C_din13,
    collection_date = donation_date,
    nc_total_ig_test = E_nc_assay,
    nc_sco = E_nc_sco,
    roche_sco = ECOV_ASCV_SCO,
    roche_interpretation = ECOV_ASCV_8310
  ) %>%
  mutate(
    nc_sco = case_when(
      is.na(nc_sco) & !is.na(roche_sco) ~ roche_sco,
      !is.na(nc_sco) ~ nc_sco),
    nc_total_ig_test = case_when(
      roche_interpretation %in% c("POSITIVE", "NEGATIVE") ~ "roche",
      !is.na(roche_sco) ~ "roche",
      nc_total_ig_test == "Ortho Total Ig NC" ~ "ortho",
      nc_total_ig_test == "Roche Total Ig NC" ~ "roche")) %>%
  filter(!is.na(nc_total_ig_test))

rdc_dins = lc_conn %>% tbl("visits") %>% pull("din")

historical_cleaned = historical_subset %>%
  select(din, collection_date, nc_total_ig_test, nc_sco) %>%
  mutate(
    nc_interpretation = case_when(
      nc_sco >= 1 ~ "reactive",
      nc_sco < 1 ~ "non-reactive"),
    universal_testing = case_when(
      collection_date <= as.Date("2021-07-06") ~ T,
      collection_date > as.Date("2021-07-06") ~ F)
    ) %>%
  filter(din %in% rdc_dins)

for (i in 1:nrow(historical_subset)) {
  if (historical_subset[i,]$nc_total_ig_test == "roche" & is.na(historical_subset[i,]$nc_sco) & !is.na(historical_subset[i,]$roche_sco)) {
    historical_subset[i,]$nc_sco = historical_subset[i,]$roche_sco
  }
}

historical_update = historical_subset %>%
  filter(!is.na(nc_total_ig_test)) %>%
  mutate(nc_interpretation = case_when(

    )) %>%
  select(din, nc_total_ig_test, nc_sco, nc_interpretation)


summary(nc_eg$din %in% historical_update$din | nc_eg$din %in% ss_results$din)
vtl_nc$ck = paste0(vtl_nc$din, vtl_nc$nc_total_ig_test)
historical_update$ck = paste0(historical_update$din, historical_update$nc_total_ig_test)

ss_nc = ss_results %>%
  select(din, nc_total_ig_test = nc_test_name, nc_sco, nc_interpretation) %>%
  filter(nc_total_ig_test != "not_tested" & din %in% nc_eg$din) %>%
  mutate(nc_total_ig_test = case_when(
    nc_total_ig_test == "roche_total_ig_nc" ~ "roche",
    nc_total_ig_test == "ortho_total_ig_nc" ~ "ortho"),
    ck = paste0(din, nc_total_ig_test))

pilot_ortho = pilot %>%
  select(din, nc_total_ig_test = nc_test_name, nc_sco, nc_interpretation) %>%
  mutate(source = "pilot_testing")
nc_update = read.csv(file.choose())
nc_update$source = "serosurvey|edw"
nc_update = rbind(nc_update, pilot_ortho)
write.csv(nc_update, "~/Desktop/nc_update_all_sources.csv", row.names = F)

historical_cts = historical %>%
  select(
    din = C_din13,
    collection_date = donation_date,
    nc_total_ig_test = E_nc_assay,
    nc_sco = E_nc_sco,
    roche_sco = ECOV_ASCV_SCO,
    roche_interpretation = ECOV_ASCV_8310
  ) %>%
  filter(!is.na(nc_sco))

cil_results = rdc_vitros_pull(key_list = visits$din)
cts_results = pull_cts_results()

# Historical results from universal testing
{
cohortdat = haven::read_dta("~/Downloads/DatasetVRDC_uptoJune2022_forWestatplusothers.dta")
cohortdat %>%
  mutate(
    s_total_interpretation = case_when(
      # Prioritise CTS result
      CVDA_RESULTS == "N" | CVDA_RESULTS == "Non-reactive" ~ "non-reactive",
      CVDA_RESULTS == "P" | CVDA_RESULTS == "Reactive" ~ "reactive",
      COVID_8300 == "NEGATIVE" & (CVDA_RESULTS == "" | is.na(CVDA_RESULTS))~ "non-reactive",
      COVID_8300 == "POSITIVE" & (CVDA_RESULTS == "" | is.na(CVDA_RESULTS))~ "reactive"
    )
  ) -> cohortdat
cohortdat %>%
  mutate(
    s_total_sco = case_when(
      !is.na(CVDA_SCO) ~ as.numeric(CVDA_SCO),
      is.na(CVDA_SCO) ~ NA_real_
    )
  ) -> cohortdat

cohortdat %>%
  mutate(
    nc_total_interpretation = case_when(
      ECOV_RESULTS == "POS" ~ "reactive",
      ECOV_RESULTS == "NEG" ~ "non-reactive",
      ASCV_RESULTS == "P" | ASCV_RESULTS == "HP" | ASCV_RESULTS == "LP" ~ "reactive",
      ASCV_RESULTS == "N" ~ "non-reactive",
      COVID_8310 == "POSITIVE" ~ "reactive",
      COVID_8310 == "NEGATIVE" ~ "non-reactive"
    ),
    nc_total_sco = case_when(
      !is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have only ecov
      !is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ECOV_SCO), # have both
      is.na(ECOV_SCO) & !is.na(ASCV_SCO) ~ as.numeric(ASCV_SCO), # have only ascv
      is.na(ECOV_SCO) & is.na(ASCV_SCO) ~ NA_real_ # have neither
    )
  ) -> cohortdat

cohortdat = cohortdat %>%
  select(din, donor_id, collection_date, s_total_sco, s_total_interpretation, nc_total_sco, nc_total_interpretation) %>%
  mutate(
    din = substr(din,1,13),
    donor_id = paste0("VTL",donor_id),
    nc_total_ig_test = "roche",
    nc_total_interpretation = case_when(
      nc_total_sco >= 1 ~ "reactive",
      nc_total_sco < 1 ~ "non-reactive"
    )
  ) %>%
  filter(!is.na(nc_total_sco))
}

# Pulling all the base reports from multiple sources
{
ss_nc = ss_results %>%
  select(din, collection_date, nc_test, nc_sco, nc_interpretation) %>%
  mutate(source = "serosurvey") %>%
  filter(nc_interpretation != "not_tested")

cohortdat_bind = cohortdat %>%
  select(din, collection_date, nc_test = nc_total_ig_test, nc_sco = nc_total_sco, nc_interpretation = nc_total_interpretation) %>%
  mutate(source = "universal")

cts_historical = cts_results %>%
  select(din, collection_date, nc_total_ig_test_date, nc_total_ig_reagent_lot, nc_test = nc_test_name, nc_sco = nc_quant, nc_interpretation) %>%
  mutate(source = "CTS") %>%
  filter(din %in% visits$din)

cil_historical = cil_results %>%
  mutate(nc_test = "ortho", source = "CIL", collection_date = NA) %>%
  select(all_of(names(cts_historical))) %>%
  filter(!is.na(nc_sco) & nc_interpretation != "not_tested")
}

# Priority list -> CIL-CTS-SS-EDW. Generate dataset.
# CIL results
cil_historical %>% mutate(ck = paste0(din,nc_test)) -> cil_nc_legacy
cil_nc_legacy$nc_sco = as.numeric(cil_nc_legacy$nc_sco)
# CTS results
cts_historical %>%
  mutate(nc_test = case_when(
           nc_test == "ortho_total_ig_nc" ~ "ortho",
           nc_test == "roche_total_ig_nc" ~ "roche"),
         ck = paste0(din, nc_test)) %>%
  filter(!is.na(nc_test) & ck %in% cil_nc_legacy$ck == F) -> cts_historical
cts_historical$nc_total_ig_test_date = as.Date(cts_historical$nc_total_ig_test_date, format = "%Y-%m-%d")
cts_historical %>% group_by(ck) %>% slice(n = 1) %>% ungroup() -> cts_historical
nc_legacy = rbind(cil_nc_legacy, cts_historical)
# CDC Serosurvey
ss_nc %>%
  mutate(ck = paste0(din, nc_test),
         collection_date = as.Date(collection_date, format = "%Y-%m-%d")) %>%
  filter(ck %in% nc_legacy$ck == F) -> ss_nc
nc_legacy = rbind(nc_legacy, ss_nc)
# Universal testing
cohortdat_bind %>%
  mutate(ck = paste0(din, nc_test),
         collection_date = as.Date(collection_date, format = "%Y-%m-%d")) %>%
  filter(ck %in% nc_legacy$ck == F) -> cohortdat_bind
nc_legacy = rbind(nc_legacy, cohortdat_bind)

nc_legacy = left_join(nc_legacy, coldates, by = "din")
nc_legacy %>%
  select(-ck) %>%
  mutate(ck = paste0(din, nc_test)) %>%
  filter(collection_date < as.Date("2022-01-01")) -> nc_legacy

dup_dins = nc_legacy$ck[duplicated(nc_legacy$ck)]
nc_legacy$pk = row.names(nc_legacy)

# Priority list -> CIL-CTS-SS-EDW
total_purge = c()
for (i in 1:length(dup_dins)) {
  temp = subset(nc_legacy, nc_legacy$ck %in% dup_dins[i])
  if ("CIL" %in% temp$source) {
    purge_rows = temp$pk[temp$source != "CIL"]
  } else if ("CTS" %in% temp$source) {
    purge_rows = temp$pk[temp$source != "CTS"]
  } else if ("serosurvey" %in% temp$source) {
    purge_rows = temp$pk[temp$source != "serosurvey"]
  } else {
    random = temp %>% slice(n = 1)
    purge_rows = temp$pk[temp$pk != random$pk]
  }
  total_purge = append(total_purge, purge_rows)
}

nc_legacy = unique(nc_legacy)
nc_legacy = nc_legacy %>% filter(pk %in% total_purge == F) %>% select(-pk)

# Doublecheck serosurvey results ----
ws_conn = connect_db(db_name = "warpsurvey")
ws_roche = dbReadTable(ws_conn, "roche_results")
ws_ortho = dbReadTable(ws_conn, "vitros_nc_results")
names(ws_roche) = c("sample_id", "nc_sco", "nc_interpretation")
ws_roche$nc_test = "roche"
names(ws_ortho) = c("sample_id", "nc_sco", "nc_interpretation")
ws_ortho$nc_test = "ortho"
ws_nc = rbind(ws_roche, ws_ortho)

# Pulling the relevant records ----
nc_db = dbReadTable(lc_conn, "nc_total_ig_results")
nc_db = right_join(coldates, nc_db)
nc_db %>% filter(collection_date < as.Date("2022-01-01")) -> nc_db

# Deleting the records in the database ----
deldins = unique(nc_results$din)
# Begin a transaction
dbBegin(lc_conn)
# Send statement
rows_affected = dbExecute(lc_conn,
                          statement = "DELETE FROM dbo.nc_total_ig_results WHERE din = ?",
                          params = list(deldins))
# Commit the transaction if rows_affected == nrow(nc_db)
dbCommit(lc_conn)


# Preparing the newly cleaned legacy NC records for import ----
#  Adding testing metadata for CTS results
cts_nc_meta = cts_results %>%
  select(din, nc_test = nc_test_name, nc_total_ig_test_date, nc_total_ig_reagent_lot) %>%
  mutate(nc_test = case_when(
    nc_test == "ortho_total_ig_nc" ~ "ortho",
    nc_test == "roche_total_ig_nc" ~ "roche"),
    nc_total_ig_test_date = as.Date(nc_total_ig_test_date, format = "%Y-%m-%d"),
    ck = paste0(din, nc_test),
    source = "CTS") %>%
  filter(ck %in% nc_legacy$ck[nc_legacy$source == "CTS"])
#  Adding testing metadata for CIL results
cil_results = rdc_vitros_pull(key_list = nc_legacy$din[nc_legacy$source == "CIL"])
cil_nc_meta = cil_results %>%
  mutate(nc_test = "ortho",
         source = "CIL",
         collection_date = NA,
         ck = paste0(din, nc_test)) %>%
  select(all_of(names(cts_nc_meta)))
#  Binding the two metadata tables
nc_meta = rbind(cts_nc_meta, cil_nc_meta) %>% select(-"din", -"nc_test")
nc_legacy = left_join(nc_legacy, nc_meta, by = c("ck", "source"))

# Importing the cleaned legacy NC results ----
write.csv(nc_legacy, "~/Desktop/nc_legacy_no_ortho_duplicates.csv", row.names = F)

# Accession everything but results with 2 sources from CTS and Serosurvey



















