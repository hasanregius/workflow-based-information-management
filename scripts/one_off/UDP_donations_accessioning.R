udp_raw = read.csv("~/Desktop/Presentations_File_UDP_VRDC_asof20230413edw.csv")

# Basic reformatting ----
udp = udp_raw %>%
  select(din, donor_id, collection_date, gender,
         dhq_vaccination_status_donation = vaccination_status_donation,
         dhq_vaccination_status_ever = vaccination_status_ever,
         zip_code, phlebotomy_status, donation_type, donation_procedure,
         edw_eth_code = DONOR_ETHNICITY_CODE,
         edw_race_desc = RACE_DESCRIPTION,
         map_region = MAP_REGION,
         map_division = MAP_DIVISION,
         location = DRIVE_TYPE_FIXED_MOBILE) %>%
  mutate(din = substr(din, 1, 13),
         donor_id = str_pad(donor_id, width = 7, pad = "0", side = "left"),
         collection_date = as.Date(collection_date, format = "%d%b%Y"),
         gender = tolower(gender), zip_code = str_pad(zip_code, width = 5, pad = "0", side = "left"),
         edw_eth_code = str_pad(edw_eth_code, width = 3, pad = "0", side = "left"),
         location = tolower(location),
         map_region = tolower(map_region),
         map_division = tolower(map_division),
         dhq_vaccination_status_donation = case_when(
           dhq_vaccination_status_donation == "vaccinated" ~ 1,
           dhq_vaccination_status_donation == "not_vaccinated" ~ 0
         ),
         dhq_vaccination_status_ever = case_when(
           dhq_vaccination_status_ever == "vaccinated_ever" ~ 1,
           dhq_vaccination_status_ever == "not_vaccinated_ever" ~ 0
         ),
         phlebotomy_status = case_when(
           phlebotomy_status %in% c("unsuccessful_phlebotomy","no_phlebotomy") ~ 0,
           phlebotomy_status %in% c("sample_only","successful_phlebotomy") ~ 1
         ),
         donation_type = na_if(donation_type, ""),
         donation_procedure = na_if(donation_procedure, ""))

udp$din[nchar(udp$din) == 7] = paste0("A9999",udp$din[nchar(udp$din) == 7],"0")
for (i in 1:nrow(merged)) {
  udp$donor_id[udp$donor_id == merged[i,]$previous_donor_id] = merged[i,]$current_donor_id
}

summary(udp$donor_id %in% donors$donor_id)


# Checking for previously merged donor ids ----
lc_conn = connect_db()
merged = lc_conn %>% tbl("merged_donor_ids") %>% collect()
merged$current_donor_id[merged$current_donor_id %in% udp$donor_id]
for (id in merged$previous_donor_id) {
  udp$donor_id[udp$donor_id == id] = merged$current_donor_id[merged$previous_donor_id == id]
}
donors = lc_conn %>% tbl("donors") %>% collect()

# Checking in CTS' FW export ----
summary(donors$donor_id %in% merged$previous_donor_id)

# donors
udp_donors = read.csv("~/Desktop/backup_udp_donors.csv")
write.csv(udp_donors, "~/Desktop/backup_udp_donors.csv", row.names = F)

mod = udp_donors %>%
  filter(donor_id %in% donors$donor_id) %>%
  mutate(dob = as.Date(dob, format = "%d%b%Y"),
         mob = floor_date(dob),
         vaxplas = 0) %>%
  select(all_of(donors_fnames))

update_db(mod, "donor_id", "rh_group", connect_db(), "dbo", "donors")


merged$merged_ids = paste0(merged$previous_donor_id,merged$current_donor_id)
merged = subset(merged, duplicated(merged$merged_ids) == F)
merged = merged[,1:6]

merged_sub = subset(merged, merged$previous_donor_id %in% udp_donors$donor_id)
for (i in 1:length(merged_sub$previous_donor_id)) {
  udp_donors$donor_id[udp_donors$donor_id == merged_sub[i,]$previous_donor_id] = merged_sub$current_donor_id[merged_sub$previous_donor_id == merged_sub[i,]$previous_donor_id]
}

# Check for merged donor ids
dir = "~/rdc_files/merged_donor_ids/"
filenames = list.files(dir)
i = 5L
merged1 = read.csv(paste0(dir,filenames[i])) %>%
  mutate(previous_donor_id = paste0("VTL",str_pad(SOURCE_DONOR, width = 7, pad = "0", side = "left")),
         filename = filenames[i]) %>%
  filter(previous_donor_id %in% udp_donors$donor_id & previous_donor_id %in% merged$previous_donor_id == F) %>%
  select(all_of(names(temp)))
temp = rbind(merged1, temp)

merged_add = temp %>%
  mutate(current_donor_id = paste0("VTL",str_pad(TARGET_DONOR, width = 7, pad = "0", side = "left")))
merged_add$previous_donor_did = NA
merged_add$current_donor_did = NA

summary(merged_add$previous_donor_id %in% udp_donors$donor_id)
for (id in merged_add$previous_donor_id) {
  merged_add$previous_donor_did[id == merged_add$previous_donor_id] = udp_donors$donor_did[udp_donors$donor_id == id]
}
merged_add %>%
  mutate(change_date = as.Date(as.character(MERGE_DATE_YYYYMMDD), format = "%Y%m%d")) %>%
  select(previous_donor_id, current_donor_id, previous_donor_did, current_donor_did, change_date,
         update_file = filename) -> merged_add
lc_conn = connect_db()

dbWriteTable(lc_conn, "merged_donor_ids", merged_add, append = T)
merged1 = lc_conn %>% tbl("merged_donor_ids") %>% collect()
nrow(merged1)
write.csv(udp_donors, "backup_udp_donors.csv", row.names = F)

dbWriteTable(lc_conn, "merged_donor_ids", merged, overwrite = T)


# FW
dir = "~/OneDrive - Vitalant/Vitalant Repeat Donor Cohorts/Sample Management/"
filenames = list.files(dir, full.names = T, pattern = "\\.csv")
fw = data.frame()
for (i in 1:length(filenames)) {
  fwt = read.csv(filenames[i])
}

i = 10L
fwt = read.csv(filenames[i])
names(fwt)
udp$sample_in_repository[udp$din %in% fwt$LT.Sample.ID] = T

df = select(donors, c("donor_id","mob"))
df$mob = floor_date(df$mob, unit = "month")
lc_conn = connect_db()
update_db(df, "donor_id", "mob", lc_conn, "dbo", "donors")

udp = left_join(udp, df, by = "donor_id")
names(udp)
udp$age_at_donation = as.period(interval(udp$mob, udp$collection_date))$year
visits_fnames = dbListFields(lc_conn, "visits")
udp %>%
  select(all_of(visits_fnames)) -> udp

write.csv(udp, "~/Desktop/udp_donations_backup.csv", row.names = F)
lc_conn = connect_db()
donations = lc_conn %>% tbl("visits") %>% pull(din)
don_ln = length(donations)
udp %>% filter(din %in% donations == F) -> udp
dbBegin(lc_conn)
dbWriteTable(lc_conn, "visits", udp, append = T)
donations = lc_conn %>% tbl("visits") %>% pull(din)
if (length(donations) - don_ln != nrow(udp)) {
  dbRollback(lc_conn)
} else {
  dbCommit(lc_conn)
}

source("./scripts/helper_scripts/backup_db.R")

cv2gs %>% filter(final_result == T) -> cv2gs

# I would check for Vitalant samples:
# - is there one sample per donor per quarter in the list? YES
# - is it true that the tests requested have not already performed

tundeq3 = readxl::read_xlsx("~/Downloads/vtl Q3 Q4 2021 sample 06082023/vtl_sample 06082023.xlsx", sheet = "Q3") %>%
  select(donor_id, din = findin, nc_tested, s_igg_result, s_quantification= s_quant, s_dilution = s_dilut, testing_decision)
tundeq4 = readxl::read_xlsx("~/Downloads/vtl Q3 Q4 2021 sample 06082023/vtl_sample 06082023.xlsx", sheet = "Q4") %>%
  select(donor_id, din = findin, nc_tested, s_igg_result, s_quantification= s_quant, s_dilution = s_dilut, testing_decision)
tunde = rbind(tundeq3, tundeq4)

nc = lc_conn %>% tbl("nc_total_ig_results") %>% collect() %>% filter(din %in% tunde$din)
cv2gs = lc_conn %>% tbl("cv2gs") %>% collect() %>% filter(din %in% tunde$din)

tunde = left_join(tunde, nc, by = "din")
tunde = left_join(tunde, cv2gs, by = "din")

write.csv(tunde_fixed, "~/Desktop/VTL_Q3_Q4_2021_testing_list_06092023.csv", row.names = F)
