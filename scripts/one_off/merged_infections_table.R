
# infections_analytic_table
sb_conn = connect_db()

# Look for seroconversions
nc_all = dbReadTable(sb_conn, "nc_total_ig_results") %>%
  filter(nc_total_ig_test == "ortho", nc_final_result == T, nc_dilution_factor == 1) %>%
  group_by(din) %>%
  slice_max(nc_dilution_factor, n = 1) %>%
  distinct()

first_pos = nc_all %>%
  filter(nc_total_ig_test == "ortho" & nc_interpretation == "reactive" & nc_final_result == T) %>%
  left_join(., select(visits_db, donor_id, din, collection_date), by = "din") %>%
  group_by(donor_id) %>%
  slice_min(collection_date) %>%
  ungroup() %>%
  select(donor_id,
         first_positive_din = din,
         first_positive_date = collection_date,
         first_positive_nc_sco = nc_sco) %>%
  distinct()

last_neg = nc_all %>%
  filter(nc_total_ig_test == "ortho" & nc_interpretation == "non-reactive") %>%
  left_join(., select(visits_db, donor_id, din, collection_date), by = "din") %>%
  group_by(donor_id) %>%
  slice_max(collection_date) %>%
  ungroup() %>%
  select(donor_id,
         last_negative_din = din,
         last_negative_date = collection_date,
         last_negative_nc_sco = nc_sco) %>%
  distinct()

# Seroconversions, donor level
seroconversions = inner_join(last_neg, first_pos, by = "donor_id") %>%
  mutate(infection_interval_length = as.numeric(difftime(first_positive_date, last_negative_date, units = "days"))) %>%
  filter(!duplicated(donor_id))

# Waners need a different approach
waners = seroconversions %>%
  filter(last_negative_date >= first_positive_date)

visits_nc = right_join(visits_db, nc_all, by = "din")

for (i in 1:nrow(waners)) {
  neg_date = visits_nc %>%
    filter(donor_id == waners[i,]$donor_id &
             nc_total_ig_test == "ortho" &
             nc_interpretation == "non-reactive" &
             collection_date < waners[i,]$first_positive_date) %>%
    slice_max(collection_date) %>%
    pull(collection_date)
  waners[i,]$last_negative_date = neg_date[1]
}

# Refine seroconversions
seroconversions = seroconversions %>%
  filter(!donor_id %in% waners$donor_id)

waners %>% filter(!is.na(last_negative_date)) -> waners

seroconversions = rbind(seroconversions, waners)
seroconversions$infxn_date_first = as.Date(NA)

# Pull diagnoses
diagnoses = dbReadTable(sb_conn, "survey_infections")

# Give the survey infection dates if they match
for (i in 1:nrow(seroconversions)) {
  subset = diagnoses %>% filter(donor_id == seroconversions[i,]$donor_id)
  infection_date = subset$infxn_date_first[seroconversions[i,]$last_negative_date <= subset$infxn_date_first &
                                             seroconversions[i,]$first_positive_date > subset$infxn_date_first]
  if (length(infection_date) != 0) {
    seroconversions[i,]$infxn_date_first = min(infection_date)
  }
  infection_date = NA
}

# Get midpoint for those without survey infection data
seroconversions = seroconversions %>%
  mutate(infxn_date_first = case_when(
    is.na(infxn_date_first) ~ last_negative_date + floor((first_positive_date - last_negative_date)/2),
    !is.na(infxn_date_first) ~ infxn_date_first
  ))

# Any seroconversion and/or diagnoses, 90 days apart at least
# For seroconversion infection type, three dates: last negative, first positive, infection date (halfway between the two)
seroconversions_merged = seroconversions %>%
  select(donor_id, last_negative_date, last_negative_din, infxn_date_first, first_positive_din, first_positive_date)

# infections table
infections_merged = full_join(infections_db, seroconversions_merged, by = c("donor_id", "infxn_date_first")) %>%
  group_by(donor_id) %>%
  arrange(infxn_date_first) %>%
  mutate(infection_count = row_number())

dbWriteTable(sb_conn, "infections", infections_merged)




