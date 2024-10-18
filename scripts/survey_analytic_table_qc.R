

# Analytic Tables QC and accessioning
require(yaml)
require(dplyr)
survey_codebook = read_yaml("scripts/config.yml")$survey_codebook

directory = "~/Desktop/epi_analytic_tables_20240108/"
lc_conn = connect_db()
survey_responses = dbReadTable(lc_conn, "survey_responses")
donors = dbReadTable(lc_conn, "donors")
visits = dbReadTable(lc_conn, "visits")

# Diagnoses table ----
{
#  Reading in the table
diagnoses = dbReadTable(lc_conn, "survey_diagnoses") %>%
  mutate(ck = paste0(donor_did, response_id, pos_qcat_num))
s_diagnoses = read.csv(paste0(directory, "survey_diag_upto_FU5.csv"))

#  Pulling the codebook
diagnoses_fnames = survey_codebook$survey_diagnoses

#  Renaming the donor did
names(s_diagnoses)[names(s_diagnoses) == "DID"] = "donor_did"

#  Pulling the appropriate fields and making the required transformations
s_diagnoses$pos_test_type[s_diagnoses$pos_test_type == "Virus Test"] = "PCR/NAAT"
s_diagnoses = s_diagnoses %>%
  mutate(pos_date = na_if(pos_date, pos_date %in% c("-99","NULL")),
         pos_date = as.Date(pos_date, format = "%d%b%Y"),
         pos_date_approx = na_if(pos_date_approx, pos_date_approx %in% c("-99","NULL")),
         pos_date_approx = case_when(
           pos_date_approx == "As reported" ~ "As reported",
           pos_date_approx == "Day Not Provided" ~ "Day not provided",
           pos_date_approx == "Month Not Provided" ~ "Month not provided",
           pos_date_approx == "Day Month Not Provided" ~ "Day and month not provided",
           pos_date_approx == "Year Not Provided" ~ "Year not provided",
           pos_date_approx == "Day Year Not Provided" ~ "Day and year not provided",
           pos_date_approx == "InvalidDateProvided" ~ "Invalid date provided")
         )

#  Making all the -99 and NULL into NAs, as appropriate
for (i in 1:ncol(s_diagnoses)) {
  if (class(s_diagnoses[,i]) == "character") {
    s_diagnoses[,i][s_diagnoses[,i] == "-99"] = NA
    s_diagnoses[,i][s_diagnoses[,i] == "NULL"] = NA
  }
}

#  Check for if all of the donor dids and response ids are consistent
inconsistencies = data.frame()
for (i in 1:nrow(s_diagnoses)) {
  if (s_diagnoses[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_diagnoses[i,]$response_id]) {
    inconsistencies = rbind(inconsistencies, s_diagnoses[i,])
  }
}

# Generate infections table linkage ----
diag = s_diagnoses
diag$ifxn_id = NA
ifxn_id = 0
diag$days_since = NA
donors = unique(diag$donor_did)
diag_ifxn = subset(diag, !is.na(diag$pos_date))
for (i in 1:length(donors)) {
  temp = diag_ifxn %>%
    filter(donor_did == donors[i]) %>%
    arrange(pos_date)
  if(nrow(temp) > 1) {
    ifxn_id = ifxn_id + 1
    diag_ifxn$ifxn_id[diag_ifxn$donor_did == donors[i] & diag_ifxn$pos_date == temp[1,]$pos_date] = ifxn_id
    diag_ifxn$days_since[diag_ifxn$donor_did == donors[i] & diag_ifxn$pos_date == temp[1,]$pos_date] = 0
    for (j in 2:nrow(temp)) {
      days_since = as.integer(difftime(temp[j,]$pos_date, temp[j-1,]$pos_date, units = "days"))
      diag_ifxn$days_since[diag_ifxn$donor_did == donors[i] & diag_ifxn$pos_date == temp[j,]$pos_date] = days_since
      if (days_since > 30) {
        ifxn_id = ifxn_id + 1
      }
      diag_ifxn$ifxn_id[diag_ifxn$donor_did == donors[i] & diag_ifxn$pos_date == temp[j,]$pos_date] = ifxn_id
    }
  } else {
    ifxn_id = ifxn_id + 1
    diag_ifxn$ifxn_id[diag_ifxn$donor_did == donors[i]] = ifxn_id
    diag_ifxn$days_since[diag_ifxn$donor_did == donors[i]] = 0
  }
}

# Saving the infection ID linkage
diag_ifxn$pos_date_int = as.integer(format(diag_ifxn$pos_date, "%Y%m%d"))
infection_id_linkage = diag_ifxn %>%
  mutate(pos_date_int = as.integer(format(pos_date, "%Y%m%d")),
         diag_inf_link = paste0(donor_did, response_id, pos_qcat_num, pos_date_int),
         infection_id = paste0("INFXN",str_pad(ifxn_id, width = 6, pad = "0", side = "left"))) %>%
  select(diag_inf_link, infection_id)

# Adding in the linkage from the infections table
s_diagnoses = s_diagnoses %>%
  mutate(pos_date_int = as.integer(format(pos_date, "%Y%m%d")),
         diag_inf_link = paste0(donor_did, response_id, pos_qcat_num, pos_date_int))
s_diagnoses = left_join(s_diagnoses, infection_id_linkage, by = "diag_inf_link") %>%
  select(all_of(diagnoses_fnames))

# Pulling only the required fields for diagnoses
if (all(names(diagnoses) %in% names(s_diagnoses))) {
  s_diagnoses %>% select(all_of(names(diagnoses))) -> s_diagnoses
}

# Writing the diagnoses table onto the database
dbWriteTable(lc_conn, "survey_diagnoses", s_diagnoses, overwrite = T)
}

# Suspected Diagnoses table ----
{
  #  Reading in the table
  s_sus_diagnoses = read.csv(paste0(directory,"survey_sus_diagnoses_upto_FU5.csv"))
  #  Pulling the codebook
  diagnoses_fnames = survey_codebook$suspected_diagnoses
  #  Ensuring all fieldnames are present
  diagnoses_fnames[diagnoses_fnames %in% names(s_sus_diagnoses) == F]
  names(s_sus_diagnoses)[names(s_sus_diagnoses) == "DID"] = "donor_did"
  #  Pulling the appropriate fields and making the required transformations
  s_sus_diagnoses = s_sus_diagnoses %>%
    select(all_of(diagnoses_fnames)) %>%
    mutate(sus_date = na_if(sus_date, sus_date %in% c("-99","NULL")),
           sus_date = as.Date(sus_date, format = "%d%b%Y"))
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_sus_diagnoses)) {
    if (class(s_sus_diagnoses[,i]) == "character") {
      s_sus_diagnoses[,i][s_sus_diagnoses[,i] == "-99"] = NA
      s_sus_diagnoses[,i][s_sus_diagnoses[,i] == "NULL"] = NA
    }
  }
  #  Check for if all of the donor dids and response ids are consistent
  inconsistencies = data.frame()
  for (i in 1:nrow(s_sus_diagnoses)) {
    if (s_sus_diagnoses[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_sus_diagnoses[i,]$response_id]) {
      inconsistencies = rbind(inconsistencies, s_sus_diagnoses[i,])
    }
  }
  #  If it all checks out, copy to the Db
  sus_diag_fnames = dbListFields(lc_conn, "survey_suspected_diagnoses")
  if (all(sus_diag_fnames %in% names(s_sus_diagnoses))) {
    s_sus_diagnoses %>% select(all_of(sus_diag_fnames)) -> s_sus_diagnoses
  }
  dbWriteTable(connect_db(), "survey_suspected_diagnoses", s_sus_diagnoses, overwrite = T)
}

# Vaccinations table ----
{
  #  Reading in the table
  s_vaccinations = read.csv(paste0(directory,"survey_vaccinations_upto_FU5.csv"))
  #  Pulling the codebook
  vaccinations_fnames = survey_codebook$vaccinations
  #  Ensuring all fieldnames are present
  vaccinations_fnames[vaccinations_fnames %in% names(s_vaccinations) == F]
  names(s_vaccinations)[names(s_vaccinations) == "DID"] = "donor_did"
  names(s_vaccinations)[names(s_vaccinations) == "Response_Id"] = "response_id"
  #  Pulling the appropriate fields and making the required transformations
  s_vaccinations = s_vaccinations %>%
    select(all_of(vaccinations_fnames)) %>%
    mutate(vax_date = na_if(vax_date, vax_date %in% c("-99","NULL")),
           vax_date = as.Date(vax_date, format = "%d%b%Y"))
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_vaccinations)) {
    if (class(s_vaccinations[,i]) == "character") {
      s_vaccinations[,i][s_vaccinations[,i] == "-99"] = NA
      s_vaccinations[,i][s_vaccinations[,i] == "NULL"] = NA
    }
  }

  #  Omitting all N/As from key fields
  s_vaccinations = s_vaccinations %>%
    filter(!is.na(response_id) & !is.na(donor_did) & !is.na(vax_qcat_num))
  #  Check for if all of the donor dids and response ids are consistent
  inconsistencies = data.frame()
  for (i in 1:nrow(s_vaccinations)) {
    if (s_vaccinations[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_vaccinations[i,]$response_id]) {
      inconsistencies = rbind(inconsistencies, s_vaccinations[i,])
    }
  }

  #  If it all checks out, copy to the Db
  survey_vacc_fnames = dbListFields(lc_conn, "survey_vaccinations")
  if (all(survey_vacc_fnames %in% names(s_vaccinations))) {
    s_vaccinations %>% select(all_of(survey_vacc_fnames)) -> s_vaccinations
  }
  dbWriteTable(lc_conn, "survey_vaccinations", s_vaccinations, overwrite = T)
}

# Vaccination Opinions ----
{
  #  Reading in the table
  s_vax_opinions = read.csv(paste0(directory, "survey_vaxopinions_upto_FU5.csv"))
  #  Pulling the codebook
  vaxopinions_fnames = survey_codebook$vaccination_opinions
  #  Ensuring all fieldnames are present
  vaxopinions_fnames[vaxopinions_fnames %in% names(s_vax_opinions) == F]
  names(s_vax_opinions)[names(s_vax_opinions) == "DID"] = "donor_did"
  names(s_vax_opinions)[names(s_vax_opinions) == "Response_Id"] = "response_id"
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_vax_opinions)) {
    if (class(s_vax_opinions[,i]) == "character") {
      s_vax_opinions[,i][s_vax_opinions[,i] == "-99"] = NA
      s_vax_opinions[,i][s_vax_opinions[,i] == "NULL"] = NA
    }
  }
  #  Omitting all N/As from key fields
  donors = dbReadTable(lc_conn, "donors")
  s_vax_opinions = s_vax_opinions %>%
    select(all_of(vaxopinions_fnames)) %>%
    filter(!is.na(response_id) & !is.na(donor_did) & donor_did %in% donors$donor_did)
  #  Check for if all of the donor dids and response ids are consistent
  inconsistencies = data.frame()
  for (i in 1:nrow(s_vax_opinions)) {
    if (s_vax_opinions[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_vax_opinions[i,]$response_id]) {
      inconsistencies = rbind(inconsistencies, s_vax_opinions[i,])
    }
  }
  #  If it all checks out, copy to the Db
  vaccination_opinion_fnames = dbListFields(lc_conn, "survey_vaxopinions")
  if (all(vaccination_opinion_fnames %in% names(s_vax_opinions))) {
    s_vax_opinions %>% select(all_of(vaccination_opinion_fnames)) -> s_vax_opinions
  }
  dbWriteTable(lc_conn, "survey_vaxopinions", s_vax_opinions, overwrite = T)
}

# Donor Health Conditions ----
{
  #  Reading in the table
  s_donor_summ = read.csv(paste0(directory, "survey_donorhealth_upto_FU5.csv"))
  #  Pulling the codebook
  donorhealth_fnames = survey_codebook$donorhealth
  #  Ensuring all fieldnames are present
  donorhealth_fnames[donorhealth_fnames %in% names(s_donor_summ) == F]
  names(s_donor_summ)[names(s_donor_summ) == "DID"] = "donor_did"
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_donor_summ)) {
    if (class(s_donor_summ[,i]) == "character") {
      s_donor_summ[,i][s_donor_summ[,i] == "-99"] = NA
      s_donor_summ[,i][s_donor_summ[,i] == "NULL"] = NA
    }
  }
  #  Omitting all N/As from key fields
  s_donor_summ = s_donor_summ %>%
    select(all_of(donorhealth_fnames)) %>%
    filter(!is.na(response_id) & !is.na(donor_did) & donor_did %in% donors$donor_did)
  #  Check for if all of the donor dids and response ids are consistent
  inconsistencies = data.frame()
  for (i in 1:nrow(s_donor_summ)) {
    if (s_donor_summ[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_donor_summ[i,]$response_id]) {
      inconsistencies = rbind(inconsistencies, s_donor_summ[i,])
    }
  }
  #  If it all checks out, copy to the Db
  donor_summ_fnames = dbListFields(lc_conn, "survey_donorhealth")
  if (all(donor_summ_fnames %in% names(s_donor_summ))) {
    s_donor_summ %>% select(all_of(donor_summ_fnames)) -> s_donor_summ
  }
  dbWriteTable(lc_conn, "survey_donorhealth", s_donor_summ, overwrite = T)
}

# Long Covid ----
{
  #  Reading in the table
  s_long_covid = read.csv(paste0(directory,"survey_longcovid_upto_FU5.csv"))
  #  Pulling the codebook
  longcovid_fnames = survey_codebook$longcovid
  #  Ensuring all fieldnames are present
  longcovid_fnames[longcovid_fnames %in% names(s_long_covid) == F]
  names(s_long_covid)[names(s_long_covid) == "DID"] = "donor_did"
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_long_covid)) {
    if (class(s_long_covid[,i]) == "character") {
      s_long_covid[,i][s_long_covid[,i] == "-99"] = NA
      s_long_covid[,i][s_long_covid[,i] == "NULL"] = NA
    }
  }
  #  Omitting all N/As from key fields
  s_long_covid = s_long_covid %>%
    select(all_of(longcovid_fnames)) %>%
    filter(!is.na(response_id) & !is.na(donor_did) & donor_did %in% donors$donor_did)
  #  Check for if all of the donor dids and response ids are consistent
  inconsistencies = data.frame()
  for (i in 1:nrow(s_long_covid)) {
    if (s_long_covid[i,]$donor_did != survey_responses$donor_did[survey_responses$response_id == s_long_covid[i,]$response_id]) {
      inconsistencies = rbind(inconsistencies, s_long_covid[i,])
    }
  }
  #  If it all checks out, copy to the Db
  long_covid_fnames = dbListFields(lc_conn, "survey_longcovid")
  if (all(long_covid_fnames %in% names(s_long_covid))) {
    s_long_covid %>% select(all_of(long_covid_fnames)) -> s_long_covid
  }
  dbWriteTable(lc_conn, "survey_longcovid", s_long_covid, overwrite = T)
}

# Donor Summary ----
{
  #  Reading in the table
  s_donor_summ = read.csv(paste0(directory,"SURVEY_DONOR.csv"))
  #  Pulling the codebook
  donor_summ_fnames = survey_codebook$survey_donor
  #  Ensuring all fieldnames are present
  names(s_donor_summ) = tolower(names(s_donor_summ))
  names(s_donor_summ)[names(s_donor_summ) == "did"] = "donor_did"
  s_donor_summ$date_last_responded = NA
  s_donor_summ$date_last_responded = as.Date(s_donor_summ$date_last_responded)
  #  Making all the -99 and NULL into NAs, as appropriate
  for (i in 1:ncol(s_donor_summ)) {
    if (class(s_donor_summ[,i]) == "character") {
      s_donor_summ[,i][s_donor_summ[,i] == "-99"] = NA
      s_donor_summ[,i][s_donor_summ[,i] == "NULL"] = NA
      s_donor_summ[,i][s_donor_summ[,i] == ""] = NA
    }
  }
  #  Omitting all N/As from key fields
  s_donor_summ = s_donor_summ %>%
    select(all_of(donor_summ_fnames)) %>%
    filter(!is.na(donor_did) & donor_did %in% donors$donor_did)
  #  Date of last response
  for (i in 1:nrow(s_donor_summ)) {
    s_donor_summ[i,]$date_last_responded = max(survey_responses$response_date[survey_responses$donor_did == s_donor_summ[i,]$donor_did])
  }
  #  If it all checks out, copy to the Db
  lc_conn = connect_db()
  dbWriteTable(lc_conn, "survey_donor", s_donor_summ, overwrite = T)
}
