survey_q = dbReadTable(lc_conn, "survey_questions")
survey_r = dbReadTable(lc_conn, "survey_responses")
survey_a = dbReadTable(lc_conn, "survey_answers")

# Vaccination Table ----
# Vaccination_date and approximation flag
vacc_qcat = survey_q$question_id[str_detect(survey_q$question_id, "Vax[1-9]_(?:DD|MM|YY|YYYY)")]
vacc_date = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% vacc_qcat & answer != "-99") %>%
  mutate(vax_qcat_num = substr(question_id,1,4),
         question_id = substr(question_id,6,nchar(question_id)),
         vacc_date_approx = NA) %>%
  pivot_wider(names_from = question_id,
              values_from = answer) %>%
  mutate(vacc_date_approx = case_when(is.na(DD)|is.na(MM)|is.na(YYYY) ~ TRUE,
                                      !is.na(DD) & !is.na(MM) & !is.na(YYYY) ~ FALSE),
         MM = case_when(MM %in% "January" ~ 1,
                        MM %in% "February" ~ 2,
                        MM %in% "March" ~ 3,
                        MM %in% "April" ~ 4,
                        MM %in% "May" ~ 5,
                        MM %in% "June" ~ 6,
                        MM %in% "July" ~ 7,
                        MM %in% "August" ~ 8,
                        MM %in% "September" ~ 9,
                        MM %in% "October" ~ 10,
                        MM %in% "November" ~ 11,
                        MM %in% "December" ~ 12)) %>%
  select(donor_did,
         response_id,
         vax_qcat_num,
         vacc_date_approx,
         DD,
         MM,
         YYYY)

# Vaccination dose
vacc_qcat = survey_q$question_id[str_detect(survey_q$question_id, "Vax[1-9]_dose")]
vacc_dose = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% vacc_qcat & answer != "-99") %>%
  mutate(vax_qcat_num = substr(question_id,1,4),
         question_id = substr(question_id, 6, nchar(question_id))) %>%
  select(donor_did,
         response_id,
         vax_qcat_num,
         dose_no = answer) %>%
  mutate(booster = case_when(str_detect(tolower(dose_no), "yes") ~ T,
                             str_detect(tolower(dose_no), "no") ~ F),
    dose_no = case_when(str_detect(dose_no, "Eighth") ~ 8,
                             str_detect(dose_no, "Fifth") ~ 5,
                             str_detect(dose_no, "Fourth") ~ 4,
                             str_detect(dose_no, "Seventh") ~ 7,
                             str_detect(dose_no, "Third") ~ 3,
                             str_detect(dose_no, "Sixth") ~ 7,
                             str_detect(dose_no, "First") ~ 1,
                             str_detect(dose_no, "Second") ~ 2,
                             str_detect(dose_no, "Tenth") ~ 10))
# Note that for some, the dose # is a yes/no answer
vacc_tbl = full_join(vacc_date, vacc_dose, by = c("donor_did","response_id","vax_qcat_num"))

# Manufacturer
vacc_qcat = survey_q$question_id[str_detect(survey_q$question_id, "Vax[1-9]_name")]
vacc_name = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% vacc_qcat & answer != "-99") %>%
  mutate(vax_qcat_num = substr(question_id,1,4),
         question_id = substr(question_id, 6, nchar(question_id))) %>%
  select(response_id,
         donor_did,
         vax_qcat_num,
         vacc_manufacturer = answer) %>%
  mutate(vacc_manufacturer = case_when(
    vacc_manufacturer %in% c("Janssen, Johnson & Johnson, or J&J","Johnson & Johnson") ~ "Johnson & Johnson",
    vacc_manufacturer == "Moderna" ~ "Moderna",
    vacc_manufacturer == "Pfizer-BioNTech" ~ "Pfizer-BioNTech",
    vacc_manufacturer == "Novavax" ~ "Novavax",
    vacc_manufacturer == "AstraZeneca-Oxford" ~ "AstraZeneca-Oxford",
    vacc_manufacturer == "other" ~ "other"))
vacc_tbl = full_join(vacc_tbl, vacc_name, by = c("donor_did","response_id","vax_qcat_num"))

# Date approximation algorithm using the median of complete date data
dose_ct = unique(vacc_tbl$dose_no[is.na(vacc_tbl$dose_no) == F])
approx_ref = data_frame(dose_ct = as.integer(dose_ct),
                        date_median = NA,
                        YYYY_approx = NA,
                        MM_approx = NA)

for (i in 1:length(dose_ct)) {
  temp = subset(vacc_tbl, vacc_tbl$dose_no == dose_ct[i] & !is.na(vacc_tbl$DD) &
                  !is.na(vacc_tbl$MM) & !is.na(vacc_tbl$YYYY))
  temp$vacc_date = as.Date(paste0(temp$DD,"-",temp$MM,"-",temp$YYYY), format = "%d-%m-%Y")
  approx_ref[i,]$date_median = median(temp$vacc_date)
  approx_ref[i,]$YYYY_approx = year(approx_ref[i,]$date_median)
  approx_ref[i,]$MM_approx = month(approx_ref[i,]$date_median)
  vacc_tbl$MM[is.na(vacc_tbl$MM) & vacc_tbl$dose_no == dose_ct[i]] = approx_ref[i,]$MM_approx
  vacc_tbl$YYYY[is.na(vacc_tbl$YYYY) & vacc_tbl$dose_no == dose_ct[i]] = approx_ref[i,]$YYYY_approx
}

for (i in 1:nrow(vacc_tbl)) {
  if (!is.na(vacc_tbl[i,]$DD) & !is.na(vacc_tbl[i,]$MM) & !is.na(vacc_tbl[i,]$YYYY)) {
    if (vacc_tbl[i,]$DD) {
      if (vacc_tbl[i,]$MM == 2) {
        vacc_tbl[i,]$DD = 28
      }
      if (vacc_tbl[i,]$DD == 31 & vacc_tbl[i,]$MM %in% c(4,6,9,11)) {
        vacc_tbl[i,]$DD = 30
      }
      vacc_tbl[i,]$vacc_date = as.Date(glue("{vacc_tbl[i,]$YYYY}-{vacc_tbl[i,]$MM}-{vacc_tbl[i,]$DD}"))
    }
  }
}

# Diagnoses analytics table ----
#  Diagnosis date and approximation flag ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id, "Pos[1-9]_(?:DD|MM|YY|YYYY)")]
diag_date = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer != "-99") %>%
  mutate(diag_qcat_num = substr(question_id,1,4),
         question_id = substr(question_id,6,nchar(question_id)),
         diag_date_approx = NA) %>%
  pivot_wider(names_from = question_id,
              values_from = answer) %>%
  mutate(diag_date_approx = case_when(is.na(DD)|is.na(MM)|is.na(YYYY) ~ TRUE,
                                      !is.na(DD) & !is.na(MM) & !is.na(YYYY) ~ FALSE),
         MM = case_when(MM %in% "January" ~ 1,
                        MM %in% "February" ~ 2,
                        MM %in% "March" ~ 3,
                        MM %in% "April" ~ 4,
                        MM %in% "May" ~ 5,
                        MM %in% "June" ~ 6,
                        MM %in% "July" ~ 7,
                        MM %in% "August" ~ 8,
                        MM %in% "September" ~ 9,
                        MM %in% "October" ~ 10,
                        MM %in% "November" ~ 11,
                        MM %in% "December" ~ 12)) %>%
  filter(response_consented == T) %>%
  select(donor_did,
         response_id,
         diag_qcat_num,
         diag_date_approx,
         DD,
         MM,
         YYYY)

#  Symptomatic diagnosis ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id, "Pos[1-9]_AnySx") |
                                 str_detect(survey_q$question_id,"Pos_Sx")]
diag_sym = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer %in% c("Unsure","-99") == F) %>%
  mutate(diag_qcat_num = gsub("_","",substr(question_id,1,4)),
         question_id = "symptomatic") %>%
  pivot_wider(names_from = question_id,
              values_from = answer) %>%
  mutate(symptomatic = case_when(symptomatic == "Yes" ~ T,
                                 symptomatic == "No" ~ F)) %>%
  filter(response_consented == T) %>%
  select(donor_did, response_id, diag_qcat_num, symptomatic)

diag_tbl = full_join(diag_date, diag_sym, by = c("donor_did","response_id","diag_qcat_num"))

# Diagnosis symptoms yes/no ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id,"Pos") &
                                   str_detect(survey_q$question_id,"YNU")]
diag_sym = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer %in% c("-99","Unsure") == F) %>%
  mutate(diag_qcat_num = gsub("_","",substr(question_id,1,4)),
         question_id = gsub("_","",substr(question_id,5,nchar(question_id)))) %>%
  mutate(question_id = case_when(str_detect(question_id,"brain") ~ "symptom_brain_fog",
                                 str_detect(question_id,"cough") ~ "symptom_cough",
                                 str_detect(question_id,"fatigue") ~ "symptom_fatigue",
                                 str_detect(question_id,"fever") ~ "symptom_fever",
                                 str_detect(question_id,"head") ~ "symptom_headache",
                                 str_detect(question_id,"memory") ~ "symptom_memory_problem",
                                 str_detect(question_id,"muscle") ~ "symptom_muscle_ache",
                                 str_detect(question_id,"nausea") ~ "symptom_nausea",
                                 str_detect(question_id,"runny") ~ "symptom_runny_nose",
                                 str_detect(question_id,"senses") ~ "symptom_anosmia_ageusia",
                                 str_detect(question_id,"sob") ~ "symptom_trouble_breathing",
                                 str_detect(question_id,"throat") ~ "symptom_sore_throat",
                                 str_detect(question_id,"Other1YNUTEXT") ~ "symptom_other_1",
                                 str_detect(question_id,"Other2YNUTEXT") ~ "symptom_other_2")) %>%
  filter(!is.na(question_id)) %>%
  pivot_wider(names_from = question_id, values_from = answer) %>%
  mutate(symptom_brain_fog = case_when(symptom_brain_fog == "Yes" ~ T,
                                       symptom_brain_fog == "No" ~ F),
         symptom_cough = case_when(symptom_cough == "Yes" ~ T,
                                   symptom_cough == "No" ~ F),
         symptom_fatigue = case_when(symptom_fatigue == "Yes" ~ T,
                                     symptom_fatigue == "No" ~ F),
         symptom_headache = case_when(symptom_headache == "Yes" ~ T,
                                      symptom_headache == "No" ~ F),
         symptom_memory_problem = case_when(symptom_memory_problem == "Yes" ~ T,
                                            symptom_memory_problem == "No" ~ F),
         symptom_muscle_ache = case_when(symptom_muscle_ache == "Yes" ~ T,
                                         symptom_muscle_ache == "No" ~ F),
         symptom_nausea = case_when(symptom_nausea == "Yes" ~ T,
                                    symptom_nausea == "No" ~ F),
         symptom_runny_nose = case_when(symptom_runny_nose == "Yes" ~ T,
                                        symptom_runny_nose == "No" ~ F),
         symptom_anosmia_ageusia = case_when(symptom_anosmia_ageusia == "Yes" ~ T,
                                             symptom_anosmia_ageusia == "No" ~ F),
         symptom_trouble_breathing = case_when(symptom_trouble_breathing == "Yes" ~ T,
                                               symptom_trouble_breathing == "No" ~ F),
         symptom_sore_throat = case_when(symptom_sore_throat == "Yes" ~ T,
                                         symptom_sore_throat == "No" ~ F)) %>%
  filter(response_consented == T) %>%
  select(donor_did, response_id, diag_qcat_num, symptom_brain_fog, symptom_cough,
         symptom_fatigue, symptom_headache, symptom_memory_problem,
         symptom_muscle_ache, symptom_nausea, symptom_runny_nose,
         symptom_anosmia_ageusia, symptom_trouble_breathing,
         symptom_sore_throat, symptom_other_1, symptom_other_2)

diag_tbl = full_join(diag_tbl, diag_sym, by = c("donor_did","response_id","diag_qcat_num"))

# Diagnosis symptoms number of episodes ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id,"Pos") &
                                   str_detect(survey_q$question_id,"episnum")]
diag_sym = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer %in% c("-99","Unsure") == F) %>%
  mutate(diag_qcat_num = gsub("_","",substr(question_id,1,4)),
         question_id = gsub("_","",substr(question_id,5,nchar(question_id)))) %>%
  mutate(question_id = case_when(str_detect(question_id,"brain") ~ "symptom_brain_fog_no_episodes",
                                 str_detect(question_id,"cough") ~ "symptom_cough_no_episodes",
                                 str_detect(question_id,"fatigue") ~ "symptom_fatigue_no_episodes",
                                 str_detect(question_id,"fever") ~ "symptom_fever_no_episodes",
                                 str_detect(question_id,"head") ~ "symptom_headache_no_episodes",
                                 str_detect(question_id,"memory") ~ "symptom_memory_problem_no_episodes",
                                 str_detect(question_id,"muscle") ~ "symptom_muscle_ache_no_episodes",
                                 str_detect(question_id,"nausea") ~ "symptom_nausea_no_episodes",
                                 str_detect(question_id,"runny") ~ "symptom_runny_nose_no_episodes",
                                 str_detect(question_id,"senses") ~ "symptom_anosmia_ageusia_no_episodes",
                                 str_detect(question_id,"sob") ~ "symptom_trouble_breathing_no_episodes",
                                 str_detect(question_id,"throat") ~ "symptom_sore_throat_no_episodes",
                                 str_detect(question_id,"Other1episnumTEXT") ~ "symptom_other_1_no_episodes",
                                 str_detect(question_id,"Other2episnumTEXT") ~ "symptom_other_2_no_episodes"))

# Manual cleanup
diag_sym$answer = gsub("episodes","", diag_sym$answer)
diag_sym$answer = gsub("episode","", diag_sym$answer)
diag_sym$answer = gsub("or more","", diag_sym$answer)
diag_sym$answer = trimws(diag_sym$answer)

# Continue piping
diag_sym = diag_sym %>%
  filter(!is.na(question_id)) %>%
  pivot_wider(names_from = question_id, values_from = answer) %>%
  filter(response_consented == T) %>%
  select(!response_consented, !survey_round, !response_date)

diag_tbl = full_join(diag_tbl, diag_sym, by = c("donor_did","response_id","diag_qcat_num"))

# Diagnosis symptoms length of episodes ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id,"Pos") &
                                   str_detect(survey_q$question_id,"episl")]
diag_sym = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer %in% c("-99","Unsure") == F) %>%
  mutate(diag_qcat_num = gsub("_","",substr(question_id,1,4)),
         question_id = gsub("_","",substr(question_id,5,nchar(question_id)))) %>%
  mutate(question_id = case_when(str_detect(question_id,"brain") ~ "symptom_brain_fog_episode_length",
                                 str_detect(question_id,"cough") ~ "symptom_cough_episode_length",
                                 str_detect(question_id,"fatigue") ~ "symptom_fatigue_episode_length",
                                 str_detect(question_id,"fever") ~ "symptom_fever_episode_length",
                                 str_detect(question_id,"head") ~ "symptom_headache_episode_length",
                                 str_detect(question_id,"memory") ~ "symptom_memory_problem_episode_length",
                                 str_detect(question_id,"muscle") ~ "symptom_muscle_ache_episode_length",
                                 str_detect(question_id,"nausea") ~ "symptom_nausea_episode_length",
                                 str_detect(question_id,"runny") ~ "symptom_runny_nose_episode_length",
                                 str_detect(question_id,"senses") ~ "symptom_anosmia_ageusia_episode_length",
                                 str_detect(question_id,"sob") ~ "symptom_trouble_breathing_episode_length",
                                 str_detect(question_id,"throat") ~ "symptom_sore_throat_episode_length",
                                 str_detect(question_id,"Other1epislTEXT") ~ "symptom_other_1_episode_length",
                                 str_detect(question_id,"Other2epislTEXT") ~ "symptom_other_2_episode_length"))

# Manual cleanup
diag_sym$answer = gsub("Less than 1 day","<1", diag_sym$answer)
diag_sym$answer = gsub("Less than 1","<1", diag_sym$answer)
diag_sym$answer = gsub("6 days or more","6<", diag_sym$answer)
diag_sym$answer = gsub("6 or more","6<", diag_sym$answer)
diag_sym$answer = gsub("3 to 5 days","3-5", diag_sym$answer)
diag_sym$answer = gsub("3 to 5","3-5", diag_sym$answer)
diag_sym$answer = gsub("1 to 2 days","1-2", diag_sym$answer)
diag_sym$answer = gsub("1 to 2","1-2", diag_sym$answer)
diag_sym$answer = trimws(diag_sym$answer)

# Continue piping
diag_sym = diag_sym %>%
  filter(!is.na(question_id)) %>%
  pivot_wider(names_from = question_id, values_from = answer) %>%
  filter(response_consented == T) %>%
  select(-response_consented, -survey_round, -response_date)

diag_tbl = full_join(diag_tbl, diag_sym, by = c("donor_did","response_id","diag_qcat_num"))

#  Diagnosis test type ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id,"Pos[1-9]_TestType")|
                                 str_detect(survey_q$question_id,"Pos[1-9]_AbRapid") |
                                 str_detect(survey_q$question_id,"Pos[1-9]_AgRapid")]
diag_test = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat) %>%
  mutate(diag_qcat_num = substr(question_id,1,4),
         question_id = substr(question_id,6,nchar(question_id))) %>%
  mutate(question_id = case_when(question_id %in% c("AbRapid","AgRapid") ~ "rapid_test",
                                 question_id == "TestType" ~ "test_type")) %>%
  pivot_wider(names_from = question_id, values_from = answer) %>%
  mutate(rapid_test = case_when(rapid_test == "Yes" ~ T,
                                rapid_test == "No" ~ F),
         test_type = case_when(str_detect(test_type, "Virus Test") ~ "naat/pcr",
                              str_detect(test_type, "physician") ~ "physician",
                              str_detect(test_type, "Antibody") ~ "antibody",
                              str_detect(test_type, "Antigen") ~ "antigen",
                              str_detect(test_type, "Unsure") ~ "unsure")) %>%
  filter(!is.na(test_type) & response_consented == T) %>%
  select(donor_did, response_id, diag_qcat_num, rapid_test, test_type)

diag_tbl = full_join(diag_tbl, diag_test, by = c("donor_did","response_id","diag_qcat_num"))

# Diagnosis hospitalization and healthcare-seeking ----
diag_qcat = survey_q$question_id[str_detect(survey_q$question_id,"Pos") &
                                 str_detect(survey_q$question_id,"Hospitalized")]
diag_qcat = append(diag_qcat, survey_q$question_id[str_detect(survey_q$question_id,"Pos") &
                                                   str_detect(survey_q$question_id,"Healthcare")])
diag_hosp = right_join(survey_r, survey_a, by = "response_id") %>%
  filter(question_id %in% diag_qcat & answer != "-99") %>%
  mutate(diag_qcat_num = gsub("_","",substr(question_id,1,4)),
         question_id = gsub("_","",substr(question_id,5,nchar(question_id)))) %>%
  mutate(question_id = case_when(str_detect(question_id,"Healthcare1") ~ "seeked_hospital",
                                 str_detect(question_id,"Healthcare2") ~ "seeked_telehealth",
                                 str_detect(question_id,"Healthcare3") ~ "seeked_urgent_care",
                                 str_detect(question_id,"Healthcare4") ~ "seeked_clinic",
                                 str_detect(question_id,"Healthcare5TEXT") ~ "seeked_other_text",
                                 str_detect(question_id,"Healthcare5") ~ "seeked_other",
                                 str_detect(question_id,"Healthcare6") ~ "no_healthcare_seeked",
                                 str_detect(question_id,"Hospitalized") ~ "hospitalized")) %>%
  pivot_wider(names_from = question_id, values_from = answer) %>%
  mutate(seeked_hospital = case_when(seeked_hospital == "0" ~ F,
                                     seeked_hospital != "0" & !is.na(seeked_hospital) ~ T),
         seeked_telehealth = case_when(seeked_telehealth == "0" ~ F,
                                       seeked_telehealth != "0" & !is.na(seeked_telehealth) ~ T),
         seeked_urgent_care = case_when(seeked_urgent_care == "0" ~ F,
                                        seeked_urgent_care != "0" & !is.na(seeked_urgent_care) ~ T),
         seeked_clinic = case_when(seeked_clinic == "0" ~ F,
                                   seeked_clinic != "0" & !is.na(seeked_clinic) ~ T),
         seeked_other = case_when(seeked_other == "0" ~ F,
                                  seeked_other != "0" & !is.na(seeked_other) ~ T),
         hospitalized = case_when(hospitalized == "No" ~ F,
                                  hospitalized == "Yes" ~ T)) %>%
  filter(response_consented == T) %>%
  select(donor_did, response_id, diag_qcat_num, seeked_hospital,
         seeked_telehealth, seeked_urgent_care, seeked_clinic,
         seeked_other = seeked_other_text, hospitalized)

diag_tbl = full_join(diag_tbl, diag_hosp, by = c("donor_did","response_id","diag_qcat_num"))
diag_tbl$test_date = as.Date(glue("{diag_tbl$YYYY}-{diag_tbl$MM}-{diag_tbl$DD}"))

class(diag_tbl$symptom_brain_fog_no_episodes) = "integer"
class(diag_tbl$symptom_cough_no_episodes) = "integer"
class(diag_tbl$symptom_fatigue_no_episodes) = "integer"
class(diag_tbl$symptom_headache_no_episodes) = "integer"
class(diag_tbl$symptom_memory_problem_no_episodes) = "integer"
class(diag_tbl$symptom_muscle_ache_no_episodes) = "integer"
class(diag_tbl$symptom_nausea_no_episodes) = "integer"
class(diag_tbl$symptom_anosmia_ageusia_no_episodes) = "integer"
class(diag_tbl$symptom_trouble_breathing_no_episodes) = "integer"
class(diag_tbl$symptom_sore_throat_no_episodes) = "integer"
class(diag_tbl$symptom_runny_nose_no_episodes) = "integer"

