# ARC ----
# Read in ARC survey results for 2023
asu_1_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/0_Input_Files/4_Survey_Files/ARC/CY23_1/ARC_RDC_CY23_1_Survey_20240510.csv"
arc_surveys_1 = read.csv(asu_1_path, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_1) = tolower(gsub("Y31_", "", names(arc_surveys_1)))
arc_surveys_1 = arc_surveys_1 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "cy23_1_y31") %>%
  select(donor_id, round, question, answer)

asu_2_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/0_Input_Files/4_Survey_Files/ARC/CY23_2/ARC_RDC_CY23_2_Survey_20240510.csv"
arc_surveys_2 = read.csv(asu_2_path, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_2) = tolower(gsub("Y32_", "", names(arc_surveys_2)))
arc_surveys_2 = arc_surveys_2 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "cy23_2_y32") %>%
  select(donor_id, round, question, answer)

asu_3_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/0_Input_Files/4_Survey_Files/ARC/CY23_3/ARC_RDC_CY23_3_Survey_20240507.csv"
arc_surveys_3 = read.csv(asu_3_path, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_3) = tolower(gsub("Y33_", "", names(arc_surveys_3)))
arc_surveys_3 = arc_surveys_3 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "cy23_3_y33") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

arc_surveys = rbind(arc_surveys_1, arc_surveys_2) %>%
  rbind(., arc_surveys_3)

# Read in ARC survey from 2022
asu_4 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/ARC/2022 Q1 AND BASELINE/Repeat_Donor_Initial_Survey_07OCT22.csv"
arc_surveys_4 = read.csv(asu_4, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_4) = tolower(names(arc_surveys_4))
arc_surveys_4 = arc_surveys_4 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "baseline") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

asu_5 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/ARC/2022 Q1 AND BASELINE/Refresh_Non_Responders_Survey_24OCT22.csv"
arc_surveys_5 = read.csv(asu_5, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_5) = tolower(names(arc_surveys_5))
arc_surveys_5 = arc_surveys_5 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "baseline") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

asu_6 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/ARC/2022 Q2/ARC_RDC_FQ2_Survey_06OCT22.csv"
arc_surveys_6 = read.csv(asu_6, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_6) = tolower(names(arc_surveys_6))
arc_surveys_6 = arc_surveys_6 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq2",
         question = gsub("fq2_", "", question)) %>%
  select(donor_id, round, question, answer) %>%
  distinct()

asu_7 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/ARC/2022 Q3/ARC_RDC_FQ3_Survey_22NOV22.csv"
arc_surveys_7 = read.csv(asu_7, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_7) = tolower(names(arc_surveys_7))
arc_surveys_7 = arc_surveys_7 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq3",
         question = gsub("fq3_", "", question)) %>%
  select(donor_id, round, question, answer) %>%
  distinct()

asu_8 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/ARC/2022 Q4/ARC_RDC_FQ4_Survey_09FEB23.csv"
arc_surveys_8 = read.csv(asu_8, as.is = T, colClasses = "character") %>% select(-BCO)
names(arc_surveys_8) = tolower(names(arc_surveys_8))
arc_surveys_8 = arc_surveys_8 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq4",
         question = gsub("fq4_", "", question)) %>%
  select(donor_id, round, question, answer) %>%
  distinct()

# Bind them all together
asu = rbind(arc_surveys_1, arc_surveys_2) %>%
  rbind(., arc_surveys_3) %>%
  rbind(., arc_surveys_4) %>%
  rbind(., arc_surveys_5) %>%
  rbind(., arc_surveys_6) %>%
  rbind(., arc_surveys_7) %>%
  rbind(., arc_surveys_8)

# Compare
sb_conn = connect_db(db_name = "VRI_Sandbox")
survey_db = dbReadTable(sb_conn, "donor_survey_results") %>%
  filter(substr(donor_id,1,3) == "ARC")
summ_db = survey_db %>%
  group_by(survey_round) %>%
  summarize(n = n_distinct(donor_id))
names(summ_db)[2] = "Number of unique donor IDs in Westat file"
names(summ_db)[1] = "Survey Round"

summ_new = asu %>%
  group_by(round) %>%
  summarize(n = n_distinct(donor_id))

names(summ_new)[2] = "Number of unique donor IDs in parsed dataset"
names(summ_new)[1] = "Survey Round"

kableExtra::kable(summ)

# Compare responses per donor
summ_db_1 = survey_db %>%
  group_by(survey_round, donor_id) %>%
  summarize(n = n())
summ_db_2 = summ_db_1 %>%
  group_by(survey_round, n) %>%
  summarize(n_1 = n()) %>%
  pivot_wider(id_cols = "survey_round", names_from = n, names_glue = "{n}_db", values_from = n_1)

summ_new_1 = asu %>%
  group_by(round, donor_id) %>%
  summarize(n = n())
summ_new_2 = summ_new_1 %>%
  group_by(round, n) %>%
  summarize(n_1 = n()) %>%
  pivot_wider(id_cols = "round", names_from = n, names_glue = "{n}_new", values_from = n_1)
names(summ_new_2)[1] = "survey_round"

summ_2 = full_join(summ_db_2, summ_new_2, by = "survey_round")

write.csv(summ_2, "~/Desktop/new_survey_data.csv", row.names = F)

summary(as.factor(asu$question[str_detect(asu$question, "inf")]))

# Infection dates
infection_questions = unique(asu$question[str_detect(asu$question, "_inf_")])

# First infection
first_inf_qs = unique(infection_questions[str_detect(infection_questions, "first")])
first_inf = asu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_first" ~ "cov_inf_pos_first",
    question != "covid_inf_pos_first" ~ question
  )) %>%
  filter(question %in% first_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_first != "98" &
           !is.na(cov_inf_pos_first) &
           !is.na(date_inf_pos_first_month) &
           !is.na(date_inf_pos_first_year)) %>%
  mutate(
    date_inf_pos_first_day = case_when(
      is.na(date_inf_pos_first_day) ~ "15",
      !is.na(date_inf_pos_first_day) ~ date_inf_pos_first_day),
    infxn_date_first = ymd(glue("{date_inf_pos_first_year}-{date_inf_pos_first_month}-{date_inf_pos_first_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_first == "0" ~ "uknown",
      cov_inf_pos_first == "1" ~ "virus test",
      cov_inf_pos_first == "2" ~ "antibody test",
      cov_inf_pos_first == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
    ) %>%
  select(-round, -cov_inf_pos_first,
         -date_inf_pos_first_day, -date_inf_pos_first_month,
         -date_inf_pos_first_year) %>%
  distinct()

# Second infection
second_inf_qs = unique(infection_questions[str_detect(infection_questions, "second")])
second_inf = asu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_second" ~ "cov_inf_pos_second",
    question != "covid_inf_pos_second" ~ question
  )) %>%
  filter(question %in% second_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_second != "98" &
           !is.na(cov_inf_pos_second) &
           !is.na(date_inf_pos_second_month) &
           !is.na(date_inf_pos_second_year)) %>%
  mutate(
    date_inf_pos_second_day = case_when(
      is.na(date_inf_pos_second_day) ~ "15",
      !is.na(date_inf_pos_second_day) ~ date_inf_pos_second_day),
    infxn_date_first = ymd(glue("{date_inf_pos_second_year}-{date_inf_pos_second_month}-{date_inf_pos_second_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_second == "0" ~ "uknown",
      cov_inf_pos_second == "1" ~ "virus test",
      cov_inf_pos_second == "2" ~ "antibody test",
      cov_inf_pos_second == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
  ) %>%
  select(-round, -cov_inf_pos_second,
         -date_inf_pos_second_day, -date_inf_pos_second_month,
         -date_inf_pos_second_year) %>%
  distinct()

# Third infection
third_inf_qs = unique(infection_questions[str_detect(infection_questions, "third")])
third_inf = asu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_third" ~ "cov_inf_pos_third",
    question != "covid_inf_pos_third" ~ question
  )) %>%
  filter(question %in% third_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_third != "98" &
           !is.na(cov_inf_pos_third) &
           !is.na(date_inf_pos_third_month) &
           !is.na(date_inf_pos_third_year)) %>%
  mutate(
    date_inf_pos_third_day = case_when(
      is.na(date_inf_pos_third_day) ~ "15",
      !is.na(date_inf_pos_third_day) ~ date_inf_pos_third_day),
    infxn_date_first = ymd(glue("{date_inf_pos_third_year}-{date_inf_pos_third_month}-{date_inf_pos_third_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_third == "0" ~ "unknown",
      cov_inf_pos_third == "1" ~ "virus test",
      cov_inf_pos_third == "2" ~ "antibody test",
      cov_inf_pos_third == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
  ) %>%
  select(-round, -cov_inf_pos_third,
         -date_inf_pos_third_day, -date_inf_pos_third_month,
         -date_inf_pos_third_year) %>%
  distinct()

arc_infections = rbind(first_inf, second_inf) %>%
  rbind(., third_inf) %>%
  distinct() %>%
  group_by(donor_id) %>%
  arrange(infxn_date_first) %>%
  mutate(infection_count = row_number()) %>%
  ungroup() %>%
  select(donor_id, infection_count, infxn_date_first, infxn_test_type_first,
         infxn_date_plausible, infxn_test_virus)

# Symptoms
symptoms_questions = unique(asu$question[str_detect(asu$question, "sx") & !str_detect(asu$question, "lc")])

# First infection
first_inf_date_qs = c("date_inf_pos_first_month", "date_inf_pos_first_day", "date_inf_pos_first_year")
first_sx_qs = append(first_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "first")]))
first_sx = asu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_first" ~ "cov_inf_pos_first",
    question != "covid_inf_pos_first" ~ question
  )) %>%
  filter(question %in% first_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_first_month) &
         !is.na(date_inf_pos_first_year)) %>%
  mutate(
    date_inf_pos_first_day = case_when(
      is.na(date_inf_pos_first_day) ~ "15",
      !is.na(date_inf_pos_first_day) ~ date_inf_pos_first_day),
    infxn_date_first = ymd(glue("{date_inf_pos_first_year}-{date_inf_pos_first_month}-{date_inf_pos_first_day}")),
  ) %>%
  select(-round, -date_inf_pos_first_day, -date_inf_pos_first_month,
         -date_inf_pos_first_year, -sx_first_nvd, -sx_first_con_runny_nose,
         -sx_first_other_text) %>%
  rename(infxn_anysx = sx_first_any,
         infxn_brain = sx_first_cognition,
         infxn_cough = sx_first_cough,
         infxn_fatigue = sx_first_fatigue,
         infxn_fever = sx_first_fever,
         infxn_head = sx_first_headache,
         infxn_muscle = sx_first_muscle_ache,
         infxn_runny = sx_first_congestion_runny_nose,
         infxn_senses = sx_first_loss_smell_taste,
         infxn_sob = sx_first_sob_diffbreathing,
         infxn_throat = sx_first_sore_throat,
         infxn_other_sx = sx_first_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  )

# Second infection
second_inf_date_qs = c("date_inf_pos_second_month", "date_inf_pos_second_day", "date_inf_pos_second_year")
second_sx_qs = append(second_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "second")]))
second_sx = asu %>%
  filter(question %in% second_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_second_month) &
           !is.na(date_inf_pos_second_year)) %>%
  mutate(
    date_inf_pos_second_day = case_when(
      is.na(date_inf_pos_second_day) ~ "15",
      !is.na(date_inf_pos_second_day) ~ date_inf_pos_second_day),
    infxn_date_first = ymd(glue("{date_inf_pos_second_year}-{date_inf_pos_second_month}-{date_inf_pos_second_day}")),
  ) %>%
  select(-round, -date_inf_pos_second_day, -date_inf_pos_second_month,
         -date_inf_pos_second_year, -sx_second_nvd, -sx_second_con_runny_nose,
         -sx_second_other_text) %>%
  rename(infxn_anysx = sx_second_any,
         infxn_brain = sx_second_cognition,
         infxn_cough = sx_second_cough,
         infxn_fatigue = sx_second_fatigue,
         infxn_fever = sx_second_fever,
         infxn_head = sx_second_headache,
         infxn_muscle = sx_second_muscle_ache,
         infxn_runny = sx_second_congestion_runny_nose,
         infxn_senses = sx_second_loss_smell_taste,
         infxn_sob = sx_second_sob_diffbreathing,
         infxn_throat = sx_second_sore_throat,
         infxn_other_sx = sx_second_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  )

# Third infection
third_inf_date_qs = c("date_inf_pos_third_month", "date_inf_pos_third_day", "date_inf_pos_third_year")
third_sx_qs = append(third_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "third")]))
third_sx = asu %>%
  filter(question %in% third_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_third_month) &
           !is.na(date_inf_pos_third_year)) %>%
  mutate(
    date_inf_pos_third_day = case_when(
      is.na(date_inf_pos_third_day) ~ "15",
      !is.na(date_inf_pos_third_day) ~ date_inf_pos_third_day),
    infxn_date_first = ymd(glue("{date_inf_pos_third_year}-{date_inf_pos_third_month}-{date_inf_pos_third_day}")),
  ) %>%
  select(-round, -date_inf_pos_third_day, -date_inf_pos_third_month,
         -date_inf_pos_third_year, -sx_third_nvd, -sx_third_con_runny_nose,
         -sx_third_other_text) %>%
  rename(infxn_anysx = sx_third_any,
         infxn_brain = sx_third_cognition,
         infxn_cough = sx_third_cough,
         infxn_fatigue = sx_third_fatigue,
         infxn_fever = sx_third_fever,
         infxn_head = sx_third_headache,
         infxn_muscle = sx_third_muscle_ache,
         infxn_runny = sx_third_congestion_runny_nose,
         infxn_senses = sx_third_loss_smell_taste,
         infxn_sob = sx_third_sob_diffbreathing,
         infxn_throat = sx_third_sore_throat,
         infxn_other_sx = sx_third_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  ) %>%
  distinct()

# Merge them together
arc_symptoms = rbind(first_sx, second_sx) %>%
  rbind(., third_sx) %>%
  distinct() %>%
  mutate(ck = paste0(donor_id, infxn_date_first)) %>%
  filter(!duplicated(ck)) %>%
  select(-ck)

dups_ifxn = arc_infections %>%
  mutate(ck = paste0(donor_id, infxn_date_first)) %>%
  filter(ck %in% ck[duplicated(ck)])

arc_infxns = arc_infections %>%
  mutate(ck = paste0(donor_id, infxn_date_first)) %>%
  filter(!ck %in% ck[duplicated(ck)]) %>%
  select(-ck) %>%
  left_join(., arc_symptoms, by = c("donor_id", "infxn_date_first"))

lc_conn = connect_db()
vtl_infxns = dbReadTable(lc_conn, "survey_infections")

sb_conn = connect_db(db_name = "VRI_Sandbox")
donor_survey = dbReadTable(sb_conn, "donor_survey_results")

arc_survey_infxn_db = dbReadTable(sb_conn, "survey_infections")
dbWriteTable(sb_conn, "arc_survey_infections", arc_infxns, overwrite = T)

db_summ = arc_survey_infxn_db %>%
  group_by(infection_count) %>%
  summarize(database_n = n())

new_summ = arc_infxns %>%
  group_by(infection_count) %>%
  summarize(new_n = n())

summ = full_join(db_summ, new_summ)

db_summ = arc_survey_infxn_db %>%
  mutate(infxn_test_type_first = case_when(
    infxn_test_type_first == "Virus Test" ~ "virus test",
    infxn_test_type_first == "Antibody Test" ~ "antibody test",
    infxn_test_type_first == "Diagnosed by Physician" ~ "physician diagnosis"
  )) %>%
  group_by(infxn_test_type_first) %>%
  summarize(database_n = n())

new_summ = arc_infxns %>%
  group_by(infxn_test_type_first) %>%
  summarize(new_n = n())

summ = full_join(db_summ, new_summ)

# Vaccinations
lc_conn = connect_db()
survey_vaccinations = dbReadTable(lc_conn, "survey_vaccinations")
unique(survey_vaccinations$vax_name)

vacc_questions = unique(asu$question[str_detect(asu$question, "vax")])
asu$question[str_detect(asu$question, "covid")] = str_replace(asu$question[str_detect(asu$question, "covid")], "covid", "cov")
arc_survey_vacc = asu %>%
  filter(question %in% vacc_questions & answer != "0") %>%
  distinct() %>%
  pivot_wider(id_cols = c(donor_id, round), names_from = question, values_from = answer)

# Have to split between 2022 and 2023 because Tundeisms in the data
arc_survey_vacc_22 = arc_survey_vacc %>%
  filter(round %in% c(c("baseline","fq2","fq3","fq4")) &
           cov_vax_dose != "98") %>%
  mutate(cov_vax_dose = cov_vax_dose,
         vax_dose1_day = replace_na("15"),
         vax_dose2_day = replace_na("15"),
         vax_dose3_day = replace_na("15"),
         vax_dose4_day = replace_na("15"))

# Had to do these out of mutate for some reason
arc_survey_vacc_22$vax_dose1_date = ymd(paste(arc_survey_vacc_22$vax_dose1_year,arc_survey_vacc_22$vax_dose1_month,
                                              arc_survey_vacc_22$vax_dose1_day, sep = "-"))
arc_survey_vacc_22$vax_dose2_date = ymd(paste(arc_survey_vacc_22$vax_dose2_year, arc_survey_vacc_22$vax_dose2_month,
                                              arc_survey_vacc_22$vax_dose2_day, sep = "-"))
arc_survey_vacc_22$vax_dose3_date = ymd(paste(arc_survey_vacc_22$vax_dose3_year, arc_survey_vacc_22$vax_dose3_month,
                                              arc_survey_vacc_22$vax_dose3_day, sep = "-"))
arc_survey_vacc_22$vax_dose4_date = ymd(paste(arc_survey_vacc_22$vax_dose4_year, arc_survey_vacc_22$vax_dose4_month,
                                              arc_survey_vacc_22$vax_dose4_day, sep = "-"))

arc_survey_vacc_22 = arc_survey_vacc_22 %>%
  select(donor_id, round, cov_vax_dose, vax_dose1_name, vax_dose1_date,
         vax_dose2_name, vax_dose2_date, vax_dose3_name, vax_dose3_date,
         vax_dose4_name, vax_dose4_date)

arc_survey_vacc_22_names = arc_survey_vacc_22 %>%
  pivot_longer(cols = ends_with("name"), names_to = "dose_number", values_to = "vaccine_manufacturer") %>%
  select(donor_id, round, dose_number, vaccine_manufacturer) %>%
  distinct() %>%
  mutate(
    vaccine_manufacturer = case_when(
      str_detect(vaccine_manufacturer, "1") ~ "Pfizer-BioNTech",
      str_detect(vaccine_manufacturer, "2") ~ "Moderna",
      str_detect(vaccine_manufacturer, "3") ~ "Jannsen, Johnson and Johnson, or J&J",
      str_detect(vaccine_manufacturer, "4") ~ "AstraZeneca-Oxford",
      str_detect(vaccine_manufacturer, "5") ~ "Novavax",
      str_detect(vaccine_manufacturer, "6") ~ "Other"),
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4)
    ) %>%
  filter(!is.na(vaccine_manufacturer))

arc_survey_vacc_22_dates = arc_survey_vacc_22 %>%
  pivot_longer(cols = ends_with("date"), names_to = "dose_number", values_to = "vaccination_date") %>%
  select(donor_id, round, dose_number, vaccination_date) %>%
  filter(!is.na(vaccination_date)) %>%
  distinct() %>%
  mutate(
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4)
  )

# Final 2022 survey vaccination data
arc_survey_vacc_22_final = arc_survey_vacc_22_names %>%
  inner_join(., arc_survey_vacc_22_dates, by = c("donor_id", "round", "dose_number")) %>%
  distinct()

arc_survey_vacc_23 = arc_survey_vacc %>%
  filter(round %in% c("cy23_1_y31", "cy23_2_y32", "cy23_3_y33") &
           cov_vax_dose != "98" &
           !is.na(cov_vax_dose)) %>%
  mutate(vax_dose1_day = replace_na("15"),
         vax_dose2_day = replace_na("15"),
         vax_dose3_day = replace_na("15"),
         vax_dose4_day = replace_na("15"),
         vax_dose5_day = replace_na("15"),
         vax_dose6_day = replace_na("15"),
         vax_dose7_day = replace_na("15"),
         vax_dose8_day = replace_na("15"))

# Had to do these out of mutate for some reason
arc_survey_vacc_23$vax_dose1_date = ymd(paste(arc_survey_vacc_23$vax_dose1_year,arc_survey_vacc_23$vax_dose1_month,
                                              arc_survey_vacc_23$vax_dose1_day, sep = "-"))
arc_survey_vacc_23$vax_dose2_date = ymd(paste(arc_survey_vacc_23$vax_dose2_year, arc_survey_vacc_23$vax_dose2_month,
                                              arc_survey_vacc_23$vax_dose2_day, sep = "-"))
arc_survey_vacc_23$vax_dose3_date = ymd(paste(arc_survey_vacc_23$vax_dose3_year, arc_survey_vacc_23$vax_dose3_month,
                                              arc_survey_vacc_23$vax_dose3_day, sep = "-"))
arc_survey_vacc_23$vax_dose4_date = ymd(paste(arc_survey_vacc_23$vax_dose4_year, arc_survey_vacc_23$vax_dose4_month,
                                              arc_survey_vacc_23$vax_dose4_day, sep = "-"))
arc_survey_vacc_23$vax_dose5_date = ymd(paste(arc_survey_vacc_23$vax_dose5_year,arc_survey_vacc_23$vax_dose5_month,
                                              arc_survey_vacc_23$vax_dose5_day, sep = "-"))
arc_survey_vacc_23$vax_dose6_date = ymd(paste(arc_survey_vacc_23$vax_dose6_year, arc_survey_vacc_23$vax_dose6_month,
                                              arc_survey_vacc_23$vax_dose6_day, sep = "-"))
arc_survey_vacc_23$vax_dose7_date = ymd(paste(arc_survey_vacc_23$vax_dose7_year, arc_survey_vacc_23$vax_dose7_month,
                                              arc_survey_vacc_23$vax_dose7_day, sep = "-"))
arc_survey_vacc_23$vax_dose8_date = ymd(paste(arc_survey_vacc_23$vax_dose8_year, arc_survey_vacc_23$vax_dose8_month,
                                              arc_survey_vacc_23$vax_dose8_day, sep = "-"))

arc_survey_vacc_23 = arc_survey_vacc_23 %>%
  select(donor_id, round, cov_vax_dose, vax_dose1_name, vax_dose1_date,
         vax_dose2_name, vax_dose2_date, vax_dose3_name, vax_dose3_date,
         vax_dose4_name, vax_dose4_date, vax_dose5_name, vax_dose5_date,
         vax_dose6_name, vax_dose6_date, vax_dose7_name, vax_dose7_date,
         vax_dose8_name, vax_dose8_date)

arc_survey_vacc_23_names = arc_survey_vacc_23 %>%
  pivot_longer(cols = ends_with("name"), names_to = "dose_number", values_to = "vaccine_manufacturer") %>%
  select(donor_id, round, dose_number, vaccine_manufacturer) %>%
  distinct() %>%
  mutate(
    vaccine_manufacturer = case_when(
      str_detect(vaccine_manufacturer, "1") ~ "Pfizer-BioNTech",
      str_detect(vaccine_manufacturer, "2") ~ "Moderna",
      str_detect(vaccine_manufacturer, "3") ~ "Jannsen, Johnson and Johnson, or J&J",
      str_detect(vaccine_manufacturer, "4") ~ "AstraZeneca-Oxford",
      str_detect(vaccine_manufacturer, "5") ~ "Novavax",
      str_detect(vaccine_manufacturer, "6") ~ "Other"),
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4,
      str_detect(dose_number, "5") ~ 5,
      str_detect(dose_number, "6") ~ 6,
      str_detect(dose_number, "7") ~ 7,
      str_detect(dose_number, "8") ~ 8)
  ) %>%
  filter(!is.na(vaccine_manufacturer))

arc_survey_vacc_23_dates = arc_survey_vacc_23 %>%
  pivot_longer(cols = ends_with("date"), names_to = "dose_number", values_to = "vaccination_date") %>%
  select(donor_id, round, dose_number, vaccination_date) %>%
  filter(!is.na(vaccination_date)) %>%
  distinct() %>%
  mutate(
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4,
      str_detect(dose_number, "5") ~ 5,
      str_detect(dose_number, "6") ~ 6,
      str_detect(dose_number, "7") ~ 7,
      str_detect(dose_number, "8") ~ 8)
  )

arc_survey_vacc_23_final = arc_survey_vacc_23_names %>%
  inner_join(., arc_survey_vacc_23_dates, by = c("donor_id", "round", "dose_number")) %>%
  distinct()

arc_survey_vacc_final = rbind(arc_survey_vacc_22_final, arc_survey_vacc_23_final) %>% distinct()
sb_conn = connect_db(db_name = "VRI_Sandbox")
dbWriteTable(sb_conn, "arc_survey_vaccinations", arc_survey_vacc_final, overwrite = T)

arc_survey_vacc_cleaned = arc_survey_vacc_final %>%
  select(-round) %>%
  distinct() %>%
  group_by(donor_id) %>%
  arrange(vaccination_date) %>%
  mutate(dose_number = row_number()) %>%
  ungroup() %>%
  distinct()

summ = arc_survey_vacc_cleaned %>%
  group_by(dose_number) %>%
  summarize(n = n())

donor_survey = dbReadTable(sb_conn, "donor_survey_results")

write.csv(summ, "~/Desktop/ARC_vaccinations_parsed.csv")

substudy_din_list$cop_4_1[substudy_din_list$din %in% substudy_caco_ri$din[substudy_caco_ri$msd_sample %in% c(1,2)]] = T
sb_conn = connect_db(db_name = "VRI_Sandbox")
dbWriteTable(sb_conn, "substudy_din_list", substudy_din_list, append = T)

arc_sx = arc_sx_pull %>%
  mutate(round = tolower(ques)) %>%
  select(-ques) %>%
  left_join(., asu, by = c("donor_id", "round"), relationship = "many-to-many")


# VTL ----
# Read in VTL survey results for 2023
vsu_1_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/0_Input_Files/4_Survey_Files/VTL/CY23_1/VTL_RDC_CY23_1_Survey_20240501.csv"
vtl_surveys_1 = read.csv(vsu_1_path, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_1) = tolower(gsub("Y31_", "", names(vtl_surveys_1)))
vtl_surveys_1 = vtl_surveys_1 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "cy23_1_y31") %>%
  select(donor_id, round, question, answer)

vsu_2_path = "~/Downloads/RDC_2023_Final_Data_2024-06-12/0_Input_Files/4_Survey_Files/VTL/CY23_2/VTL_RDC_CY23_2_Survey_20240501.csv"
vtl_surveys_2 = read.csv(vsu_2_path, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_2) = tolower(gsub("Y32_", "", names(vtl_surveys_2)))
vtl_surveys_2 = vtl_surveys_2 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "cy23_2_y32") %>%
  select(donor_id, round, question, answer)

vtl_surveys = rbind(vtl_surveys_1, vtl_surveys_2)

# Read in VTL survey from 2022
vsu_4 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/BASELINE/BASELINE WAVE 1/VTL_RDC_Initial_Survey_20221023.csv"
vtl_surveys_4 = read.csv(vsu_4, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_4) = tolower(names(vtl_surveys_4))
vtl_surveys_4 = vtl_surveys_4 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "bl1") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_5 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/BASELINE/BASELINE WAVE 2/VTL_RDC_Initial_Survey_20221027.csv"
vtl_surveys_5 = read.csv(vsu_5, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_5) = tolower(names(vtl_surveys_5))
vtl_surveys_5 = vtl_surveys_5 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "bl2") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_6 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/BASELINE/BASELINE WAVE 3/VTL_RDC_Initial_Survey_20221215.csv"
vtl_surveys_6 = read.csv(vsu_6, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_6) = tolower(names(vtl_surveys_6))
vtl_surveys_6 = vtl_surveys_6 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "bl3") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_7 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/BASELINE/BASELINE WAVE 4/VTL_RDC_Initial_Survey_20230407.csv"
vtl_surveys_7 = read.csv(vsu_7, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_7) = tolower(names(vtl_surveys_7))
vtl_surveys_7 = vtl_surveys_7 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "bl4") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_8 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/2022 Q1/VTL_RDC_Q1_Survey_20221215.csv"
vtl_surveys_8 = read.csv(vsu_8, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_8) = tolower(names(vtl_surveys_8))
vtl_surveys_8 = vtl_surveys_8 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq1") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_9 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/2022 Q2/VTL_RDC_Q2_Survey_20221215.csv"
vtl_surveys_9 = read.csv(vsu_9, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_9) = tolower(names(vtl_surveys_9))
vtl_surveys_9 = vtl_surveys_9 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq2") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_10 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/2022 Q3/VTL_RDC_Q3_Survey_20221215.csv"
vtl_surveys_10 = read.csv(vsu_10, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_10) = tolower(names(vtl_surveys_10))
vtl_surveys_10 = vtl_surveys_10 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq3") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu_11 = "~/Downloads/NBDC_Final_Data_2024-05-03/0_Input_Files/4_Survey_Files/VTL/2022 Q4/VTL_RDC_Q4_Survey_20230407.csv"
vtl_surveys_11 = read.csv(vsu_11, as.is = T, colClasses = "character") %>% select(-BCO)
names(vtl_surveys_11) = tolower(names(vtl_surveys_11))
vtl_surveys_11 = vtl_surveys_11 %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  filter(!is.na(answer) & answer != "" & answer != "9999" & answer != "99") %>%
  mutate(round = "fq4") %>%
  select(donor_id, round, question, answer) %>%
  distinct()

vsu = vtl_surveys %>%
  rbind(., vtl_surveys_4) %>%
  rbind(., vtl_surveys_5) %>%
  rbind(., vtl_surveys_6) %>%
  rbind(., vtl_surveys_7) %>%
  rbind(., vtl_surveys_8) %>%
  rbind(., vtl_surveys_9) %>%
  rbind(., vtl_surveys_10) %>%
  rbind(., vtl_surveys_11) %>%
  mutate(
    question = str_replace(question, "covid", "cov"),
    question = gsub("fq[1-9]_", "", question)
  )

# Infection dates
infection_questions = unique(vsu$question[str_detect(vsu$question, "_inf_")])

# First infection
first_inf_qs = unique(infection_questions[str_detect(infection_questions, "first")])
first_inf = vsu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_first" ~ "cov_inf_pos_first",
    question != "covid_inf_pos_first" ~ question
  )) %>%
  filter(question %in% first_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_first != "98" &
           !is.na(cov_inf_pos_first) &
           !is.na(date_inf_pos_first_month) &
           !is.na(date_inf_pos_first_year)) %>%
  mutate(
    date_inf_pos_first_day = replace_na("15"),
    infxn_date_first = ymd(glue("{date_inf_pos_first_year}-{date_inf_pos_first_month}-{date_inf_pos_first_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_first == "0" ~ "uknown",
      cov_inf_pos_first == "1" ~ "virus test",
      cov_inf_pos_first == "2" ~ "antibody test",
      cov_inf_pos_first == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
  ) %>%
  select(-round, -cov_inf_pos_first, -cov_inf_pos_first_test_type,
         -date_inf_pos_first_day, -date_inf_pos_first_month,
         -date_inf_pos_first_year) %>%
  distinct()

# Second infection
second_inf_qs = unique(infection_questions[str_detect(infection_questions, "second")])
second_inf = vsu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_second" ~ "cov_inf_pos_second",
    question != "covid_inf_pos_second" ~ question
  )) %>%
  filter(question %in% second_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_second != "98" &
           !is.na(cov_inf_pos_second) &
           !is.na(date_inf_pos_second_month) &
           !is.na(date_inf_pos_second_year)) %>%
  mutate(
    date_inf_pos_second_day = replace_na(15),
    infxn_date_first = ymd(glue("{date_inf_pos_second_year}-{date_inf_pos_second_month}-{date_inf_pos_second_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_second == "0" ~ "uknown",
      cov_inf_pos_second == "1" ~ "virus test",
      cov_inf_pos_second == "2" ~ "antibody test",
      cov_inf_pos_second == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
  ) %>%
  select(-round, -cov_inf_pos_second, -cov_inf_pos_second_test_type,
         -date_inf_pos_second_day, -date_inf_pos_second_month,
         -date_inf_pos_second_year) %>%
  distinct()

# Third infection
third_inf_qs = unique(infection_questions[str_detect(infection_questions, "third")])
third_inf = vsu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_third" ~ "cov_inf_pos_third",
    question != "covid_inf_pos_third" ~ question
  )) %>%
  filter(question %in% third_inf_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(cov_inf_pos_third != "98" &
           !is.na(cov_inf_pos_third) &
           !is.na(date_inf_pos_third_month) &
           !is.na(date_inf_pos_third_year)) %>%
  mutate(
    date_inf_pos_third_day = replace_na(15),
    infxn_date_first = ymd(glue("{date_inf_pos_third_year}-{date_inf_pos_third_month}-{date_inf_pos_third_day}")),
    infxn_test_type_first = case_when(
      cov_inf_pos_third == "0" ~ "unknown",
      cov_inf_pos_third == "1" ~ "virus test",
      cov_inf_pos_third == "2" ~ "antibody test",
      cov_inf_pos_third == "3" ~ "physician diagnosis"),
    infxn_date_plausible = case_when(
      infxn_date_first >= ymd("2021-01-01") ~ T,
      infxn_date_first < ymd("2021-01-01") ~ F),
    infxn_test_virus = case_when(
      infxn_test_type_first == "virus test" ~ T,
      infxn_test_type_first != "virus test" ~ F),
  ) %>%
  select(-round, -cov_inf_pos_third, -cov_inf_pos_third_test_type,
         -date_inf_pos_third_day, -date_inf_pos_third_month,
         -date_inf_pos_third_year) %>%
  distinct()

vtl_infections = rbind(first_inf, second_inf) %>%
  rbind(., third_inf) %>%
  distinct() %>%
  group_by(donor_id) %>%
  arrange(infxn_date_first) %>%
  mutate(infection_count = row_number()) %>%
  ungroup() %>%
  select(donor_id, infection_count, infxn_date_first, infxn_test_type_first,
         infxn_date_plausible, infxn_test_virus)

# Symptoms
symptoms_questions = unique(vsu$question[str_detect(vsu$question, "sx") & !str_detect(vsu$question, "lc")])

# First infection
first_inf_date_qs = c("date_inf_pos_first_month", "date_inf_pos_first_day", "date_inf_pos_first_year")
first_sx_qs = append(first_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "first")]))
first_sx = vsu %>%
  mutate(question = case_when(
    question == "covid_inf_pos_first" ~ "cov_inf_pos_first",
    question != "covid_inf_pos_first" ~ question
  )) %>%
  filter(question %in% first_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_first_month) &
           !is.na(date_inf_pos_first_year)) %>%
  mutate(
    date_inf_pos_first_day = replace_na(15),
    infxn_date_first = ymd(glue("{date_inf_pos_first_year}-{date_inf_pos_first_month}-{date_inf_pos_first_day}")),
  ) %>%
  select(-round, -date_inf_pos_first_day, -date_inf_pos_first_month,
         -date_inf_pos_first_year, -sx_first_nvd, -sx_first_con_runny_nose,
         -sx_first_other_text) %>%
  rename(infxn_anysx = sx_first_any,
         infxn_brain = sx_first_cognition,
         infxn_cough = sx_first_cough,
         infxn_fatigue = sx_first_fatigue,
         infxn_fever = sx_first_fever,
         infxn_head = sx_first_headache,
         infxn_muscle = sx_first_muscle_ache,
         infxn_runny = sx_first_congestion_runny_nose,
         infxn_senses = sx_first_loss_smell_taste,
         infxn_sob = sx_first_sob_diffbreathing,
         infxn_throat = sx_first_sore_throat,
         infxn_other_sx = sx_first_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  )

# Second infection
second_inf_date_qs = c("date_inf_pos_second_month", "date_inf_pos_second_day", "date_inf_pos_second_year")
second_sx_qs = append(second_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "second")]))
second_sx = vsu %>%
  filter(question %in% second_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_second_month) &
           !is.na(date_inf_pos_second_year)) %>%
  mutate(
    date_inf_pos_second_day = replace_na(15),
    infxn_date_first = ymd(glue("{date_inf_pos_second_year}-{date_inf_pos_second_month}-{date_inf_pos_second_day}")),
  ) %>%
  select(-round, -date_inf_pos_second_day, -date_inf_pos_second_month,
         -date_inf_pos_second_year, -sx_second_nvd, -sx_second_con_runny_nose,
         -sx_second_other_text) %>%
  rename(infxn_anysx = sx_second_any,
         infxn_brain = sx_second_cognition,
         infxn_cough = sx_second_cough,
         infxn_fatigue = sx_second_fatigue,
         infxn_fever = sx_second_fever,
         infxn_head = sx_second_headache,
         infxn_muscle = sx_second_muscle_ache,
         infxn_runny = sx_second_congestion_runny_nose,
         infxn_senses = sx_second_loss_smell_taste,
         infxn_sob = sx_second_sob_diffbreathing,
         infxn_throat = sx_second_sore_throat,
         infxn_other_sx = sx_second_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  )

# Third infection
third_inf_date_qs = c("date_inf_pos_third_month", "date_inf_pos_third_day", "date_inf_pos_third_year")
third_sx_qs = append(third_inf_date_qs, unique(symptoms_questions[str_detect(symptoms_questions, "third")]))
third_sx = vsu %>%
  filter(question %in% third_sx_qs) %>%
  distinct() %>%
  pivot_wider(id_cols = c("donor_id", "round"),
              names_from = "question",
              values_from = "answer",
              values_fn = max) %>%
  filter(!is.na(date_inf_pos_third_month) &
           !is.na(date_inf_pos_third_year)) %>%
  mutate(
    date_inf_pos_third_day = replace_na(15),
    infxn_date_first = ymd(glue("{date_inf_pos_third_year}-{date_inf_pos_third_month}-{date_inf_pos_third_day}")),
  ) %>%
  select(-round, -date_inf_pos_third_day, -date_inf_pos_third_month,
         -date_inf_pos_third_year, -sx_third_nvd, -sx_third_con_runny_nose) %>%
  rename(infxn_anysx = sx_third_any,
         infxn_brain = sx_third_cognition,
         infxn_cough = sx_third_cough,
         infxn_fatigue = sx_third_fatigue,
         infxn_fever = sx_third_fever,
         infxn_head = sx_third_headache,
         infxn_muscle = sx_third_muscle_ache,
         infxn_runny = sx_third_congestion_runny_nose,
         infxn_senses = sx_third_loss_smell_taste,
         infxn_sob = sx_third_sob_diffbreathing,
         infxn_throat = sx_third_sore_throat,
         infxn_other_sx = sx_third_other) %>%
  distinct() %>%
  mutate(
    infxn_anysx = case_when(
      infxn_anysx == "1" ~ T,
      infxn_anysx == "0" ~ F
    ),
    infxn_brain = case_when(
      infxn_brain == "1" ~ T,
      infxn_brain == "0" ~ F,
    ),
    infxn_cough = case_when(
      infxn_cough == "1" ~ T,
      infxn_cough == "0" ~ F,
    ),
    infxn_fatigue = case_when(
      infxn_fatigue == "1" ~ T,
      infxn_fatigue == "0" ~ F,
    ),
    infxn_fever = case_when(
      infxn_fever == "1" ~ T,
      infxn_fever == "0" ~ F,
    ),
    infxn_head = case_when(
      infxn_head == "1" ~ T,
      infxn_head == "0" ~ F,
    ),
    infxn_muscle = case_when(
      infxn_muscle == "1" ~ T,
      infxn_muscle == "0" ~ F,
    ),
    infxn_runny = case_when(
      infxn_runny == "1" ~ T,
      infxn_runny == "0" ~ F,
    ),
    infxn_senses = case_when(
      infxn_senses == "1" ~ T,
      infxn_senses == "0" ~ F,
    ),
    infxn_sob = case_when(
      infxn_sob == "1" ~ T,
      infxn_sob == "0" ~ F,
    ),
    infxn_throat = case_when(
      infxn_throat == "1" ~ T,
      infxn_throat == "0" ~ F,
    ),
    infxn_other_sx = case_when(
      infxn_other_sx == "1" ~ T,
      infxn_other_sx == "0" ~ F,
    )
  ) %>%
  distinct()

# Merge them together
vtl_symptoms = rbind(first_sx, second_sx) %>%
  rbind(., third_sx) %>%
  distinct() %>%
  mutate(ck = paste0(donor_id, infxn_date_first)) %>%
  filter(!duplicated(ck)) %>%
  select(-ck)

vtl_infxns = vtl_infections %>%
  mutate(ck = paste0(donor_id, infxn_date_first)) %>%
  filter(!ck %in% ck[duplicated(ck)]) %>%
  select(-ck) %>%
  left_join(., vtl_symptoms, by = c("donor_id", "infxn_date_first")) %>%
  distinct()

su = rbind(vsu, asu) %>%
  distinct()

dbWriteTable(sb_conn, "survey_results", su, overwrite = T)

infxns = rbind(arc_infxns, vtl_infxns) %>%
  distinct()

dbWriteTable(sb_conn, "survey_infections", infxns, overwrite = T)

# Vaccinations
vacc_questions = unique(vsu$question[str_detect(vsu$question, "vax")])
vsu$question[str_detect(vsu$question, "covid")] = str_replace(vsu$question[str_detect(vsu$question, "covid")], "covid", "cov")
vtl_survey_vacc = vsu %>%
  distinct() %>%
  filter(question %in% vacc_questions & answer != "0" & !is.na(answer)) %>%
  distinct() %>%
  pivot_wider(id_cols = c(donor_id, round),
              names_from = question,
              values_from = answer)

# Have to split between 2022 and 2023 because Tundeisms in the data
vtl_survey_vacc_22 = vtl_survey_vacc %>%
  mutate(cov_vax_dose = cov_vax_dose,
         vax_dose1_day = replace_na("15"),
         vax_dose2_day = replace_na("15"),
         vax_dose3_day = replace_na("15"),
         vax_dose4_day = replace_na("15"))

# Had to do these out of mutate for some reason
vtl_survey_vacc_22$vax_dose1_date = ymd(paste(vtl_survey_vacc_22$vax_dose1_year,vtl_survey_vacc_22$vax_dose1_month,
                                              vtl_survey_vacc_22$vax_dose1_day, sep = "-"))
vtl_survey_vacc_22$vax_dose2_date = ymd(paste(vtl_survey_vacc_22$vax_dose2_year, vtl_survey_vacc_22$vax_dose2_month,
                                              vtl_survey_vacc_22$vax_dose2_day, sep = "-"))
vtl_survey_vacc_22$vax_dose3_date = ymd(paste(vtl_survey_vacc_22$vax_dose3_year, vtl_survey_vacc_22$vax_dose3_month,
                                              vtl_survey_vacc_22$vax_dose3_day, sep = "-"))
vtl_survey_vacc_22$vax_dose4_date = ymd(paste(vtl_survey_vacc_22$vax_dose4_year, vtl_survey_vacc_22$vax_dose4_month,
                                              vtl_survey_vacc_22$vax_dose4_day, sep = "-"))
vtl_survey_vacc_22$vax_dose5_date = ymd(paste(vtl_survey_vacc_22$vax_dose5_year, vtl_survey_vacc_22$vax_dose5_month,
                                              vtl_survey_vacc_22$vax_dose5_day, sep = "-"))
vtl_survey_vacc_22$vax_dose6_date = ymd(paste(vtl_survey_vacc_22$vax_dose6_year, vtl_survey_vacc_22$vax_dose6_month,
                                              vtl_survey_vacc_22$vax_dose6_day, sep = "-"))
vtl_survey_vacc_22$vax_dose7_date = ymd(paste(vtl_survey_vacc_22$vax_dose7_year, vtl_survey_vacc_22$vax_dose7_month,
                                              vtl_survey_vacc_22$vax_dose7_day, sep = "-"))
vtl_survey_vacc_22$vax_dose8_date = ymd(paste(vtl_survey_vacc_22$vax_dose8_year, vtl_survey_vacc_22$vax_dose8_month,
                                              vtl_survey_vacc_22$vax_dose8_day, sep = "-"))

vtl_survey_vacc_22 = vtl_survey_vacc_22 %>%
  select(donor_id, round, cov_vax_dose, vax_dose1_name, vax_dose1_date,
         vax_dose2_name, vax_dose2_date, vax_dose3_name, vax_dose3_date,
         vax_dose4_name, vax_dose4_date, vax_dose5_name, vax_dose5_date,
         vax_dose6_name, vax_dose6_date)

vtl_survey_vacc_22_names = vtl_survey_vacc_22 %>%
  pivot_longer(cols = ends_with("name"), names_to = "dose_number", values_to = "vaccine_manufacturer") %>%
  select(donor_id, round, dose_number, vaccine_manufacturer) %>%
  distinct() %>%
  mutate(
    vaccine_manufacturer = case_when(
      str_detect(vaccine_manufacturer, "1") ~ "Pfizer-BioNTech",
      str_detect(vaccine_manufacturer, "2") ~ "Moderna",
      str_detect(vaccine_manufacturer, "3") ~ "Jannsen, Johnson and Johnson, or J&J",
      str_detect(vaccine_manufacturer, "4") ~ "AstraZeneca-Oxford",
      str_detect(vaccine_manufacturer, "5") ~ "Novavax",
      str_detect(vaccine_manufacturer, "6") ~ "Other"),
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4)
  ) %>%
  filter(!is.na(vaccine_manufacturer))

vtl_survey_vacc_22_dates = vtl_survey_vacc_22 %>%
  pivot_longer(cols = ends_with("date"), names_to = "dose_number", values_to = "vaccination_date") %>%
  select(donor_id, round, dose_number, vaccination_date) %>%
  filter(!is.na(vaccination_date)) %>%
  distinct() %>%
  mutate(
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4,
      str_detect(dose_number, "5") ~ 5,
      str_detect(dose_number, "6") ~ 6)
  )

# Final 2022 survey vaccination data
vtl_survey_vacc_22_final = vtl_survey_vacc_22_names %>%
  inner_join(., vtl_survey_vacc_22_dates, by = c("donor_id", "round", "dose_number")) %>%
  distinct()

vtl_survey_vacc_23 = vtl_survey_vacc %>%
  filter(round %in% c("cy23_1_y31", "cy23_2_y32", "cy23_3_y33") &
           cov_vax_dose != "98" &
           !is.na(cov_vax_dose)) %>%
  mutate(vax_dose1_day = replace_na("15"),
         vax_dose2_day = replace_na("15"),
         vax_dose3_day = replace_na("15"),
         vax_dose4_day = replace_na("15"),
         vax_dose5_day = replace_na("15"),
         vax_dose6_day = replace_na("15"),
         vax_dose7_day = replace_na("15"),
         vax_dose8_day = replace_na("15"))

# Had to do these out of mutate for some reason
vtl_survey_vacc_23$vax_dose1_date = ymd(paste(vtl_survey_vacc_23$vax_dose1_year,vtl_survey_vacc_23$vax_dose1_month,
                                              vtl_survey_vacc_23$vax_dose1_day, sep = "-"))
vtl_survey_vacc_23$vax_dose2_date = ymd(paste(vtl_survey_vacc_23$vax_dose2_year, vtl_survey_vacc_23$vax_dose2_month,
                                              vtl_survey_vacc_23$vax_dose2_day, sep = "-"))
vtl_survey_vacc_23$vax_dose3_date = ymd(paste(vtl_survey_vacc_23$vax_dose3_year, vtl_survey_vacc_23$vax_dose3_month,
                                              vtl_survey_vacc_23$vax_dose3_day, sep = "-"))
vtl_survey_vacc_23$vax_dose4_date = ymd(paste(vtl_survey_vacc_23$vax_dose4_year, vtl_survey_vacc_23$vax_dose4_month,
                                              vtl_survey_vacc_23$vax_dose4_day, sep = "-"))
vtl_survey_vacc_23$vax_dose5_date = ymd(paste(vtl_survey_vacc_23$vax_dose5_year,vtl_survey_vacc_23$vax_dose5_month,
                                              vtl_survey_vacc_23$vax_dose5_day, sep = "-"))
vtl_survey_vacc_23$vax_dose6_date = ymd(paste(vtl_survey_vacc_23$vax_dose6_year, vtl_survey_vacc_23$vax_dose6_month,
                                              vtl_survey_vacc_23$vax_dose6_day, sep = "-"))
vtl_survey_vacc_23$vax_dose7_date = ymd(paste(vtl_survey_vacc_23$vax_dose7_year, vtl_survey_vacc_23$vax_dose7_month,
                                              vtl_survey_vacc_23$vax_dose7_day, sep = "-"))
vtl_survey_vacc_23$vax_dose8_date = ymd(paste(vtl_survey_vacc_23$vax_dose8_year, vtl_survey_vacc_23$vax_dose8_month,
                                              vtl_survey_vacc_23$vax_dose8_day, sep = "-"))

vtl_survey_vacc_23 = vtl_survey_vacc_23 %>%
  select(donor_id, round, cov_vax_dose, vax_dose1_name, vax_dose1_date,
         vax_dose2_name, vax_dose2_date, vax_dose3_name, vax_dose3_date,
         vax_dose4_name, vax_dose4_date, vax_dose5_name, vax_dose5_date,
         vax_dose6_name, vax_dose6_date, vax_dose7_name, vax_dose7_date,
         vax_dose8_name, vax_dose8_date)

vtl_survey_vacc_23_names = vtl_survey_vacc_23 %>%
  pivot_longer(cols = ends_with("name"), names_to = "dose_number", values_to = "vaccine_manufacturer") %>%
  select(donor_id, round, dose_number, vaccine_manufacturer) %>%
  distinct() %>%
  mutate(
    vaccine_manufacturer = case_when(
      str_detect(vaccine_manufacturer, "1") ~ "Pfizer-BioNTech",
      str_detect(vaccine_manufacturer, "2") ~ "Moderna",
      str_detect(vaccine_manufacturer, "3") ~ "Jannsen, Johnson and Johnson, or J&J",
      str_detect(vaccine_manufacturer, "4") ~ "AstraZeneca-Oxford",
      str_detect(vaccine_manufacturer, "5") ~ "Novavax",
      str_detect(vaccine_manufacturer, "6") ~ "Other"),
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4,
      str_detect(dose_number, "5") ~ 5,
      str_detect(dose_number, "6") ~ 6,
      str_detect(dose_number, "7") ~ 7,
      str_detect(dose_number, "8") ~ 8)
  ) %>%
  filter(!is.na(vaccine_manufacturer))

vtl_survey_vacc_23_dates = vtl_survey_vacc_23 %>%
  pivot_longer(cols = ends_with("date"), names_to = "dose_number", values_to = "vaccination_date") %>%
  select(donor_id, round, dose_number, vaccination_date) %>%
  filter(!is.na(vaccination_date)) %>%
  distinct() %>%
  mutate(
    dose_number = case_when(
      str_detect(dose_number, "1") ~ 1,
      str_detect(dose_number, "2") ~ 2,
      str_detect(dose_number, "3") ~ 3,
      str_detect(dose_number, "4") ~ 4,
      str_detect(dose_number, "5") ~ 5,
      str_detect(dose_number, "6") ~ 6,
      str_detect(dose_number, "7") ~ 7,
      str_detect(dose_number, "8") ~ 8)
  )

vtl_survey_vacc_23_final = vtl_survey_vacc_23_names %>%
  inner_join(., vtl_survey_vacc_23_dates, by = c("donor_id", "round", "dose_number")) %>%
  distinct()

vtl_survey_vacc_final = rbind(vtl_survey_vacc_22_final, vtl_survey_vacc_23_final) %>%
  select(-round, -dose_number) %>%
  distinct() %>%
  group_by(donor_id) %>%
  arrange(vaccination_date) %>%
  mutate(dose_number = row_number()) %>%
  select(donor_id, dose_number, vaccination_date, vaccine_manufacturer)

arc_survey_vacc_final = rbind(arc_survey_vacc_22_final, arc_survey_vacc_23_final) %>%
  select(-round, -dose_number) %>%
  distinct() %>%
  group_by(donor_id) %>%
  arrange(vaccination_date) %>%
  mutate(dose_number = row_number()) %>%
  select(donor_id, dose_number, vaccination_date, vaccine_manufacturer)

survey_vacc_final = rbind(vtl_survey_vacc_final, arc_survey_vacc_final) %>%
  distinct()

dbWriteTable(sb_conn, "survey_vaccinations", survey_vacc_final)

unique(infxns$infxn_test_type_first)

infxns %>% select(-infxn_date_plausible) -> infxns
dbWriteTable(sb_conn, "survey_diagnoses", infxns)


