tables = dbListTables(lc_conn)
survey_tables = subset(tables, grepl("survey", tables))
survey_tables = survey_tables[survey_tables %in% c("survey_questions", "survey_responses",
                                                   "survey_answers", "survey_infections") == F]

check_factors = function(df) {
  for (i in 1:ncol(df)) {
    cat(names(df)[i], fill = T)
    if (length(unique(df[,i])) < 10) {
      cat(unique(df[,i]), sep = ", ")
    }
    if ("-99" %in% unique(df[,i]) | -99 %in% unique(df[,i])) {
      cat("> -99 is in the field", fill = T)
    }
    cat("\n", fill = T)
  }
}

# Survey Diagnoses ----
j = 1L
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    pos_healthcare_hospital = case_when(
      pos_healthcare_hospital == "0" ~ F,
      pos_healthcare_hospital != "0" ~ T
    ),
    pos_healthcare_tele = case_when(
      pos_healthcare_tele == "0" ~ F,
      pos_healthcare_tele != "0" ~ T
    ),
    pos_healthcare_urgent = case_when(
      pos_healthcare_urgent == "0" ~ F,
      pos_healthcare_urgent != "0" ~ T
    ),
    pos_healthcare_clinic = case_when(
      pos_healthcare_clinic == "0" ~ F,
      pos_healthcare_clinic != "0" ~ T
    ),
    pos_healthcare_other = case_when(
      pos_healthcare_other == "0" ~ F,
      pos_healthcare_other != "0" ~ T
    ),
    pos_healthcare_none = case_when(
      pos_healthcare_none == "0" ~ F,
      pos_healthcare_none != "0" ~ T
    ),
    pos_hospitalized = case_when(
      pos_hospitalized == "No" ~ F,
      pos_hospitalized == "Yes" ~ T
    )
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)


# Survey Donor Summary ----
j = 2L
survey_tables[survey_tables == survey_tables[j]]
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    positive_ever = case_when(
      positive_ever == "Yes" ~ T,
      positive_ever == "No" ~ F
    ),
    vaccine_ever = case_when(
      vaccine_ever == "Yes" ~ T,
      vaccine_ever == "No" ~ F
    ),
    suspected_ever = case_when(
      suspected_ever == "Yes" ~ T,
      suspected_ever == "No" ~ F
    ),
    declined_ever = case_when(
      declined_ever == "Yes" ~ T,
      declined_ever == "No" ~ F
    ),
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)


# Survey Donor Health ----
j = 3L
survey_tables[survey_tables == survey_tables[j]]
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    smoking = case_when(
      smoking == "Yes" ~ T,
      smoking == "No" ~ F
    ),
    healthconditions_asthma = case_when(
      healthconditions_asthma == "0" ~ F,
      healthconditions_asthma != "0" ~ T
    ),
    healthconditions_resp_other = case_when(
      healthconditions_resp_other == "0" ~ F,
      healthconditions_resp_other != "0" ~ T
    ),
    healthconditions_cardio = case_when(
      healthconditions_cardio == "0" ~ F,
      healthconditions_cardio != "0" ~ T
    ),
    healthconditions_hbp = case_when(
      healthconditions_hbp == "0" ~ F,
      healthconditions_hbp != "0" ~ T
    ),
    healthconditions_diabetes = case_when(
      healthconditions_diabetes == "0" ~ F,
      healthconditions_diabetes != "0" ~ T
    ),
    healthconditions_immune = case_when(
      healthconditions_immune == "0" ~ F,
      healthconditions_immune != "0" ~ T
    ),
    healthconditions_kidney = case_when(
      healthconditions_kidney == "0" ~ F,
      healthconditions_kidney != "0" ~ T
    ),
    healthconditions_liver = case_when(
      healthconditions_liver == "0" ~ F,
      healthconditions_liver != "0" ~ T
    ),
    healthconditions_neuro = case_when(
      healthconditions_neuro == "0" ~ F,
      healthconditions_neuro != "0" ~ T
    ),
    healthconditions_cancer = case_when(
      healthconditions_cancer == "0" ~ F,
      healthconditions_cancer != "0" ~ T
    ),
    healthconditions_none_above = case_when(
      healthconditions_none_above == "0" ~ F,
      healthconditions_none_above != "0" ~ T
    ),
    healthrating = na_if(healthrating, "-99")
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)

# Survey Long Covid ----
j = 4L
survey_tables[survey_tables == survey_tables[j]]
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    lc_sx4wks = case_when(
      lc_sx4wks == "Yes" ~ T,
      lc_sx4wks == "No" ~ F
    ),
    lc_healthcare_unk = case_when(
      lc_healthcare_unk == "0" ~ F,
      lc_healthcare_unk != "0" ~ T
    ),
    lc_healthcare_other = case_when(
      lc_healthcare_other == "0" ~ F,
      lc_healthcare_other != "0" ~ T
    ),
    lc_healthcare_pulm = case_when(
      lc_healthcare_pulm == "0" ~ F,
      lc_healthcare_pulm != "0" ~ T
    ),
    lc_healthcare_pt = case_when(
      lc_healthcare_pt == "0" ~ F,
      lc_healthcare_pt != "0" ~ T
    ),
    lc_healthcare_neuro = case_when(
      lc_healthcare_neuro == "0" ~ F,
      lc_healthcare_neuro != "0" ~ T
    ),
    lc_healthcare_mental = case_when(
      lc_healthcare_mental == "0" ~ F,
      lc_healthcare_mental != "0" ~ T
    ),
    lc_healthcare_derm = case_when(
      lc_healthcare_derm == "0" ~ F,
      lc_healthcare_derm != "0" ~ T
    ),
    lc_healthcare_cardio = case_when(
      lc_healthcare_cardio == "0" ~ F,
      lc_healthcare_cardio != "0" ~ T
    ),
    lc_healthcare_primary = case_when(
      lc_healthcare_primary == "0" ~ F,
      lc_healthcare_primary != "0" ~ T
    )
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)

# Survey Suspected Diagnoses ----
j = 5L
survey_tables[survey_tables == survey_tables[j]]
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    sus_belief_other = case_when(
      sus_belief_other == "0" ~ F,
      sus_belief_other != "0" ~ T
    ),
    sus_belief_negative = case_when(
      sus_belief_negative == "0" ~ F,
      sus_belief_negative != "0" ~ T
    ),
    sus_belief_symptoms = case_when(
      sus_belief_symptoms == "0" ~ F,
      sus_belief_symptoms != "0" ~ T
    ),
    sus_belief_exposed = case_when(
      sus_belief_exposed == "0" ~ F,
      sus_belief_exposed != "0" ~ T
    )
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)

# Survey Vaccine Opinions ----
j = 7L
survey_tables[survey_tables == survey_tables[j]]
df = dbReadTable(lc_conn, survey_tables[survey_tables == survey_tables[j]])

# Check the entries
check_factors(df)

# Mutate
df1 = df %>%
  mutate(
    unvaxxed_barrier_safety = case_when(
      unvaxxed_barrier_safety == "0" ~ F,
      unvaxxed_barrier_safety != "0" ~ T
    ),
    unvaxxed_barrier_beliefs = case_when(
      unvaxxed_barrier_beliefs == "0" ~ F,
      unvaxxed_barrier_beliefs != "0" ~ T
    ),
    unvaxxed_barrier_young = case_when(
      unvaxxed_barrier_young == "0" ~ F,
      unvaxxed_barrier_young != "0" ~ T
    ),
    unvaxxed_barrier_exposure = case_when(
      unvaxxed_barrier_exposure == "0" ~ F,
      unvaxxed_barrier_exposure != "0" ~ T
    ),
    unvaxxed_barrier_transport = case_when(
      unvaxxed_barrier_transport == "0" ~ F,
      unvaxxed_barrier_transport != "0" ~ T
    ),
    unvaxxed_barrier_clinic_unk = case_when(
      unvaxxed_barrier_clinic_unk == "0" ~ F,
      unvaxxed_barrier_clinic_unk != "0" ~ T
    ),
    unvaxxed_barrier_ineligible = case_when(
      unvaxxed_barrier_ineligible == "0" ~ F,
      unvaxxed_barrier_ineligible != "0" ~ T
    ),
    unvaxxed_barrier_clinic_far_hrs = case_when(
      unvaxxed_barrier_clinic_far_hrs == "0" ~ F,
      unvaxxed_barrier_clinic_far_hrs != "0" ~ T
    ),
    unvaxxed_barrier_appointment = case_when(
      unvaxxed_barrier_appointment == "0" ~ F,
      unvaxxed_barrier_appointment != "0" ~ T
    ),
    unvaxxed_barrier_busy = case_when(
      unvaxxed_barrier_busy == "0" ~ F,
      unvaxxed_barrier_busy != "0" ~ T
    ),
    unvaxxed_barrier_childcare = case_when(
      unvaxxed_barrier_childcare == "0" ~ F,
      unvaxxed_barrier_childcare != "0" ~ T
    ),
    unvaxxed_barrier_work = case_when(
      unvaxxed_barrier_work == "0" ~ F,
      unvaxxed_barrier_work != "0" ~ T
    ),
    unvaxxed_barrier_other = case_when(
      unvaxxed_barrier_other == "0" ~ F,
      unvaxxed_barrier_other != "0" ~ T
    ),
    unvaxxed_barrier_unsure = case_when(
      unvaxxed_barrier_unsure == "0" ~ F,
      unvaxxed_barrier_unsure != "0" ~ T
    ),
    vaxxed_motivation_health_self = case_when(
      vaxxed_motivation_health_self == "0" ~ F,
      vaxxed_motivation_health_self != "0" ~ T
    ),
    vaxxed_motivation_health_fam = case_when(
      vaxxed_motivation_health_fam == "0" ~ F,
      vaxxed_motivation_health_fam != "0" ~ T
    ),
    vaxxed_motivation_health_comm = case_when(
      vaxxed_motivation_health_comm == "0" ~ F,
      vaxxed_motivation_health_comm != "0" ~ T
    ),
    vaxxed_motivation_work_school = case_when(
      vaxxed_motivation_work_school == "0" ~ F,
      vaxxed_motivation_work_school != "0" ~ T
    ),
    vaxxed_motivation_social = case_when(
      vaxxed_motivation_social == "0" ~ F,
      vaxxed_motivation_social != "0" ~ T
    ),
    vaxxed_motivation_travel = case_when(
      vaxxed_motivation_travel == "0" ~ F,
      vaxxed_motivation_travel != "0" ~ T
    ),
    vaxxed_motivation_encouraged = case_when(
      vaxxed_motivation_encouraged == "0" ~ F,
      vaxxed_motivation_encouraged != "0" ~ T
    ),
    vaxxed_motivation_unsure = case_when(
      vaxxed_motivation_unsure == "0" ~ F,
      vaxxed_motivation_unsure != "0" ~ T
    ),
    vaxxed_motivation_other = case_when(
      vaxxed_motivation_other == "0" ~ F,
      vaxxed_motivation_other != "0" ~ T
    )
  )

# Check the changes
check_factors(df1)

# Replace the table
dbWriteTable(lc_conn, survey_tables[survey_tables == survey_tables[j]], df1, overwrite = T)
