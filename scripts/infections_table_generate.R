#############################################
# Generate Infections Table from Survey Data
# v1.0; 3/13/2024
#############################################

#' @export
generate_ifxn_table = function() {
  # Dependencies ----
  source("scripts/helper_scripts/db_connect.R")
  require(odbc)
  require(DBI)
  require(readxl)
  dict = read_xlsx("~/Desktop/Data Dictionary_Final Draft_23MAR2023_HS.xlsx")

  # Pull the diagnoses table ----
  lc_conn = connect_db()
  diag = dbReadTable(lc_conn, "survey_diagnoses")

  # Field categorization ----
  ifxns_fieldnames = dict$`Database Field`[dict$`Database Table` == "survey_covid_infections"]
  ifxns_fieldnames = subset(ifxns_fieldnames, !is.na(ifxns_fieldnames))
  ifxns_fieldnames[3] = "infxn_test_type_first"
  ifxns_fieldnames[4] = "infxn_date_first"

  #  Metadata fields
  meta_fnames = subset(names(diag), grepl("pos", names(diag)) == F)
  meta_temp = subset(names(diag), grepl("qcat", names(diag)))
  meta_fnames = append(meta_fnames, meta_temp)
  meta_temp = subset(names(diag), grepl("date", names(diag)) | grepl("test_type", names(diag)))
  meta_fnames = append(meta_fnames, meta_temp)

  #  Episode length fields
  episl_fnames = subset(names(diag), grepl("episl", names(diag)))

  #  Episode number fields
  episnum_fnames = subset(names(diag), grepl("episnum", names(diag)))

  # Healthcare seeking fields
  healthcare_fnames = subset(names(diag), grepl("healthcare", names(diag)))

  #  Symptom fields
  symptoms_fnames = subset(names(diag),
                           names(diag) %in% episnum_fnames == F &
                             names(diag) %in% episl_fnames == F &
                             names(diag) %in% healthcare_fnames == F &
                             names(diag) %in% meta_fnames == F)
  symptoms_fnames = subset(symptoms_fnames, symptoms_fnames %in% c("pos_hospitalized","pos_other2_sx_text","pos_other1_sx_text") == F)
  symptom_list = c("brain","cough","fatigue","fever","head","memory","muscle","nausea","runny","senses","sob","throat")

  # Infections table ----
  infection_ids = unique(diag$infection_id[!is.na(diag$infection_id)])
  infections = as.data.frame(matrix(nrow = length(infection_ids), ncol = length(ifxns_fieldnames)))
  names(infections) = ifxns_fieldnames
  infections$infxn_date_first = as.Date(infections$infxn_date_first)
  infections$infxn_test_virus_date_first = as.Date(infections$infxn_test_virus_date_first)
  infections$infection_id = infection_ids

  for (i in 1:nrow(infections)) {
    # Setup
    id = infections[i,]$infection_id
    temp = diag %>% filter(infection_id == id)

    # donor_did
    infections[i,]$donor_did = unique(temp$donor_did)

    # infxn_date_first
    infections[i,]$infxn_date_first = min(temp$pos_date)

    # infxn_test_type_first
    infections[i,]$infxn_test_type_first = temp$pos_test_type[temp$pos_date == infections[i,]$infxn_date_first][1]

    # infxn_test_virus
    temp = temp %>% mutate(infxn_test_virus = case_when("Virus Test" %in% pos_test_type ~ T,
                                                        "Virus Test" %in% pos_test_type == F ~ F))
    infections[i,]$infxn_test_virus = temp[1,]$infxn_test_virus

    # infxn_test_virus_date_first
    if (infections[i,]$infxn_test_virus == 1) {
      infections[i,]$infxn_test_virus_date_first = min(temp$pos_date[temp$pos_test_type == "Virus Test"])
    }

    # infxn_anysx
    sx_answers = as.character(temp[,symptoms_fnames])
    if (TRUE %in% grepl("Yes", sx_answers)) {
      infections[i,]$infxn_anysx = 1
    } else {
      infections[i,]$infxn_anysx = 0
    }

    # infection symptom fields
    for (k in 1:length(symptom_list)) {
      symptom_fname = paste0("pos_",symptom_list[k])
      symptom_answer = as.character(temp[paste0("pos_",symptom_list[k])])
      if (TRUE %in% grepl("Yes", symptom_answer)) {
        infections[i, paste0("infxn_",symptom_list[k])] = "Yes"
      } else if (TRUE %in% grepl("Unsure", symptom_answer)) {
        infections[i,paste0("infxn_",symptom_list[k])] = "Unsure"
      } else if (TRUE %in% grepl("No", symptom_answer)) {
        infections[i,paste0("infxn_",symptom_list[k])] = "No"
      }
    }

    # infxn_other_sx
    other_sx = as.character(temp[, c("pos_other1_sx", "pos_other2_sx")])
    if (TRUE %in% grepl("Yes", other_sx)) {
      infections[i, "infxn_other_sx"] = "Yes"
    } else if (TRUE %in% grepl("Unsure", other_sx)) {
      infections[i, "infxn_other_sx"] = "Unsure"
    } else if (TRUE %in% grepl("No", other_sx)) {
      infections[i, "infxn_other_sx"] = "No"
    }

    # infxn_healthcare_hospital
    hospital = as.character(temp[,"pos_healthcare_hospital"])
    if (TRUE %in% hospital) {
      infections[i,]$infxn_healthcare_hospital = T
    } else if (FALSE %in% hospital)  {
      infections[i,]$infxn_healthcare_hospital = F
    }

    # infxn_healthcare_tele
    tele = as.character(temp[,"pos_healthcare_tele"])
    if (TRUE %in% tele) {
      infections[i,]$infxn_healthcare_tele = T
    } else if (FALSE %in% tele) {
      infections[i,]$infxn_healthcare_tele = F
    }

    # infxn_healthcare_urgent
    urgent = as.character(temp[,"pos_healthcare_urgent"])
    if (TRUE %in% urgent) {
      infections[i,]$infxn_healthcare_urgent = T
    } else if (FALSE %in% urgent) {
      infections[i,]$infxn_healthcare_urgent = F
    }

    # infxn_healthcare_clinic
    clinic = as.character(temp[,"pos_healthcare_clinic"])
    if (TRUE %in% clinic) {
      infections[i,]$infxn_healthcare_clinic = T
    } else if (FALSE %in% clinic) {
      infections[i,]$infxn_healthcare_clinic = F
    }

    # infxn_healthcare_other
    healthcare_other = as.character(temp[,"pos_healthcare_other"])
    if (TRUE %in% healthcare_other) {
      infections[i,]$infxn_healthcare_other = T
    } else if (FALSE %in% healthcare_other) {
      infections[i,]$infxn_healthcare_other = F
    }

    # infxn_hospitalized
    hospitalized = as.character(temp[,"pos_hospitalized"])
    if (TRUE %in% hospitalized) {
      infections[i,]$infxn_hospitalized = T
    } else if (FALSE %in% hospitalized) {
      infections[i,]$infxn_hospitalized = F
    }
  }

  # Date and infection id formats
  infections$infxn_test_virus_date_first = as.Date(infections$infxn_test_virus_date_first, origin = "1970-01-01")

  # Add whether the infection dates are plausible
  infections = infections %>%
    mutate(infxn_date_plausible = case_when(
      infxn_date_first < as.Date("2020-01-01") ~ 0,
      infxn_date_first >= as.Date("2020-01-01") ~ 1)) %>%
    select(c(names(infections)[1:4], "infxn_date_plausible", names(infections)[5:length(names(infections))]))

  # Write the table in the study database
  dbWriteTable(lc_conn, "survey_infections", infections, overwrite = T)
}
