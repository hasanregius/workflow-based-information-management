#####################################################
# Qualtrics Survey Data Processing and Accessioning
# v1.2; 4/20/2023
#####################################################

#' @export
process_survey_responses = function(round, raw_responses_dir) {
  # Dependencies ----
  require(dplyr)
  require(DBI)
  require(odbc)
  require(stringr)
  require(svDialogs)
  require(lubridate)

  # Read in the Db connection script
  if (file.exists("scripts/helper_scripts/db_connect.R")) {
    source("scripts/helper_scripts/db_connect.R")
    lc_conn = connect_db()
  } else {
    stop("No Db connection script found")
  }

  # Read in better glue script
  if (file.exists("scripts/helper_scripts/better_glue.R")) {
    source("scripts/helper_scripts/better_glue.R")
  } else {
    stop("No better glue script found")
  }

  # Progress bar function
  progress = function(progress, nqids) {
    percent = (progress/nqids)*100
    cat(sprintf("\r[%-50s] %.0f%% (%.0f of %.0f questions processed)",
                paste0(rep('|', percent/2), collapse = ''), floor(percent),
                progress, nqids))
    if (progress == nqids) {
      cat('\n', fill = T)
    }
  }

  # Raw export data prep ----
  if (missing(raw_responses_dir)) {
    stop("no raw file directory supplied")
  } else {
    if (file.exists(raw_responses_dir)) {
      raw_responses = read.csv(raw_responses_dir)
      raw_responses = raw_responses[c(-1,-2),]
      for (i in 1:length(names(raw_responses))) {
        if (str_detect(names(raw_responses)[i], "_")) {
          split = str_split(names(raw_responses)[i], "_", n = 2)
          names(raw_responses)[i] = split[[1]][2]
        }
      }
    } else {
      stop("raw file directory is invalid")
    }
  }

  # Check the survey round variable
  survey_questions_fnames = dbListFields(lc_conn, "survey_questions")
  valid_rounds = gsub("_qid", "", survey_questions_fnames[3:length(survey_questions_fnames)])
  if (missing(round)) {
    round = dlgInput(message = "please state the survey round (bl1/fu2/etc)")$res
    if (round %in% valid_rounds == F) {
      stop("invalid survey round")
    }
  } else {
    round = tolower(round)
    if (round %in% valid_rounds == F) {
      stop("invalid survey round")
    }
  }

  # Perform survey_responses accessioning ----
  #  Check for merged donor ids before processing
  merged_ids = dbReadTable(lc_conn, "merged_donor_ids")
  if (TRUE %in% (raw_responses$DID %in% merged_ids$previous_donor_did)) {
    to_merge = raw_responses$DID[raw_responses$DID %in% merged_ids$previous_donor_did]
    for (i in 1:length(to_merge)) {
      raw_responses$DID[raw_responses$DID == to_merge[i]] = merged_ids$current_donor_did[merged_ids$previous_donor_id == to_merge[i]]
    }
  }

  # Temporary sequestering of merged donors with missing current DIDs
  raw_responses %>%
    filter(DID %in% to_merge) -> no_current_dids
  write.csv(no_current_dids, "~/Desktop/fu5_raw_responses_no_current_dids.csv", row.names = F)
  raw_responses %>%
    filter(DID %in% to_merge == F) -> raw_responses

  #  Extract and prepare the table from raw responses
  responses = raw_responses %>%
    select(response_id = ResponseId,
           donor_did = DID,
           response_date = RecordedDate,
           response_consented = Consent) %>%
    mutate(response_consented = case_when(
      response_consented == "Yes, I agree to take the survey" ~ T,
      response_consented == "No, I do not agree to take the survey" ~ F),
      response_date = as.Date(response_date,
                              tryFormats = c("%Y-%m-%d %H:%M:%S","%m/%d/%Y %H:%M")),
      survey_round = round)

  #  Prepare the age_at_response variable
  donors = dbReadTable(lc_conn, "donors") %>% select(donor_did, mob)
  responses = left_join(responses, donors, by = "donor_did")
  responses = responses %>%
    mutate(age_at_response = as.integer(time_length(difftime(response_date, mob), "years"))) %>%
    select(-mob)

  # Get the fieldnames from the Db, sequester odd records
  fnames_survey_responses = dbListFields(lc_conn, "survey_responses")
  unknown = subset(responses, responses$donor_did %in% donors$donor_did == F)
  write.csv(unknown, paste0("~/Desktop/",round,"_raw_responses_unknown_dids.csv"), row.names = F)
  responses = subset(responses, responses$donor_did %in% donors$donor_did)

  # If data passes sanity check, move forward
  if (all(fnames_survey_responses %in% names(responses))) {
    # Pulling the required fields
    responses = responses %>%
      select(all_of(fnames_survey_responses)) %>%
      filter(!is.na(response_id) & !is.na(donor_did) & !is.na(response_date) &
               !is.na(response_consented) & !is.na(survey_round))
    # Check if the responses have been accessioned before
    db_responses = lc_conn %>%
      tbl("survey_responses") %>%
      pull(response_id)
    responses_length = length(db_responses)
    # Accessioning sample data with transaction
    dbWithTransaction(lc_conn, {
      # If some samples are already in the Db, we omit those samples
      if (TRUE %in% (responses$response_id %in% db_responses)) {
        cat("Duplicate survey responses found, subsetting for unique response IDs", fill = T)
        responses = subset(responses, responses$response_id %in% db_responses == F)
        dbWriteTable(lc_conn, "survey_responses", responses, append = TRUE)
      } else {
        cat("No duplicate records found. Attempting to import all survey responses", fill = T)
        dbWriteTable(lc_conn, "survey_responses", responses, append = TRUE)
      }
      # Check for if accessioning was successful
      db_responses = lc_conn %>%
        tbl("survey_responses") %>%
        pull(response_id)

      # Rollback in case of error
      if (length(db_responses) - responses_length != nrow(responses)) {
        dsc = length(db_responses) - (responses_length + nrow(responses))
        if (dsc < 0) {
          dsc = dsc - dsc*2
        }
        cat(paste0(dsc," response IDs failed to be imported. Aborting.\n"), fill = T)
        dbBreak()
      } else {
        cat("All survey responses data successfully imported. \n", fill = T)
      }
    })
  }

  # Perform survey_answers accessioning ----
  #  Extract and prepare the answers from the raw export
  fnames_survey_answers = dbListFields(lc_conn, "survey_answers")
  survey_questions = dbReadTable(lc_conn, "survey_questions")
  survey_questions = survey_questions %>%
    filter(!is.na(paste0(round,"_qid"))) %>%
    pull(question_id)
  fnames_round = names(raw_responses)[names(raw_responses) %in% survey_questions]
  # pivot the dataset
  answers = raw_responses %>%
    select(all_of(fnames_round)) %>%
    mutate(response_id = ResponseId) %>%
    filter(Consent == "Yes, I agree to take the survey") %>%
    pivot_longer(!response_id, names_to = "question_id", values_to = "answer")
  # clean up the pivoted dataset
  answers = answers %>%
    filter(answer != "" & !is.na(answer))
  answers = answers %>%
    select(all_of(fnames_survey_answers)) %>%
    filter(response_id %in% db_responses & question_id %in% survey_questions)

  # If data passes sanity check, move forward
  if (all(fnames_survey_answers %in% names(answers))) {
    # Pulling the required fields
    answers = answers %>% select(all_of(fnames_survey_answers))
    answers$uq = paste0(answers$response_id, answers$question_id)
    answers_uq = answers$uq
    # Check if the answers have been accessioned before
    db_answers = dbReadTable(lc_conn, "survey_answers") %>%
      mutate(uq = paste0(response_id, question_id)) %>%
      pull(uq)
    db_answers_length = length(db_answers)
    # If some samples are already in the Db, we omit those samples
    if (TRUE %in% (answers$uq %in% db_answers)) {
      cat("Duplicate survey answers found, subsetting for unique composite key combination", fill = T)
      answers = subset(answers, answers$uq %in% db_answers == F) %>% select(-uq)
    } else {
      cat("No duplicate records found. Attempting to import all survey answers", fill = T)
      answers = answers %>% select(-uq)
    }
    # We have to paginate the accessioning lest Janine's wrath be invoked
    qids = unique(answers$question_id)
    if (nrow(answers) != 0) {
      for (i in 1:length(qids)) {
        progress(i, length(qids))
        answers_temp = answers %>%
          filter(question_id == qids[i])
        dbWriteTable(lc_conn, "survey_answers", answers_temp, append = T)
        Sys.sleep(0.2)
      }
    }
    # Check for if all the answers were accessioned
    db_answers = dbReadTable(lc_conn, "survey_answers") %>%
      mutate(uq = paste0(response_id, question_id)) %>%
      pull(uq)
    if (length(db_answers) - db_answers_length == length(answers_uq)) {
      # Done
      cat("All survey answers successfully imported. \n", fill = T)
    } else {
      cat("Not all answers were accessioned \n", fill = T)
    }
  }
}
