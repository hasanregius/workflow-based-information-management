###################################
# Parse Qualtrics Survey Questions
# v1.1; 3/13/2024
###################################

#' @export
parse_qualtrics_questions = function(questions_dir) {
  # Dependencies ----
  require(readxl)
  require(dplyr)
  require(stringr)

  # Question directory supply check
  if (missing(questions_dir)) {
    stop("missing excel question file")
  }

  # Cleanup function
  qualtrics_cleanup = function (data_frame) {
    require(stringr)
    for (i in 1:ncol(data_frame)) {
      if (str_detect(names(data_frame)[i], "qid")) {
        data_frame[,i] = gsub('"', '', x = data_frame[,i])
        data_frame[,i] = gsub('ImportId:', '', x = data_frame[,i])
        data_frame[,i] = gsub(',choiceId:', '', x = data_frame[,i])
        data_frame[,i] = gsub('}', '', x = data_frame[,i])
        data_frame[,i] = gsub("\\{", '', x = data_frame[,i])
        data_frame[,i] = gsub(",timeZone:America/Los_Angeles", '', x = data_frame[,i])
      }
    }
  }

  # Loading the questions in ----
  #  List the sheets in the excel file
  sheet_names = excel_sheets(questions_dir)

  #  Read each sheet in as it appears in the list
  bl1_q = read_xlsx(questions_dir, trim_ws = T)
  names(bl1_q) = c("q_category","fu4_questions","_qid")
  qcat = str_split(bl1_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  bl1_q$q_category = qcat
  bl1_q$bl1_qid = gsub('"', '', x = bl1_q$bl1_qid)
  bl1_q$bl1_qid = gsub('ImportId:', '', x = bl1_q$bl1_qid)
  bl1_q$bl1_qid = gsub(',choiceId:', '', x = bl1_q$bl1_qid)
  bl1_q$bl1_qid = gsub('}', '', x = bl1_q$bl1_qid)
  bl1_q$bl1_qid = gsub("\\{", '', x = bl1_q$bl1_qid)
  bl1_q$bl1_qid = gsub(",timeZone:America/Los_Angeles", '', x = bl1_q$bl1_qid)

  bl2_q = read_xlsx(questions_dir, sheet = 2)
  names(bl2_q) = c("q_category","bl2_questions","bl2_qid")
  qcat = str_split(bl2_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  bl2_q$q_category = qcat
  bl2_q$bl2_qid = gsub('"', '', x = bl2_q$bl2_qid)
  bl2_q$bl2_qid = gsub('ImportId:', '', x = bl2_q$bl2_qid)
  bl2_q$bl2_qid = gsub(',choiceId:', '', x = bl2_q$bl2_qid)
  bl2_q$bl2_qid = gsub('}', '', x = bl2_q$bl2_qid)
  bl2_q$bl2_qid = gsub("\\{", '', x = bl2_q$bl2_qid)
  bl2_q$bl2_qid = gsub(",timeZone:America/Los_Angeles", '', x = bl2_q$bl2_qid)

  bl3_q = read_xlsx(questions_dir, sheet = 3)
  names(bl3_q) = c("q_category","bl3_questions","bl3_qid")
  qcat = str_split(bl3_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  bl3_q$q_category = qcat
  bl3_q$bl3_qid = gsub('"', '', x = bl3_q$bl3_qid)
  bl3_q$bl3_qid = gsub('ImportId:', '', x = bl3_q$bl3_qid)
  bl3_q$bl3_qid = gsub(',choiceId:', '', x = bl3_q$bl3_qid)
  bl3_q$bl3_qid = gsub('}', '', x = bl3_q$bl3_qid)
  bl3_q$bl3_qid = gsub("\\{", '', x = bl3_q$bl3_qid)
  bl3_q$bl3_qid = gsub(",timeZone:America/Los_Angeles", '', x = bl3_q$bl3_qid)

  fu1_q = read_xlsx(questions_dir, sheet = 4)
  names(fu1_q) = c("q_category","fu1_questions","fu1_qid")
  qcat = str_split(fu1_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  fu1_q$q_category = qcat
  fu1_q$fu1_qid = gsub('"', '', x = fu1_q$fu1_qid)
  fu1_q$fu1_qid = gsub('ImportId:', '', x = fu1_q$fu1_qid)
  fu1_q$fu1_qid = gsub(',choiceId:', '', x = fu1_q$fu1_qid)
  fu1_q$fu1_qid = gsub('}', '', x = fu1_q$fu1_qid)
  fu1_q$fu1_qid = gsub("\\{", '', x = fu1_q$fu1_qid)
  fu1_q$fu1_qid = gsub(",timeZone:America/Los_Angeles", '', x = fu1_q$fu1_qid)

  fu2_q = read_xlsx(questions_dir, sheet = 5)
  names(fu2_q) = c("q_category","fu2_questions","fu2_qid")
  qcat = str_split(fu2_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  fu2_q$q_category = qcat
  fu2_q$fu2_qid = gsub('"', '', x = fu2_q$fu2_qid)
  fu2_q$fu2_qid = gsub('ImportId:', '', x = fu2_q$fu2_qid)
  fu2_q$fu2_qid = gsub(',choiceId:', '', x = fu2_q$fu2_qid)
  fu2_q$fu2_qid = gsub('}', '', x = fu2_q$fu2_qid)
  fu2_q$fu2_qid = gsub("\\{", '', x = fu2_q$fu2_qid)
  fu2_q$fu2_qid = gsub(",timeZone:America/Los_Angeles", '', x = fu2_q$fu2_qid)

  fu3_q = read_xlsx(questions_dir, sheet = 6)
  names(fu3_q) = c("q_category","fu3_questions","fu3_qid")
  qcat = str_split(fu3_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  fu3_q$q_category = qcat
  fu3_q$fu3_qid = gsub('"', '', x = fu3_q$fu3_qid)
  fu3_q$fu3_qid = gsub('ImportId:', '', x = fu3_q$fu3_qid)
  fu3_q$fu3_qid = gsub(',choiceId:', '', x = fu3_q$fu3_qid)
  fu3_q$fu3_qid = gsub('}', '', x = fu3_q$fu3_qid)
  fu3_q$fu3_qid = gsub("\\{", '', x = fu3_q$fu3_qid)
  fu3_q$fu3_qid = gsub(",timeZone:America/Los_Angeles", '', x = fu3_q$fu3_qid)

  # Followup survey 4 ----
  fu4_q = read_xlsx(questions_dir, trim_ws = T, col_names = F)
  names(fu4_q) = c("q_category","fu4_questions","fu4_qid")
  qcat = str_split(fu4_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  fu4_q$q_category = qcat
  fu4_q$fu4_qid = gsub('"', '', x = fu4_q$fu4_qid)
  fu4_q$fu4_qid = gsub('ImportId:', '', x = fu4_q$fu4_qid)
  fu4_q$fu4_qid = gsub(',choiceId:', '', x = fu4_q$fu4_qid)
  fu4_q$fu4_qid = gsub('}', '', x = fu4_q$fu4_qid)
  fu4_q$fu4_qid = gsub("\\{", '', x = fu4_q$fu4_qid)
  fu4_q$fu4_qid = gsub(",timeZone:America/Los_Angeles", '', x = fu4_q$fu4_qid)

  # Followup survey 5 ----
  questions_dir = "~/Desktop/FU5_Questions.xlsx"
  fu5_q = read_xlsx(questions_dir, trim_ws = T, col_names = F)
  fu5_qt = data.frame(q_category = as.character(fu5_q[1,]),
                      fu5_questions = as.character(fu5_q[2,]),
                      fu5_qid = as.character(fu5_q[3,]))
  fu5_q = fu5_qt

  qcat = str_split(fu5_q$q_category, pattern = "_", n = 2)
  for (i in 1:length(qcat)) {
    if (length(qcat[[i]]) > 1) {
      qcat[[i]] = qcat[[i]][2]
    }
  }
  fu5_q$q_category = qcat
  fu5_q$fu5_qid = gsub('"', '', x = fu5_q$fu5_qid)
  fu5_q$fu5_qid = gsub('ImportId:', '', x = fu5_q$fu5_qid)
  fu5_q$fu5_qid = gsub(',choiceId:', '', x = fu5_q$fu5_qid)
  fu5_q$fu5_qid = gsub('}', '', x = fu5_q$fu5_qid)
  fu5_q$fu5_qid = gsub("\\{", '', x = fu5_q$fu5_qid)
  fu5_q$fu5_qid = gsub(",timeZone:America/Los_Angeles", '', x = fu5_q$fu5_qid)
  names(fu5_q)[names(fu5_q) == "q_category"] = "question_id"
  class(fu5_q$question_id) = "character"

  #  Join them all
  qids = full_join(bl1_q, bl2_q, by = "q_category")
  qids = full_join(qids, bl3_q, by = "q_category")
  qids = full_join(qids, fu1_q, by = "q_category")
  qids = full_join(qids, fu2_q, by = "q_category")
  qids = full_join(qids, fu3_q, by = "q_category")
  qids = full_join(survey_questions, fu5_q, by = "question_id")
  class(qids$q_category) = "character"
  for(i in 1:nrow(qids)) {
    if (is.na(qids[i,]$question_text) & !is.na(qids[i,]$fu5_questions)) {
      qids[i,]$question_text = qids[i,]$fu5_questions
    }
  }

  # Check for if the questions have all been asked before
  survey_questions = dbReadTable(lc_conn, "survey_questions")
  names(fu4_q)[names(fu4_q) == "q_category"] = "question_id"
  update_db(data_frame = qids, key_name = "question_id", connection = connect_db(),
            field_to_update = "fu5_qid", schema_name = "dbo", table_name = "survey_questions")



}
