# Dependencies
require(haven)

# Read in the donor summary file from Westat
donsum = read.csv("~/Downloads/NBDC_Final_Data_2024-05-03/2_Summary_Files/3_Donor_Level_Key_Info/Donor_Summary_Key_Info_05022024.csv",
                  as.is = T, colClasses = "character")

# Determine which fields make the cut
donsum_names = unique(names(donsum))
survey_fnames = unique(append(c("donor_id"), donsum_names[str_detect(donsum_names, "FQ")]))

names(donsum)[1] = "donor_id"

survey_summ = donsum %>%
  select(all_of(survey_fnames)) %>%
  pivot_longer(cols = !donor_id,
               names_to = "question",
               values_to = "answer") %>%
  mutate(survey_round = tolower(substr(question,1,3)),
         question = tolower(substr(question,5,nchar(question)))) %>%
  filter(answer != "") %>%
  select(donor_id, survey_round, question, answer)

sb_conn = connect_db(db_name = "VRI_Sandbox")
dbWriteTable(sb_conn, "donor_survey_results", survey_summ, append = T)

surv_db = dbReadTable(sb_conn, "donor_survey_results")

