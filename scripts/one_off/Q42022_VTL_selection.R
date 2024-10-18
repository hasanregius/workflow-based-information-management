# VTL Q4 2022 Sample Selection
source("data_management/helper_scripts/db_connect.R")
lc_conn = connect_db()
visits = dbReadTable(lc_conn, "visits")

# Set the seed so the random selection can be recreated
set.seed(20230203)

# Selection process
q4_selection = visits %>%
  filter(substr(din,1,1) == "W" & sample_in_repository == T & between(collection_date, as.Date("2022-10-01"), as.Date("2022-12-31"))) %>%
  group_by(donor_id) %>%
  sample_n(size = 1) %>%
  select(din, donation_date = collection_date) %>%
  mutate(Ortho_Quant_IgG = TRUE,
         Ortho_NC = TRUE)
write.csv(q4_selection, "~/Desktop/20230203_VTL_Q4_2022_CTS_testing_order.csv", row.names = F)
