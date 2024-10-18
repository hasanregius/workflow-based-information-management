# VTL Q1 2023 Sample Selection
source("scripts/helper_scripts/db_connect.R")
lc_conn = connect_db()
visits = dbReadTable(lc_conn, "visits")
donors = dbReadTable(lc_conn, "donors")

# Set the seed so the random selection can be recreated
set.seed(20230724)

# Selection process
donits = donors %>%
  filter(rdc_2023 == T) %>%
  left_join(visits, by = "donor_id")
q123_selection = donits %>%
  filter(substr(din,1,1) == "W" &
           sample_in_repository == T &
           collection_date > as.Date("2022-12-31")) %>%
  group_by(donor_id) %>%
  sample_n(size = 1) %>%
  select(din, donation_date = collection_date) %>%
  mutate(Ortho_Quant_IgG = TRUE,
         Ortho_NC = TRUE)
write.csv(q123_selection, "~/Desktop/20230724_VTL_Q1_2023_CTS_testing_order.csv", row.names = F)
