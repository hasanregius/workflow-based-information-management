# Check whether donor_id changed in subsequent VTL donor reports, as per Tunde's email June 8 2022

projectdir = "~/rdc_data_management/"
datadir = "~/data/vtl_rd_cohorts/"
setwd(projectdir)
library(tidyverse)

donor_report_1 = read_csv(paste0(datadir, "Vitalant_donor_report_20220411.csv"))
donor_report_2 = read_csv(paste0(datadir, "Vitalant_donor_report_20220531.csv"))
donor_ids_1 = sort(unique(donor_report_1$donor_id))
donor_ids_2 = sort(unique(donor_report_2$donor_id))
identical(donor_ids_1, donor_ids_2)
