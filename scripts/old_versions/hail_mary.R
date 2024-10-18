# Libraries
require(readxl)
require(dplyr)

# Read the codebooks in.
# Why the fuck do they have a separate codebook for each fucking report
## Combined sheet ----
xlcry = "~/Desktop/RDC_program_codebook_v1.19_02SEP2022_KZ Notes_Svy Match v1.20.xlsx"
combined = readxl::read_xlsx(xlcry, trim_ws = T, sheet = 1)
combined_fieldnames = subset(combined$VarName, is.na(combined$VarName) == F)
combined_fieldnames = subset(combined_fieldnames, combined_fieldnames %in% c(""," ") == F)

combined = combined[,c(1,3,4,5,6)]
summary(is.na(combined))
combined = subset(combined, combined$VarName %in% combined_fieldnames)

## FQ1 sheet ----
fq1 = readxl::read_xlsx(xlcry, trim_ws = T, sheet = 2)
fq1_fieldnames = subset(fq1$VarName, is.na(fq1$VarName) == F)
fq1_fieldnames = subset(fq1_fieldnames, fq1_fieldnames %in% c(""," ") == F)

fq1 = fq1[,c(2,4)]
fq1 = subset(fq1, fq1$VarName %in% fq1_fieldnames)
summary(tolower(fq1$`VRI questionnaire (Followup 1)`) %in% tolower(combined$`VRI Field Name (Baseline 1)
(Export Row 1)`))
