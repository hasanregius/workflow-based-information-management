
# Base file
ref = read.csv("~/Downloads/ref_table_raceth_20220816.csv", colClasses = c(
  edw_eth_code = "character"
))

dontab = read.csv(file.choose(), colClasses = c(
  eth_code = "character"
))

asian_subraces = c("VIETNAMESE", "CHINESE", "JAPANESE", "OTHER ASIAN",
                   "FILIPINO", "ASIAN INDIAN", "KOREAN")
pacislander_subraces = c("OTHER PACIFIC ISLANDER", "NATIVE HAWAIIAN",
                         "SAMOAN", "GUAMANIAN OR CHAMORRO")

# Tease out the fields but dont piss them off
#  Splitting into two
ref$edw_eth_desc1 = NA
ref$edw_eth_desc2 = NA
for (i in 1:nrow(ref)) {
  split = strsplit(ref[i,]$edw_eth_desc, split = " - ", fixed = T)
  if (is.null(split[[1]][2]) == F) {
    ref[i,]$edw_eth_desc1 = split[[1]][1]
    ref[i,]$edw_eth_desc2 = split[[1]][2]
  }
}

#  Default as false
ref$american_indian = FALSE
ref$asian = FALSE
ref$black = FALSE
ref$other = FALSE
ref$pacific_islander = FALSE
ref$white = FALSE

ref = ref %>%
  mutate(
    american_indian = case_when(race == "american_indian" ~ T,
                                grepl("AMER", edw_eth_desc) == T & race == "more_than_one" ~ T,
                                grepl("AMER", edw_race_desc) == T & race == "more_than_one" ~ T,
                                grepl("AMER", edw_eth_desc1) == T & race == "more_than_one" ~ T,
                                grepl("AMER", edw_eth_desc2) == T & race == "more_than_one" ~ T),
    asian = case_when(race == "asian" ~ T,
                      grepl("ASIAN", edw_eth_desc) == T & race == "more_than_one" ~ T,
                      grepl("ASIAN", edw_race_desc) == T & race == "more_than_one" ~ T,
                      grepl("ASIAN", edw_eth_desc1) == T & race == "more_than_one" ~ T,
                      grepl("ASIAN", edw_eth_desc2) == T & race == "more_than_one" ~ T,
                      edw_eth_desc1 %in% asian_subraces ~ T,
                      edw_eth_desc2 %in% asian_subraces ~ T),
    black = case_when(race == "black" ~ T,
                      grepl("BLACK", edw_eth_desc) == T ~ T,
                      grepl("BLACK", edw_race_desc) == T & race == "more_than_one" ~ T,
                      grepl("BLACK", edw_eth_desc1) == T ~ T,
                      grepl("BLACK", edw_eth_desc2) == T ~ T),
    other = case_when(race == "other" ~ T,
                      edw_race_desc == "SOME OTHER RACE" & race == "more_than_one" ~ T,
                      edw_eth_desc1 == "SOME OTHER RACE" ~ T,
                      edw_eth_desc2 == "SOME OTHER RACE" ~ T),
    pacific_islander = case_when(race == "pacific_islander" ~ T,
                                 grepl("PACIF", edw_eth_desc) == T & race == "more_than_one" ~ T,
                                 grepl("PACIF", edw_race_desc) == T & race == "more_than_one" ~ T,
                                 grepl("PACIF", edw_eth_desc1) == T & race == "more_than_one" ~ T,
                                 grepl("PACIF", edw_eth_desc2) == T & race == "more_than_one" ~ T,
                                 edw_eth_desc1 %in% pacislander_subraces ~ T,
                                 edw_eth_desc2 %in% pacislander_subraces ~ T),
    white = case_when(race == "white" ~ T,
                      grepl("WHITE", edw_eth_desc) == T & race == "more_than_one" ~ T,
                      grepl("WHITE", edw_race_desc) == T & race == "more_than_one" ~ T,
                      grepl("WHITE", edw_eth_desc1) == T & race == "more_than_one" ~ T,
                      grepl("WHITE", edw_eth_desc2) == T & race == "more_than_one" ~ T)
  )

# Fill blanks with F
ref$asian[is.na(ref$asian)] = F
ref$black[is.na(ref$black)] = F
ref$american_indian[is.na(ref$american_indian)] = F
ref$other[is.na(ref$other)] = F
ref$pacific_islander[is.na(ref$pacific_islander)] = F
ref$white[is.na(ref$white)] = F

# Accession the new reference table
#  raceth_code
ref$raceth_code = as.integer(row.names(ref))
#  With transaction
dbWithTransaction(lc_conn, {
  ref1 = ref %>%
    select(all_of(dbListFields(lc_conn,"race_ref")))
  dbWriteTable(lc_conn, "race_ref", ref1, append = T)
  ref_pull = dbReadTable(lc_conn, "race_ref")
  check = nrow(intersect(ref, ref_pull))
  if (check != nrow(ref)) {
    dbBreak()
    cat("Fail", fill = T)
    }
  })

if (all(c("edw_eth_code","edw_race_desc") %in% names(dontab)) == F) {
  if (all(c("eth_code","race_desc") %in% names(dontab))) {
    names(dontab)[names(dontab) == "eth_code"] = "edw_eth_code"
    names(dontab)[names(dontab) == "race_desc"] = "edw_race_desc"
  }
}

dontab_joined = left_join(dontab, ref, by = c("edw_eth_code","edw_race_desc"))
donors = dbReadTable(lc_conn, "donors")
donors = donors %>%
  select(donor_id,
         abo_blood_group = abo_group,
         rh_factor = rh_group)
donors$abo_blood_group = toupper(donors$abo_blood_group)
names(dontab_joined)[names(dontab_joined) == "race.y"] = "race"
names(dontab_joined)[names(dontab_joined) == "ethnicity_hispanic.y"] = "ethnicity_hispanic"

dontab = left_join(dontab_joined, donors, by = "donor_id")

dontab = dontab %>%
  select(all_of(conf$fieldname_lists$donation_report))

dontab$abo_blood_group[dontab$abo_blood_group == "U"] = "AB"
dontab$rh_factor[dontab$rh_factor == "unknown"] = "unavailable"
dontab$phlebotomy_status[dontab$phlebotomy_status == "no_phlebotomy"] = "unsuccessful_phlebotomy"
dontab$phlebotomy_status[dontab$phlebotomy_status == "sample_only"] = "successful_phlebotomy"

for (i in 1:length(names(dontab))) {
  if (is.character(dontab[,i]) &
      names(dontab)[i] %in% c("donor_id","din") == F) {
    vals = unique(dontab[,i])
    cat(glued("Fieldname: {names(dontab)[i]}"), fill = T)
    cat("Values: ")
    cat(vals, fill = T)
    cat("\n", fill = T)
  }
}

write.csv(dontab, "~/Desktop/VTL_donation_linelist_20220817.csv", row.names = F, eol = "\r\n")

donor_file = read.csv(file.choose())
