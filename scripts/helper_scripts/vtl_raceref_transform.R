############################################
# VTL RDC race and ethnicity transformations
#
############################################
# NOTE: Assumes the field names are correct and consistent

raceref_transform = function(df) {
  # Dependencies ----
  require(DBI)
  require(odbc)
  require(dplyr)
  require(stringr)
  require(tidyr)

  #  Fieldnames static
  eth_ref = read.csv("~/rdc_data_management/static_assets/eth_ref_tbl.csv")
  race_ref = read.csv("~/rdc_data_management/static_assets/race_ref_tbl.csv")
  req_fnames = c("donor_ethnicity_code", "race_description")
  req_fnames_post = c("race_desc", "eth_code")

  # If df is missing, stop ----
  if (missing(df)) {
    stop("Missing data_frame")
  }

  # Field check ----
  names(df) = tolower(names(df))
  if (all(req_fnames_post %in% names(df)) == F) {
    if (all(req_fnames %in% names(df))) {
      names(df)[names(df) == "race_description"] = "race_desc"
      names(df)[names(df) == "donor_ethnicity_code"] = "eth_code"
    } else {
      stop("required fields missing")
    }
  }

  # Values check ----
  df$race_desc[df$race_desc == ""] = NA
  df$eth_code[df$eth_code == ""] = NA

  # Join with the reference table ----
  df_joined = left_join(df, raceref, by = all_of(req_fnames_post))
  return(df_joined)
}
