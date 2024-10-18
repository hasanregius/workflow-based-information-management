#######################################
# VTL MSSQL Donation/Visit Accessioning
# v1.2; 3/13/2024
#######################################

#' @export
vtl_visits_accession = function(df) {
  # Dependencies and Setup ----
  require(odbc)
  require(DBI)
  require(dplyr)
  require(dbplyr)
  require(tidyr)
  require(striprtf)
  require(glue)
  require(lubridate)
  require(yaml)
  require(stringr)

  # Configuration and helper scripts
  conf = read_yaml("scripts/config.yml")
  if (file.exists("scripts/helper_scripts/db_connect.R")) {
    source("scripts/helper_scripts/db_connect.R")
  } else {
    stop("missing scripts")
  }

  # Preemptive connection check. Abort if false.
  lc_conn = connect_db()
  names(df)[names(df) == "vaccination_status_donation"] = "dhq_vaccination_status_donation"
  names(df)[names(df) == "vaccination_status_ever"] = "dhq_vaccination_status_ever"
  names(df)[names(df) == "DONOR_ETHNICITY_CODE"] = "edw_eth_code"
  names(df)[names(df) == "RACE_DESCRIPTION"] = "edw_race_desc"
  names(df)[names(df) == "MAP_DIVISION"] = "map_division"
  names(df)[names(df) == "MAP_REGION"] = "map_region"
  names(df)[names(df) == "DRIVE_TYPE_FIXED_MOBILE"] = "location"
  df$din = substr(toupper(df$din),1,13)
  df$sample_status = 0
  df$sample_status[df$din %in% fw$din] = "available"
  df$sample_in_repository = NA
  df$sample_in_repository[df$sample_status == "available"] = 1
  df$sample_in_repository[df$sample_status == "unavailable"] = 0
  df$zip_code = str_pad(df$zip_code, width = 5, side = "left", pad = "0")
  df$location = tolower(df$location)
  df$map_region = tolower(df$map_region)
  df$map_division = tolower(df$map_division)

  mobs = lc_conn %>% tbl("donors") %>% collect() %>% select(donor_id, mob)
  df = left_join(df, mobs, by = "donor_id")
  df$collection_date = as.Date(df$collection_date, origin = "1960-01-01")
  df$age_at_donation = year(as.period(interval(df$mob, df$collection_date)))

  # Body ----
  # If connection can be made, proceed
  if (is.null(lc_conn) == F) {
    # Pull reference fieldnames ----
    visits_fnames = dbListFields(lc_conn, "visits")

    # Check for fields corresponding to VTL sample table ----
    if (all(visits_fnames %in% names(df))) {
      cat("df field check complete, standardizing entries..\n", fill = T)
      # Renaming the fields to how it's captured in the database
      vtlv = df %>%
        select(all_of(visits_fnames)) %>%
        mutate(
          dhq_vaccination_status_donation = case_when(
            dhq_vaccination_status_donation == "not_vaccinated" ~ 0,
            dhq_vaccination_status_donation == "vaccinated" ~ 1),
          dhq_vaccination_status_donation = na_if(dhq_vaccination_status_donation, "unavailable"),
          dhq_vaccination_status_ever = case_when(dhq_vaccination_status_ever == "vaccinated_ever" ~ 1,
                                              dhq_vaccination_status_ever == "not_vaccinated_ever" ~ 0),
          dhq_vaccination_status_ever = na_if(dhq_vaccination_status_ever, "unavailable"),
          phlebotomy_status = case_when(phlebotomy_status %in% c("successful_phlebotomy","sample_only") ~ 1,
                                        phlebotomy_status %in% c("unsuccessful_phlebotomy","no_phlebotomy") ~ 0)
        )

      # Turn date fields into date object if they aren't already
      #  Collection date
      if (is.Date(vtlv$collection_date) == F) {
        vtlv$collection_date = as.Date(vtlv$collection_date,
                                       tryFormats = c("%m/%d/%Y", "%Y-%m-%d", "%d%b%Y"))
      }
      # Adding the VTL prefix in front of the VTL donor IDs
      vtlv$donor_id[nchar(vtlv$donor_id) != 7 & substr(vtlv$donor_id,1,3) != "VTL"] = paste0(
        "VTL",vtlv$donor_id[nchar(vtlv$donor_id) == 7 & substr(vtlv$donor_id,1,3) != "VTL"])
      vtlv$gender = tolower(vtlv$gender)

      # Assigning DINs for donation numbers
      vtlv$din[nchar(vtlv$din) == 7] = paste0("A9999",vtlv$din[nchar(vtlv$din) == 7],"0")
      vtlv$donation_type[substr(vtlv$din,1,1) == "W" & vtlv$donation_type == ""] = "other"
      vtlv$donation_procedure[vtlv$donation_procedure == "" & substr(vtlv$din,1,1) == "W"] = "other"
      vtlv$donation_procedure[vtlv$donation_procedure == "" & substr(vtlv$din,1,1) == "A"] = NA
      vtlv$edw_eth_code = str_pad(vtlv$edw_eth_code, width = 3, pad = "0", side = "left")
      vtlv$edw_eth_code[is.na(vtlv$edw_eth_code)] = ""
      vtlv$edw_race_desc[is.na(vtlv$edw_race_desc)] = ""

      # Trim all whitespaces
      cols_to_be_rectified = names(vtlv)[vapply(vtlv, is.character, logical(1))]
      vtlv[,cols_to_be_rectified] = lapply(vtlv[,cols_to_be_rectified], trimws)

      # Tolower all fields
      vtlv$map_division = tolower(vtlv$map_division)
      vtlv$map_region = tolower(vtlv$map_region)
      vtlv$location = tolower(vtlv$location)

      # Accession into the Db ----
      #  Check for the list of DINs already in the Db
      vtl_visits_list = lc_conn %>%
        tbl("visits") %>%
        pull("din")
      vtl_visits_length = length(vtl_visits_list)

      #  Accessioning sample data with transaction
      dbWithTransaction(lc_conn, {
        # If some samples are already in the Db, we omit those samples
        if (TRUE %in% (vtlv$din %in% vtl_visits_list)) {
          cat("Duplicate DINs found, subsetting for unique IDs", fill = T)
          vtlv = subset(vtlv, vtlv$din %in% vtl_visits_list == F)
          dbWriteTable(lc_conn, "visits", vtlv, append = TRUE)
        } else {
          cat("No duplicate records found. Attempting to import all results", fill = T)
          dbWriteTable(lc_conn, "visits", vtlv, append = TRUE)
        }
        # Check for if accessioning was successful
        vtl_visits_list = lc_conn %>%
          tbl("visits") %>%
          pull("din")
        # Rollback in case of error
        if (length(vtl_visits_list) - vtl_visits_length != nrow(vtlv)) {
          dsc = length(vtl_visits_list) - (vtl_visits_length + nrow(vtlv))
          if (dsc < 0) {
            dsc = dsc - dsc*2
          }
          cat(paste0(dsc," records failed to be imported. Aborting..\n"), fill = T)
          dbBreak()
          return(FALSE)
        } else {
          if (nrow(vtlv) > 0) {
            # post_db_changes("VTL","visits",nrow(vtlv),"accessioned")
          }
          cat("All VTL visits data was successfully imported", fill = T)
          return(TRUE)
        }
      })
    } else {
      return(FALSE)
      stop("Required visits fields are missing..")
    }
  } else {
    return(FALSE)
    stop("Can't connect to the Db. Check your VPN or contact IT.")
  }
}
