####################
# ARC SFTP Scraper
# v2.3; 3/13/2024
####################
# NOTE: Set your own directories prior to running/scheduling for the first time
# sftp package installation: devtools::install_github("stevenang/sftp")

#' @export
sftp_scrape = function() {
  # Dependencies ----
  require(RCurl)
  require(curl)
  require(sftp)
  require(striprtf)
  require(dplyr)
  require(dbplyr)
  require(glue)
  source("~/rdc_data_management/scripts/arc_sample_mssql_accessioning.R")
  source("~/rdc_data_management/scripts/helper_scripts/arc_manifest_qc.R")

  # Directories
  submission_dir = "~/rdc_files/submissions/"
  accepted_dir = "~/rdc_files/accepted"
  validation_dir = "~/rdc_files/dvr/"

  # SFTP Connection string
  sftp_arc = sftp_connect(server = "vtllsftp1.bloodsystems.org",
                          port = 222,
                          folder = "home",
                          username = "vri_admin",
                          password = read_rtf("~/hasansulaeman/REDSIV Serosurvey/SFTP/PWD/PWD.rtf"),
                          protocol = "sftp://")

  # Body ----
  #  Move down to the upload folder
  sftp_changedir("vri_arc/repeat_donor_cohorts/upload", "sftp_arc", verbose = F)

  #  Check for new submissions
  cat("\n############################", fill = T)
  cat("Checking for new submissions", fill = T)
  cat("############################\n", fill = T)
  manifest_list = sftp_listfiles(sftp_arc, verbose = T)

  #  Run if there are, conclude if there's none
  if (nrow(manifest_list) != 0) {
    cat(paste0(nrow(manifest_list)," new submissions found:"), fill = T)
    for (i in 1:nrow(manifest_list)) {
      cat(manifest_list[i,]$name, fill = T)
    }
    # Go through each submission
    for (j in 1:nrow(manifest_list)) {
      # Setup and file prep ----
      fname = manifest_list[j,]$name
      # Helper function for if whitespace is used in the file name ----
      if (grepl("\\s+", fname)) {
        cat(paste0("\nWhitespace found, renaming ",fname), fill = T)
        fname_corrected = gsub("\\s+", "_", fname)
        sftp_rename(from = fname,
                    to = fname_corrected,
                    sftp_connection = sftp_arc,
                    verbose = T)
        fname = fname_corrected
      }
      # Download, validate, and accession if accepted ----
      download_dir = paste0(submission_dir,fname)
      dl = sftp_download(file = fname,
                         tofolder = submission_dir,
                         sftp_connection = sftp_arc,
                         verbose = T)

      # Check if download worked. Only move forward if it does
      if (dl == 1 & file.exists(download_dir)) {
        cat(glue("{fname} was successfully downloaded"), fill = T)

        # Quality control ----
        manifest = read.csv(download_dir,
                            colClasses = c(donor_ID = "character"),
                            as.is = T)
        # Default value is false for validation
        validation = F
        # Go through QC
        names(manifest) = tolower(names(manifest))
        validation = arc_manifest_qc(manifest, fname)

        # Processing the submission ----
        if (validation == T) {
          # Process the accepted submission ----
          #  Add the manifest date
          fname_nchar = nchar(fname)
          date_start = fname_nchar - 11
          date_end = fname_nchar - 4
          manifest_date = as.Date(substr(fname,date_start,date_end), format = "%Y%m%d")
          if (class(manifest_date) == "Date" & is.na(manifest_date) == F) {
            manifest$manifest_date = manifest_date
            write.csv(manifest, file.path(accepted_dir,fname), row.names = F)
          } else {
            stop("Manifest date conversion failed")
          }

          # Upload accepted submission in archive dir ----
          sftp_changedir(tofolder = "..",
                         current_connection_name = "sftp_arc",
                         verbose = F)
          sftp_changedir(tofolder = "archive",
                         current_connection_name = "sftp_arc",
                         verbose = F)
          cat(paste0("Uploading ",fname," to the archive directory on the SFTP"), fill = T)
          # Upload the file
          ul = sftp_upload(file = fname,
                           fromfolder = accepted_dir,
                           sftp_connection = sftp_arc,
                           verbose = F)
          # Check for if the upload happened
          archive_list = sftp_list(sftp_connection = sftp_arc, type = "f")$name
          if (fname %in% archive_list) {
            cat("File successfully uploaded to the archive folder\n")
          } else {
            stop("File failed to upload to the archive folder\n")
            ul = sftp_upload(file = fname,
                             fromfolder = accepted_dir,
                             sftp_connection = sftp_arc,
                             verbose = F)
          }

          # Accessioning to the Db ----
          cat("Accessioning accepted submission to the RDC database\n", fill = T)
          sample_accession_tf = arc_sample_accession(manifest)
          # Go back to the start ----
          # Delete the original from the upload bucket to avoid confusion
          cat("Navigating back to the upload folder", fill = T)
          sftp_changedir(tofolder = "..",
                         current_connection_name = "sftp_arc",
                         verbose = F)
          sftp_changedir(tofolder = "upload",
                         current_connection_name = "sftp_arc",
                         verbose = T)
          if (file.exists(paste(accepted_dir,fname, sep = "/"))) {
            if (sample_accession_tf == T) {
              rm = sftp_delete(file = fname,
                               sftp_connection = sftp_arc,
                               verbose = F)
            } else {
              cat("Accessioning failed\n", fill = T)
            }
          } else {
            cat("File archiving failed\n", fill = T)
          }
        } else {
          # Submission failed validation ----
          # set up rejection flow
          cat("Moving submission and validation report to the download folder", fill = T)
          val_fname = gsub(".csv", "_validation.csv", fname)
          dvr_dir = paste(validation_dir, val_fname, sep = "/")
          if (file.exists(dvr_dir)) {
            # Upload the submission ----
            #  Out of the upload
            sftp_changedir(tofolder = "..",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  into the reject pile
            sftp_changedir(tofolder = "download/rejected",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  upload the rejected submission
            ul_reject = sftp_upload(file = fname,
                                    fromfolder = submission_dir,
                                    sftp_connection = sftp_arc,
                                    verbose = F)

            # Upload the validation file ----
            #  Go a step up
            sftp_changedir(tofolder = "..",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  go into the validation report directory
            sftp_changedir(tofolder = "rejected_dvr",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  Upload the validation report
            ul_reject_dvr = sftp_upload(file = val_fname,
                                        fromfolder = validation_dir,
                                        sftp_connection = sftp_arc,
                                        verbose = F)

            # Go back to upload directory ----
            #  Go a step up
            sftp_changedir(tofolder = "..",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  Another step up
            sftp_changedir(tofolder = "..",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            #  Back to the upload dir to loop
            sftp_changedir(tofolder = "upload",
                           current_connection_name = "sftp_arc",
                           verbose = F)
            # Remove the original submission
            if (file.exists(paste(submission_dir,fname, sep = "/"))) {
              rm = sftp_delete(file = fname,
                               sftp_connection = sftp_arc,
                               verbose = F)
            }
          } else {
            stop("Something went wrong with the validation process \n")
          }
        }
      } else {
        cat("Download failed", fill = T)
        traceback()
      }
    }
  } else {
    cat("No new submissions found\n", fill = T)
  }

}
