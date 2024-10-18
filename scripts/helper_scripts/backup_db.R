############################
# RDC Database Backup Script
# v2.2; 3/13/2024
############################

#' @export
backup_db = function() {
  # Dependencies ----
  cat("\nDatabase Backup Process Initiated. Checking dependencies..", fill = T)
  require(tidyverse)
  require(arrow)
  require(dbplyr)
  require(lubridate)
  require(yaml)
  require(progress)

  # Working directory setting
  wd = getwd()
  if (!grepl("rdc_data_management", wd)) {
    setwd("~/rdc_data_management/")
  }

  # Database connect script
  if (file.exists("scripts/helper_scripts/db_connect.R")) {
    source("scripts/helper_scripts/db_connect.R")
  } else {
    stop("database connection script missing")
  }

  # Static variables ----
  if (file.exists("scripts/config.yml")) {
    conf = read_yaml("scripts/config.yml")
    backup_conf = conf$db_backup
  } else {
    stop("configuration file missing")
  }

  # Directory
  if (!dir.exists(backup_conf$directory)) {
    dir.create(backup_conf$directory)
  }
  date = format(Sys.Date(), format = "%Y%m%d")
  path = paste0(backup_conf$directory,date,"/")
  if (!dir.exists(path)) {
    dir.create(path)
  }

  # Database connection
  lc_conn = connect_db()

  # Backup process ----
  if (!is.null(lc_conn)) {
    cat("> Dependencies check complete\n\nStarting backup process..", fill = T)
    tables = backup_conf$tables
    progbar = progress_bar$new(total = length(tables), format = "[:bar] :percent ETA: :eta", width = 50)
    for (table in tables) {
      progbar$tick(1)
      schema_table = str_split(table, "\\.", n = 2)
      tmp = lc_conn %>%
        tbl(in_schema(schema_table[[1]][1],schema_table[[1]][2])) %>%
        collect() %>%
        write_parquet(paste0(path, gsub("\\.","_",table), ".parquet"),
                      compression = "zstd",
                      compression_level = 9)
    }
    cat("> Database backup complete\n", fill = T)
  }
}
