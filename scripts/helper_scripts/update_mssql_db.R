##############################
# MSSQL Update Records Script
# V2.2; 3/13/2024
##############################
# Limited to non-compound keys

#' @export
update_db = function(data_frame,
                     key_name,
                     field_to_update,
                     connection,
                     schema_name,
                     table_name) {
  # Dependencies ----
  require(DBI)
  require(odbc)
  require(dplyr)
  require(glue)
  require(dbplyr)
  require(stringr)
  require(svDialogs)
  require(purrr)
  require(progress)
  if (file.exists("scripts/helper_scripts/db_notification.R")) {
    source("scripts/helper_scripts/db_notification.R")
  } else {
    stop("notification script missing")
  }

  # Function variable check ----
  cat("\nDatabase update initiated", fill = T)
  cat("-------------------------", fill = T)
  cat("Checking for missing variables", fill = T)
  if (missing(connection)) {
    stop("Please provide a connection object")
  }
  if (dbIsValid(connection) == F) {
    stop("Can't connect to the database with the connection object provided")
  }
  if (missing(data_frame)) {
    stop("Please provide a data frame to perform the update with")
  } else {
    if (class(data_frame) != "data.frame") {
      data_frame = as.data.frame(data_frame)
      if (nrow(data_frame == 0)) {
        stop("Data frame provided must have at least 1 row of data")
      }
    }
  }
  if (missing(table_name)) {
    stop("Please specify a table to update")
  }
  if (missing(key_name)) {
    stop("No key fieldname provided")
  }
  if (missing(field_to_update)) {
    stop("Please specify a field to update")
  }
  if (missing(schema_name)) {
    message_text = "Warning: no schema variable provided\nShould the default schema be used?"
    schema_q = dlg_list(choices = c("Yes","No"), preselect = "No",
                        multiple = F, title = message_text)
    if (schema_q$res == "Yes") {
      schema_name = "dbo"
    } else {
      stop("ERROR: Please provide a schema name")
    }
  }
  cat("> All required variables are present \n", fill = T)

  # Check for validity of Db variables supplied ----
  cat("Checking for validity of variables supplied", fill = T)

  #  Scheme validity
  scheme_valid  = dbExistsTable(connection, Id(schema = schema_name, table = table_name))
  if (scheme_valid == F) {
    stop(glue("{table_name} table in {schema_name} schema was not found"))
  }

  # Key name validity
  if (length(key_name) > 1) {
    stop("This function only handles non-composite keys")
  }

  # Check for validity of field variables supplied ----
  #  Pull Db table information
  if (schema_name == "dbo") {
    db_fieldnames = dbListFields(connection, table_name)
  } else {
    # dbListFields somehow doesn't work with non-default MSSQL schema fed by a variable
    list_fields_pls = glue(
    "SELECT
     COLUMN_NAME
     FROM
     INFORMATION_SCHEMA.COLUMNS
     WHERE
     TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}';")
    db_fieldnames = dbGetQuery(connection, paste0(list_fields_pls))[,1]
  }

  #  Check for if the key fieldname provided is valid
  if (key_name %in% names(data_frame) == F) {
    stop("Specified key field doesn't exist in the data frame provided")
  }
  if (key_name %in% db_fieldnames == F) {
    stop("Key field provided does not exist in the specified schema and table")
  }

  #  Check for the field we want to update
  if (field_to_update %in% names(data_frame) == F |
      field_to_update %in% db_fieldnames == F) {
    stop("Specified field to update could not be found")
  }
  cat("> All variables supplied are valid \n", fill = T)

  # Update the field ----
  cat("Beginning update of records..", fill = T)
  # Set up a progress bar with a total number of updates
  nrow_update = nrow(data_frame)
  rows_left = nrow_update
  progbar = progress_bar$new(total = nrow_update, format = "[:bar] :percent ETA: :eta", width = 50)
  # Begin a transaction
  dbBegin(connection)
  # Loop over and paginate the update
  while(rows_left > 0) {
    if (rows_left >= 1000) {
      # Update the database
      dbExecute(connection, paste0("UPDATE ",schema_name,".",table_name," SET ",field_to_update," = ? WHERE ",key_name," = ?"),
                params = list(data_frame[1:1000,field_to_update], data_frame[1:1000,key_name]))
      # Advance the progress bar
      progbar$tick(1000)
      # Remove the updated records
      data_frame = data_frame[1001:nrow(data_frame),]
      rows_left = nrow(data_frame)
    } else {
      # Update the database
      dbExecute(connection, paste0("UPDATE ",schema_name,".",table_name," SET ",field_to_update," = ? WHERE ",key_name," = ?"),
                params = list(data_frame[1:nrow(data_frame),field_to_update], data_frame[1:nrow(data_frame),key_name]))
      # Advance the progress bar
      progbar$tick(nrow(data_frame))
      # Remove the updated records
      rows_left = rows_left-nrow(data_frame)
    }
  }
  # Commit the transaction
  dbCommit(connection)
  # Disconnect
  dbDisconnect(connection)
  cat("> Update of records complete\n", fill = T)
}



