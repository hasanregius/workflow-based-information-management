#################################
# Remove records in the database
# v1.1; 3/13/2024
#################################

#' @export
rm_records_db = function(data_frame,
                         key_name,
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

  # Better glue function
  glued = function(string) {
    require(glue)
    glued_string = as.character(glue(string))
    return(glued_string)
  }

  # Function variable check ----
  cat("\nDatabase record removal initiated", fill = T)
  cat("-----------------------------------", fill = T)
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

  # Check for validity of variables supplied ----
  cat("Checking for validity of variables supplied..", fill = T)

  #  Scheme validity
  scheme_valid  = dbExistsTable(connection, Id(schema = schema_name, table = table_name))
  if (scheme_valid == F) {
    stop(glue("{table_name} table in {schema_name} schema was not found"))
  }

  # Check for key field supplied ----
  list_fields_statement = glue(
    "SELECT
     COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE
     FROM
     INFORMATION_SCHEMA.COLUMNS
     WHERE
     TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}';")
  db_fieldnames = dbGetQuery(connection, paste0(list_fields_statement))[,1]

  #  Check for if the key fieldname provided is valid
  if (all(key_name %in% names(data_frame)) == F) {
    stop("Specified key field doesn't exist in the data frame provided")
  }
  if (all(key_name %in% db_fieldnames) == F) {
    stop("Key field provided does not exist in the specified schema and table")
  }

  # Delete the records ----
  cat("Beginning update of records..", fill = T)
  # Set up a progress bar with a total number of updates
  nrow_delete = nrow(data_frame)
  # Begin a transaction
  dbBegin(connection)
  # Loop over the updates and update the database
  if (length(key_name) > 1) {
    if (length(key_name) > 2) {
      # For if 3 key variables are used
      if (length(key_name) == 3) {
        rows_affected = dbExecute(connection,
                  paste0("DELETE FROM ",schema_name,".",table_name," WHERE ",
                         key_name[1]," = ?"," AND ",key_name[2]," = ?",
                         " AND ",key_name[3]," = ?"),
                  params = list(data_frame[,key_name[1]],
                                data_frame[,key_name[2]],
                                data_frame[,key_name[3]]))
      } else {
        stop("too many key variables")
      }
    } else {
      # For if 2 key variables are used
      rows_affected = dbExecute(connection, paste0("DELETE FROM ",schema_name,".",table_name,
                                                   " WHERE ",key_name[1]," = ?"," AND ",key_name[2]," = ?"),
                params = list(data_frame[,key_name[1]], data_frame[,key_name[2]]))
    }
  } else {
    # For only 1 key variable used
      rows_affected = dbExecute(connection,
                                paste0("DELETE FROM ",schema_name,".", table_name," WHERE ",key_name," = ?"),
                                params = list(data_frame[,key_name]))
  }
  # Commit the changes or rollback
  if (nrow(data_frame) == rows_affected) {
    # Commit the transaction
    dbCommit(connection)
    cat("> Update of records complete \n", fill = T)
  } else {
    dbRollback(connection)
    cat("> Update of records failed \n", fill = T)
  }
}
