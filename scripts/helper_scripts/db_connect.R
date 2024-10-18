############################
# Database Connection Script
# v1.1; 3/13/2024
############################
# I use a gsub argument to mask the password being saved as a .txt file so mind that

#' @export
connect_db = function(db_name, employee_id, password) {
  # Dependencies ----
  #  Packages
  require(DBI)
  require(odbc)
  require(yaml)
  require(svDialogs)

  #  Config file
  conf = read_yaml("scripts/config.yml")

  # Check for missing arguments ----
  if (is.null(conf$db_connect)) {
    stop("database configuration arguments are missing")
  } else {
    # Employee ID
    if (missing(employee_id)) {
      # Check the configuration file
      if (is.null(conf$db_connect$employee_id)) {
        employee_id = dlg_input(
          message = "Please enter your employee identification number:"
        )
      } else {
        employee_id = as.character(conf$db_connect$employee_id)
      }
    }
    # Password
    if (missing(password)) {
      # Check the configuration file
      if (is.null(conf$db_connect$pass_dir)) {
        password = dlg_input(
          message = "Please enter your password:"
        )
      } else {
        if (file.exists(conf$db_connect$pass_dir)) {
          text_pass = T
        }
      }
    }
    # Database name
    if (missing(db_name)) {
      # Check the configuration file
      if (is.null(conf$db_connect$db_name)) {
        db_name = dlg_input(
          message = "Please specify the database name:"
        )
      } else {
        db_name = conf$db_connect$db_name
      }
    }
  }

  # Check if a connection can be made ----
  #  Check for if password is supplied in .txt format
  if (text_pass == T) {
    can_conn = dbCanConnect(odbc(),
                            Driver = conf$db_connect$driver,
                            Server = conf$db_connect$server_address,
                            Database = db_name,
                            Port = conf$db_connect$port,
                            uid = paste0("BSI\\", toupper(employee_id)),
                            pwd = readChar(conf$db_connect$pass_dir, nchars = 28))
  } else {
    can_conn = dbCanConnect(odbc(),
                            Driver = conf$db_connect$driver,
                            Server = conf$db_connect$server_address,
                            Database = db_name,
                            Port = conf$db_connect$port,
                            uid = paste0("BSI\\",toupper(employee_id)),
                            pwd = password)
  }

  # If a connection can be made, make the connection and pass it ----
  if (can_conn == T) {
    if (text_pass == T) {
      conn = dbConnect(odbc(),
                       Driver = conf$db_connect$driver,
                       Server = conf$db_connect$server_address,
                       Database = db_name,
                       Port = conf$db_connect$port,
                       uid = paste0("BSI\\", toupper(employee_id)),
                       pwd = readChar(conf$db_connect$pass_dir, nchar = 30))
      return(conn)
    } else {
      conn = dbConnect(odbc(),
                       Driver = conf$db_connect$driver,
                       Server = conf$db_connect$server_address,
                       Database = db_name,
                       Port = conf$db_connect$port,
                       uid = paste0("BSI\\", toupper(employee_id)),
                       pwd = password)
      return(conn)
    }
  } else {
    cat("connection cannot be made. Womp Womp :(", fill = T)
  }
}
