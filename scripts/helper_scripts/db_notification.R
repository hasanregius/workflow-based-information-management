######################
# Notification Helper
# v1.0; 5/10/2023
######################

post_db_changes = function(bco, table, changes_nrow, action) {
  # Dependencies ----
  require(glue)
  require(yaml)
  conf = read_yaml("scripts/config.yml")
  notif_conf = conf$db_accessioning_flows$notifications

  # Script ----
  cat("\nAttempting to post changes to the database on Teams..", fill = T)
  if (notif_conf$post & dir.exists(notif_conf$dir)) {
    # Set the variables
    s_dtime = format(Sys.time(), format = "%m/%d/%Y at %H:%M")
    fname_dtime = format(Sys.time(), format = "%Y%m%d_%H%M%S")
    # Write the notification
    sink(glue("{notif_conf$dir}rdc_db_notify_{fname_dtime}.txt"))
    cat(glue("[{s_dtime}]"), fill = T)
    cat(glue("{as.integer(changes_nrow)} records have been {action} on the {table} table for {bco}"), fill = T)
    sink()
    cat("> Database changes successfully posted\n", fill = T)
  } else {
    cat("> Database changes failed to be posted\n", fill = T)
  }
}
