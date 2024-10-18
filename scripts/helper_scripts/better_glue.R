##############
# Better Glue
##############

glued = function(glue_string) {
  require(glue)
  string = toString(glue(glue_string))
  return(string)
}

catglue = function(glue_string) {
  require(glue)
  cat(glue(glue_string), fill = T)
}
