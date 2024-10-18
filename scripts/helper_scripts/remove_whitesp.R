##############################################
# Remove all whitespace from character fields
# 02/22/2016
##############################################

remove_whitesp = function(df) {
  cols_to_be_rectified = names(df)[vapply(df, is.character, logical(1))]
  df[,cols_to_be_rectified] = lapply(df[,cols_to_be_rectified], trimws)
}
