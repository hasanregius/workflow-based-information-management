################################
# Modular Data Quality Control
# v1.3; 08/18/2022
################################
# Add assay check, which is complicated

dat_qc = function(dframe, fname, flow_name) {
  # Dependencies ----
  #  Packages ----
  require(dplyr)
  require(stringr)
  require(striprtf)
  require(yaml)

  #  Load config ----
  conf = read_yaml("data_management/config.yml")

  #  Check for valid flow name ----
  #  Check for required lists
  req_list = c("codebook","qc_flows")
  for (i in 1:length(req_list)) {
    if (is.null(req_list[i])) {
      stop(paste0("required list is missing from the config file : ",{req_list[i]}))
    }
  }
  #   Check the flow list
  flow_list = names(conf$qc_flows)
  if (missing(flow_name)) {
    stop("no quality control flow specified")
  } else {
    if (flow_name %in% flow_list == F) {
      stop("the quality control flow supplied is not listed in the configuration file")
    } else {
      flow = conf$qc_flows[[flow_name]]
    }
  }

  #   Source scripts listed for the flow ----
  dir_script_list = list.files(getwd(), pattern = "\\.R", recursive = T)
  script_list = flow$scripts
  for (i in 1:length(script_list)) {
    fnum = which(grepl(script_list[i], dir_script_list))
    if (file.exists(dir_script_list[fnum])) {
      source(dir_script_list[fnum])
    } else {
      stop(paste0("missing ",dir_script_list[i]," script"))
    }
  }

  #   Pull the directory list for the flow -----
  if (is.null(flow$directories)) {
    stop(glued("{flow_name}'s directory list is missing from the config file"))
  } else {
    if (length(flow$directories) == 3 &
        all(names(flow$directories) %in% c("submission","accepted","validation"))) {
      for (i in 1:length(flow$directories)) {
        if (dir.exists(flow$directories[[i]]) == F) {
          stop("invalid directory specified")
        }
      }
      submission_dir = flow$directories$submission
      accepted_dir = flow$directories$accepted
      validation_dir= flow$directories$validation
    } else {
      stop("invalid directory specified")
    }
  }

  #   Pull the field list for the flow ----
  if (is.null(flow$fields)) {
    stop(glued("no field list specified for {flow_name}"))
  } else {
    field_list = flow$fields
  }

  #  Default values ----
  #   data frame ----
  if (missing(dframe)) {
    stop("no data frame to perform quality control on")
  }
  #   filename ----
  if (missing(fname)) {
    stop("file name is required to generate data validation reports")
  } else {
    if (grepl("\\.csv",fname) == F) {
      stop("invalid file name format, must be a .csv file")
    }
  }

  #  Switches ----
  field_error = F

  #  Validation report start ----
  dvr_colnames = c("issue","key","value")
  dvr = data.frame(matrix(nrow = 0, ncol = length(dvr_colnames)))

  # Start of quality control ----
  cat("Initiating QC process", fill = T)
  cat("---------------------- \n", fill = T)

  #  Field check ----
  #   Load the codebook
  cat("Checking fieldnames..", fill = T)
  codebook = conf$codebook
  conf_fieldnames = tolower(names(codebook))

  #   Load the names from the data frame
  names(dframe) = tolower(names(dframe))
  dframe_fieldnames = names(dframe)

  #   Check whether all fields specified for the flow is included
  if (all(field_list %in% dframe_fieldnames)) {
    cat("> All fieldnames listed for the flow are present\n", fill = T)
    dframe_fieldnames = subset(dframe_fieldnames, dframe_fieldnames %in% field_list)
  } else {
    temp_df = data.frame(
      issue = "required_field_missing",
      key = NA,
      value = field_list[field_list %in% dframe_fieldnames == F]
    )
    dvr = rbind(dvr, temp_df)
    field_error = T
  }
  for (i in 1:length(dframe_fieldnames)) {
    if (dframe_fieldnames[i] %in% conf_fieldnames == F) {
      catglue("> {dframe_fieldnames[i]} is not in the codebook")
      temp_dvr = data.frame(
        issue = "field_not_in_codebook",
        key = "not_applicable",
        value = dframe_fieldnames[i]
      )
      dvr = rbind(dvr, temp_dvr)
    }
  }
  cat("\nField check complete", fill = T)
  if (field_error) {
    stop("fatal error during field check")
  } else {
    cat("> All fieldnames are correct, checking values for each field\n", fill = T)
    codebook = subset(codebook, names(codebook) %in% dframe_fieldnames)
  }

  # Format the data_frame, remove all whitespaces before checking entries
  cols_to_be_rectified = names(dframe)[vapply(dframe, is.character, logical(1))]
  for (i in 1:length(cols_to_be_rectified)) {
    dframe[,cols_to_be_rectified[i]] = gsub("//s+","",dframe[,cols_to_be_rectified[i]])
  }

  #  Entry check ----
  for (i in 1:length(dframe_fieldnames)) {
    # Pull QC arguments listed in the config ----
    catglue("\n\nPerforming entry check on: {dframe_fieldnames[i]}")
    qc_args = codebook[[dframe_fieldnames[i]]]
    qc_arg_names = names(qc_args)
    # NA check ----
    if (T %in% is.na(dframe[,dframe_fieldnames[i]])) {
      if ("explicit_null" %in% qc_arg_names) {
        dframe[,dframe_fieldnames[i]][is.na(dframe[,dframe_fieldnames[i]])] = qc_args$explicit_null
      } else {
        catglue("> NAs detected in {dframe_fieldnames[i]} with no explicit null argument")
        temp_df = subset(dframe, is.na(dframe[,dframe_fieldnames[i]]))
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = dframe_fieldnames[i],
                 value = dframe_fieldnames[i])
        temp_df$issue = "NA_found_no_explicit_null"
        temp_df$value = NA
        dvr = rbind(dvr, temp_df)
        stop("quality control check aborted")
      }
    }
    # is_key ----
    if ("is_key" %in% qc_arg_names) {
      catglue("> {dframe_fieldnames[i]} is a key field")
      key_fieldname = dframe_fieldnames[i]
      # nchar ----
      if ("nchar" %in% qc_arg_names) {
        catglue("> Performing nchar check")
        # What is the nchar for the key field
        nchar_arg = qc_args$nchar
        # Subset any incorrect records
        temp_df = subset(dframe, nchar(dframe[,dframe_fieldnames[i]]) != nchar_arg)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = dframe_fieldnames[i],
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Entries with incorrect nchar detected")
          temp_df$issue = "incorrect_key_nchar"
          temp_df$value = nchar(temp_df$key)
          dvr = rbind(dvr, temp_df)
        } else {
          catglue("  > All entries passed nchar check")
        }
      }
      # prefix ----
      if ("prefixes" %in% qc_arg_names) {
        catglue("> Performing prefix check on {dframe_fieldnames[i]}")
        # What is the nchar for the key field
        prefixes_arg = qc_args$prefixes
        # Prefix nchar check
        if (all(nchar(qc_args$prefixes))) {
          prefixes_nchar = nchar(prefixes_arg[1])
        } else {
          stop("key field prefixes must be of the same length")
        }
        # Subset any incorrect records
        temp_df = subset(dframe, substr(dframe[,dframe_fieldnames[i]],1,prefixes_nchar) %in% prefixes_arg == F)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = dframe_fieldnames[i],
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Entries with incorrect prefix detected")
          temp_df$issue = "incorrect_key_prefix"
          temp_df$value = substr(temp_df$key, 1, prefixes_nchar)
          dvr = rbind(dvr, temp_df)
        } else {
          catglue("  > All entries passed nchar check")
        }
      }
    }
    # is_calculated ----
    if ("is_calculated" %in% qc_arg_names) {
      catglue("> {dframe_fieldnames[i]} is a calculated field")
      if ("script" %in% qc_arg_names) {
        if (all(qc_args$script %in% script_list) == F) {
          catglue("> Specified script for the field missing, skipping QC")
        }
      } else {
        catglue("> No script specified, skipping QC")
      }
    }
    # is_character ----
    if ("is_character" %in% qc_arg_names) {
      # is_zipcode ----
      if ("is_zipcode" %in% qc_arg_names) {
        class(dframe[,dframe_fieldnames[i]]) = "character"
        dframe[,dframe_fieldnames[i]] = str_pad(dframe[,dframe_fieldnames[i]],
                                                width = 5, side = "left", pad = "0")
      }
      # nchar ----
      if ("nchar" %in% qc_arg_names) {
        catglue("> Performing nchar check")
        # What is the nchar for the key field
        nchar_arg = qc_args$nchar
        # Subset any incorrect records
        temp_df = subset(dframe, nchar(dframe[,dframe_fieldnames[i]]) != nchar_arg)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = key_fieldname,
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Entries with incorrect nchar detected")
          temp_df$issue = "incorrect_key_nchar"
          temp_df$value = nchar(temp_df$value)
          dvr = rbind(dvr, temp_df)
        } else {
          catglue("  > All entries passed nchar check")
        }
      }
      }
    # is_factor ----
    if ("is_factor" %in% qc_arg_names) {
      catglue("> {dframe_fieldnames[i]} is a factor field")
      # factors ----
      catglue("> Performing factor check")
      if ("factors" %in% qc_arg_names) {
        # Pull the vector of allowed entries
        factor_list = qc_args$factors
        # Subset any incorrect records
        temp_df = subset(dframe, dframe[,dframe_fieldnames[i]] %in% factor_list == F)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = key_fieldname,
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Incorrect entries detected")
          temp_df$issue = "factor_not_allowed"
          dvr = rbind(dvr, temp_df)
        } else {
          catglue("  > All entries passed nchar check")
        }
      } else {
        stop(glued(" > {dframe_fieldnames[i]} is a factor field but list of factors is missing"))
      }
    }
    # is_date ----
    if ("is_date" %in% qc_arg_names) {
      catglue("> {dframe_fieldnames[i]} is a date field")
      if (class(dframe[,dframe_fieldnames[i]]) != "Date") {
        if ("date_format" %in% qc_arg_names) {
          dframe[,dframe_fieldnames[i]] = as.Date(dframe[,dframe_fieldnames[i]], tryFormats = c("%Y-%m-%d", qc_args$date_format))
          if (T %in% is.na(dframe[,dframe_fieldnames[i]])) {
            stop("  > date couldn't be formatted corectly")
          }
        } else if (all(is.na(as.Date(dframe[,dframe_fieldnames[i]], format = "%Y-%m-%d"))) == F) {
          dframe[,dframe_fieldnames[i]] = as.Date(dframe[,dframe_fieldnames[i]], format = "%Y-%m-%d")
        } else {
          stop("  > date couldn't be formatted corectly")
        }
      }
    }
    # is_numeric ----
    if ("is_numeric" %in% qc_arg_names) {
      catglue("> {dframe_fieldnames[i]} is a numeric field")
      # min_value ----
      if ("max_value" %in% qc_arg_names) {
        # Subset any incorrect records
        temp_df = subset(dframe, dframe[,dframe_fieldnames[i]] <= qc_args$min_value)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = key_fieldname,
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Entries larger than the max_value detected")
          temp_df$issue = "max_value_exceeded"
          dvr = rbind(dvr, temp_df)
        }
      }
      # max_value ----
      if ("max_value" %in% qc_arg_names) {
        # Subset any incorrect records
        temp_df = subset(dframe, dframe[,dframe_fieldnames[i]] <= qc_args$max_value)
        temp_df = temp_df %>%
          select(issue = dframe_fieldnames[i],
                 key = key_fieldname,
                 value = dframe_fieldnames[i])
        # Append to the data validation report if any incorrect records are present
        if (nrow(temp_df) != 0) {
          catglue("  > Entries larger than the max_value detected")
          temp_df$issue = "max_value_exceeded"
          dvr = rbind(dvr, temp_df)
        }
      }
      # length ----
      if ("length" %in% qc_arg_names) {
        # What is the length specified for the field
        length_arg = qc_args$length
        decimals = length_arg[2]
        if (decimals != 0) {
          nchar_arg = length_arg[1] + 1
          dframe[,dframe_fieldnames[i]] = formatC(dframe[,dframe_fieldnames[i]], width = decimals, format = "f")
        } else {
          nchar_arg = length_arg[1]
          dframe[,dframe_fieldnames[i]] = formatC(dframe[,dframe_fieldnames[i]], format = "g")
        }
        catglue("> {length_arg[1]},{length_arg[2]} format specified ")
      } else {
        stop("required attribute is missing from the config file")
      }

      # as_character ----
      if ("as_character" %in% qc_arg_names) {
        class(dframe[,dframe_fieldnames[i]]) = "character"
      }
    }
  }

  #  Data validation report generation ----
  catglue("/n/nData quality control complete")
  if (nrow(dvr) != 0) {
    catglue("> Dataset failed validation. Printing {nrow(dvr)} rows of validation report\n")
    dvr_fname = gsub(".csv", "_validation.csv", fname)
    write.csv(dvr, paste0(validation_dir, dvr_fname), row.names = T)
    return(F)
  } else {
    catglue("> Dataset passed validation\n")
    return(T)
  }
}
