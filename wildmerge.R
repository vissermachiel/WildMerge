wildmerge <- function(data1, data2, include = "USUBJID", exclude = NULL) {
  # Function to merge two datasets on all possible keys and find differences
  # Input:
  #   data1:   dataset 1
  #   data2:   dataset 2
  #   include: character vector of variables to be included in keys
  #   exclude: character vector of variables to be excluded from keys
  # Output:
  #  list containing:
  #    data1:           dataset 1, including used row numbers
  #    data2:           dataset 2, including used row numbers
  #    cols_in:         vector with columns that are in both datasets
  #    cols_out:        vector with columns that are not in both datasets
  #    keys_list:       list of all possible keys to merge on
  #    keys_df:         dataframe with properties of all key combinations
  #    merges_list:     list of merges for each key combination
  #    iterative_merge: dataframe where merges are iteratively combined
  #    final_merge:     dataframe with final merge of both datasets
  #    difference:      dataframe with differences between datasets
  
  # Messages for wrong include and exclude specifications
  if(sum(colnames(data1) %in% exclude) != length(exclude)) {
    warning(paste("Specified exclude variable(s) not available in dataset 1."))
  }
  if(sum(colnames(data2) %in% exclude) != length(exclude)) {
    warning(paste("Specified exclude variable(s) not available in dataset 2."))
  }
  if(sum(colnames(data1) %in% include) != length(include)) {
    stop(paste("Specified include variable(s) not available in dataset 1."))
  }
  if(sum(colnames(data2) %in% include) != length(include)) {
    stop(paste("Specified include variable(s) not available in dataset 2."))
  }
  
  # Get column names that are in both datasets and are eligible as key variables
  cols_in <- colnames(data1)[colnames(data1) %in% colnames(data2) & 
                               !(colnames(data1) %in% c(include, exclude))]
  
  # Get column names that are not eligible as key variables
  cols_out <- unique(c(
    colnames(data1)[!colnames(data1) %in% c(cols_in, include)], 
    colnames(data2)[!colnames(data2) %in% c(cols_in, include)]
  ))
  
  # Create list with all combinations of column names that are in both datasets
  keys_list <- list()
  for (m in length(cols_in):1) {
    keys_list <- append(keys_list, combn(cols_in, m, simplify = FALSE))
  }
  if (!is.null(include)) {
    keys_list <- append(sapply(keys_list, function(z) {c(include, z)}), 
                        list(include))
  }
  
  # Create dataframe with key properties
  keys_df <- data.frame(
    key     = sapply(keys_list, paste, collapse = "_"),
    n_var   = sapply(keys_list, length),
    power.x = sapply(keys_list, function(z) {nrow(unique(data1[, z]))}),
    power.y = sapply(keys_list, function(z) {nrow(unique(data2[, z]))}),
    power   = sapply(keys_list, function(z) {nrow(unique(rbind(data1[, z],
                                                               data2[, z])))})
  )
  keys_df <- keys_df[order(-keys_df$power, -keys_df$n_var, keys_df$key), ]
  
  # Add row numbers to datasets
  x <- cbind("row.x" = as.numeric(row.names(data1)), data1)
  y <- cbind("row.y" = as.numeric(row.names(data2)), data2)
  
  # Create list with all merged datasets based on each key combination
  merges_list <- lapply(keys_df$key, function(z) {
    merge(x, y, by = unlist(strsplit(z, split = "_")))
  })
  names(merges_list) <- keys_df$key
  
  # Iteratively combine merged datasets in order of key importance
  for (i in 1:nrow(keys_df)) {
    if (i == 1) {
      iterative_merge <- cbind("key" = keys_df[i, "key"],
                               merge(x, y,
                                     by = unlist(strsplit(keys_df[i, "key"],
                                                          split = "_"))))
    }
    if (nrow(x) > 0 & nrow(y) > 0) {
      new_merge <- merge(x, y, by = unlist(strsplit(keys_df[i, "key"],
                                                    split = "_")))
      if (nrow(new_merge) > 0) {
        new_merge <- cbind("key" = keys_df[i, "key"],
                           new_merge)
        iterative_merge <- merge(iterative_merge, new_merge, all = TRUE)
      }
    }
    x <- x[!(x$row.x %in% iterative_merge$row.x), ]
    y <- y[!(y$row.y %in% iterative_merge$row.y), ]
    if (i == nrow(keys_df) & nrow(y) > 0) {
      colnames(y)[-1] <- paste(colnames(y)[-1], "y", sep = ".")
      iterative_merge <- merge(iterative_merge, y, all = TRUE)
      y <- y[!(y$row.y %in% iterative_merge$row.y), ]
    }
    if (i == nrow(keys_df) & nrow(x) > 0) {
      colnames(x)[-1] <- paste(colnames(x)[-1], "x", sep = ".")
      iterative_merge <- merge(iterative_merge, x, all = TRUE)
      x <- x[!(x$row.x %in% iterative_merge$row.x), ]
    }
  }
  iterative_merge <- iterative_merge[, c("key", "row.x", "row.y",
                                         sort(colnames(iterative_merge)[
                                           !(colnames(iterative_merge) %in% 
                                               c("key", "row.x", "row.y"))]))]
  iterative_merge <- iterative_merge[order(iterative_merge$row.x, 
                                           iterative_merge$row.y), ]
  rownames(iterative_merge) <- NULL
  
  # Final merge of both datasets
  final_merge <- iterative_merge[!is.na(iterative_merge$key), c(
    "key", "row.x", "row.y", 
    colnames(iterative_merge)[
      !(colnames(iterative_merge) %in% c("key", "row.x", "row.y")) &
        !(endsWith(colnames(iterative_merge), suffix = ".x") | 
            endsWith(colnames(iterative_merge), suffix = ".y"))
    ])]
  final_merge <- final_merge[order(final_merge$row.x, 
                                   final_merge$row.y), ]
  rownames(final_merge) <- NULL
  
  # Differences between both datasets
  differences <- iterative_merge[, !(colnames(iterative_merge) %in% 
                                       c(cols_in, cols_out))]
  differences <- differences[rowSums(!is.na(differences)) > 3, ]
  differences <- differences[, c("key", "row.x", "row.y",
                                 sort(colnames(differences)[
                                   !(colnames(differences) %in% 
                                       c("key", "row.x", "row.y"))]))]
  differences <- differences[order(differences$row.x, 
                                   differences$row.y), ]
  rownames(differences) <- NULL
  
  # Put all results in a list
  out <- list("data1"           = cbind("row.x" = as.numeric(row.names(data1)), 
                                        data1),
              "data2"           = cbind("row.y" = as.numeric(row.names(data2)), 
                                        data2),
              "cols_in"         = c(include, cols_in),
              "cols_out"        = cols_out,
              "keys_list"       = keys_list,
              "keys_df"         = keys_df,
              "merges_list"     = merges_list,
              "iterative_merge" = iterative_merge,
              "final_merge"     = final_merge,
              "differences"     = differences
  )
  return(out)
}

out <- wildmerge(
  data1   = readxl::read_excel("data.xlsx", sheet = "crf_vs"), 
  data2   = readxl::read_excel("data.xlsx", sheet = "ext_vs"),
  include = c("USUBJID", "VISIT"),
  exclude = c("SEQ", "STUDYID")
)

out

out$data1
out$data2
out$cols_in
out$cols_out
out$keys_list
out$keys_df
out$merges_list
out$iterative_merge
out$final_merge
out$differences
