
#' Wrap single quotes around values for character, Date, factor, ordered
#' factor fields and POSIXct date or date/time vectors
#'
#' Useful for creating SQL strings.
#' @param x A vector.
#' @return The same vector with single quotes around values if needed e.g. 
#' to use in a SQL statement.
#' @examples
#' quote_for_sql_if_needed(CO2$Treatment)
#' quote_for_sql_if_needed(CO2$uptake)
quote_for_sql_if_needed <- function(x,
                                    quote_list = c('character', 'Date',
                                                   'factor', 'orderedfactor',
                                                   'POSIXct', 'POSIXctPOSIXt'
                                    )
) {
  if (class(x) %>% paste(collapse = "") %in% quote_list) {
    paste0("'", x, "'")
  } else {
    x
  }
}


#' Create a set of key-value pair strings for use in constructing SQL queries.
#'
#' Intended for use in creating WHERE statments as well as UPSERT operations.
#' @param df A data frame.
#' @param field A string identifying which field in the data frame to be used.
#' @param ... Parameters passed to quote_for_sql_if_needed e.g. quote_list
#' @return A character vector of key-value pairs.
#' @examples
#' key_value_pairs(CO2, "Treatment")
#' key_value_pairs(CO2, "uptake")
key_value_pairs <- function(df, field, ...) {
  vals <- quote_for_sql_if_needed(df[,field], ...)
  kv_pairs <- data.frame(paste(field, vals, sep = " = "),
                         stringsAsFactors = FALSE
  )
  names(kv_pairs) <- field
  kv_pairs
}


#' Create a SQL WHERE clause defining table rows matching a given data frame.
#'
#' Creates a WHERE clause which can be added to a SQL statement such as
#' SELECT or UPDATE in order to define rows in a database as matching those
#' in a given data frame.  Useful for creating custom UPSERT operations. Will
#' wrap single quotes around values for character, Date, factor and ordered
#' factor fields.
#' @param df A data frame containing primary keys with which to query the db.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @return A string with a SQL WHERE clause describing matching records.
#' @example matching_where(CO2, c("Plant", "Type", "Treatment", "conc"))
matching_where <- function(df, pk_fields) {
  field_vecs <- matrix(nrow = nrow(df), ncol = 0) %>% data.frame
  
  for (pkf in pk_fields) {
    field_vecs <- cbind(field_vecs, key_value_pairs(df, pkf))
  }
  
  full_clause <- apply(field_vecs, 1, paste, collapse = ' and ')
  all_clauses <- paste("(", full_clause, ")", sep = "", collapse = " or ")
  paste("WHERE", all_clauses)
}


#' Return sets of primary keys from a db matching primary keys in a data frame.
#' 
#' @param con A connection object.
#' @param df A data frame.
#' @param table A string naming the target table in the database.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @return A data frame of matching primary key fields from the database.
get_matching_records <- function(con, df, table, pk_fields) {
  check_where <- matching_where(df, pk_fields)
  qry_check <- paste("SELECT", 
                     paste(pk_fields, collapse = ", "), 
                     "FROM", 
                     table, 
                     check_where
  )
  dbGetQuery2(con, qry_check)
}


#' Match records in a data frame with db records by primary keys batch by batch.
#' 
#' Gets around the stack depth limit encountered when passing a very long WHERE
#' clause to SQL. Useful for planning an upsert operation (esp. separating 
#' updates from inserts).
#' @param con A connection object.
#' @param import A data frame.
#' @param batch_size The number of records to match at once.
#' @param ... Additional arguments to pass to get_matching_records.
#' @return A data frame of primary keys found in both the df and the database.
matching_records_builder <- function(con, import, batch_size = 1000, 
                                     verbose = FALSE, ...) {
  last_row <- nrow(import)
  if (last_row <= batch_size) {
    return(get_matching_records(con, import, ...))
  }
  
  matching_records <- get_matching_records(con, import[1:batch_size,], ...)
  i <- batch_size + 1
  while(i < last_row) {
    next_range <- i:min(i+batch_size-1, last_row)
    if(verbose) {print(c(min(next_range), max(next_range)))}
    matching_records <- rbind(matching_records, get_matching_records(con, import[next_range,], ...))
    i <- i + batch_size
  }
  matching_records
}



#' Create a SQL UPDATE statement from a data frame.
#'
#' Useful for creating custom UPSERT operations. Will wrap single quotes around 
#' values for character, Date, factor and ordered factor fields.
#' Implements answer to http://stackoverflow.com/questions/18797608.
#' Currently converts all fields to text in the where clause to obviate need
#' to specify types for Postgres.
#' @param df A data frame containing the primary keys and values for the update.
#' @param table A string naming the target table in the database.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @param set_fields A character vector naming the fields to be updated.
#' @param ... Additional arguments esp. postgres_types
#' @return A string comprising a SQL update statement.
#' @examples
#' write_update_statement(CO2, "co2", c("Plant", "Type"), c("conc", "uptake"))
#' write_update_statement(CO2, "co2", c("Type"), c("uptake"), c("TEXT", NA))
write_update_statement <- function(df, table, pk_fields, set_fields, ...) {
  # create pairs of value colums- one for each of the set_fields
  match_list <- paste(paste(set_fields, "=", 
                            paste0("newvals.", set_fields)
  ),
  collapse = ", ")
  
  all_cols <- c(pk_fields, set_fields)
  
  values <- write_values_tuples(df[, all_cols], ...)
  
  combined_col_names <- paste(all_cols, collapse = ", ")
  
  where_list <- update_where(table, pk_fields, ...)
  
  paste0("UPDATE ", table,
         " SET ", match_list,
         " FROM (VALUES ", values,
         ") as newvals(", combined_col_names,
         ") ", where_list)
}

#' Create a WHERE clause for use in a SQL UPDATE statement.
#' 
#' @param table A string naming the target table.
#' @param pk_fields A character vector naming the primary key fields.
#' @param postgres_types A character vector with values of either NA or the 
#' desired Postgresql data type e.g. "text", "date". Matched to df by position.
#' It is assumed that the primary keys are matched to the first set of values in
#' postgres_types (which could also refer to the value fields).
#' @return A string with a where clause appropriate for a SQL UPDATE statement.
#' @example
#' update_where("db_table", c("A", "B"), c(NA, "date", NA, "text"))
update_where <- function(table, pk_fields, postgres_types = rep(NA, length(pk_fields))) {
  
  newvals_pk_fields <- pk_fields
  
  for (i in 1:length(newvals_pk_fields)) {
    if (!is.na(postgres_types[i])) {
      newvals_pk_fields[i] <- paste0(newvals_pk_fields[i], "::", postgres_types[i])
    }
  }
  
  paste("WHERE",
        paste(paste0(table, ".", pk_fields),
              "=",
              paste0("newvals.", newvals_pk_fields), 
              collapse = " AND "
        )
  )
}


#' Create a set of tuples from rows in a data frame.
#' 
#' Intended for use in creating a SQL update statement. Set postgres_types only
#' if needed to match data type esp. for dates, timestamps.
#' See format at http://www.postgresql.org/docs/current/static/sql-values.html.
#' See also http://www.postgresql.org/docs/9.4/static/datatype.html for types.
#' @param df A data frame.
#' @param ... Additional arguments esp. postgres_types
#' @return A single string with a comma-separated list of values tuples.
#' @example write_values_tuples(head(CO2, 10), c(NA, NA, NA, "text", NA))
write_values_tuples <- function(df, ...) {
  df[,] <- lapply(df, quote_for_sql_if_needed)
  
  df <- cast_types_if_needed(df, ...)
  
  tuples <- paste0("(", apply(df, 1, paste0, collapse = ", "), ")")
  paste0(tuples, collapse = ", ")
}

#' Append "::type" to values in a row of a data frame for Postgres type casting.
#'
#' This works on the first row of a data frame.  It is intended for use in
#' constructing values tuples (which only require the first tuple to be cast).
#' The types vector must have an entry for each column in df.
#' @param df A data frame.  Only the first row will be transformed
#' @param postgres_types A character vector with values of either NA or the 
#' desired Postgresql data type e.g. "text", "date". Matched to df by position.
#' @return The original data frame with the first row transformed.
#' @example cast_types_if_needed(CO2[1:2,], c(NA, NA, NA, "text", NA))
cast_types_if_needed <- function(df, postgres_types = rep(NA, ncol(df))) {
  if (ncol(df) != length(postgres_types)) {
    stop("Error: ncol(df) must equal length(postgres_types)")
  }
  
  for (i in 1:ncol(df)) {
    if (!is.na(postgres_types[i])) {
      df[1, i] <- paste0(df[1, i], "::", postgres_types[i])
    }
  }
  df
}
