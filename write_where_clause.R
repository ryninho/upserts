#' Create a SQL WHERE clause defining table rows matching a given data frame.
#' 
#' Creates a WHERE clause which can be added to a SQL statement such as 
#' SELECT or UPDATE in order to define rows in a database as matching those
#' in a given data frame.  Useful for creating custom UPSERT operations.
#' @param df A data frame containing primary keys with which to query the db.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @return A string with a SQL WHERE clause describing matching records.
#' @examples write_where_clause(CO2, c("Plant", "Type", "Treatment"))
write_where_clause <- function(df, pk_fields) {
  field_vecs <- matrix(nrow = nrow(df), ncol = 0) %>% data.frame
  
  for (pkf in pk_fields) {
    
    if (class(df[,pkf]) %>% paste(collapse = "") %in% 
        c('character', 'Date', 'factor', 'orderedfactor')
    ) {
      pkf_col <- paste0("'", df[,pkf], "'")
    } else {
      pkf_col <- df[,pkf]
    }
    
    field_vecs <- cbind(field_vecs, 
                        data.frame(paste(pkf, pkf_col, sep = " = "),
                                   stringsAsFactors = FALSE
                        )
    )
  }
  
  full_clause <- apply(field_vecs, 1, paste, collapse = ' and ')
  all_clauses <- paste("(", full_clause, ")", sep = "", collapse = " or ")
  paste("WHERE", all_clauses)
}
