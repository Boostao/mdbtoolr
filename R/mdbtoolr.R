.is_mdb_path <- function(x) {
  is.character(x) && length(x) == 1L && grepl("\\.(mdb|accdb)$", x, ignore.case = TRUE)
}

.as_table_name <- function(name) {
  if (inherits(name, "Id")) {
    vals <- unlist(name, use.names = FALSE)
    return(as.character(vals[[length(vals)]]) )
  }
  as.character(name[[1]])
}

.require_valid_connection <- function(conn) {
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid or closed MDB connection.", call. = FALSE)
  }
}

.native_list_tables <- function(path) {
  .Call("mdbtoolr_list_tables", path)
}

.native_list_queries <- function(path) {
  .Call("mdbtoolr_list_queries", path)
}

.native_list_fields <- function(path, table) {
  .Call("mdbtoolr_list_fields", path, table)
}

.native_table_num_rows <- function(path, table) {
  .Call("mdbtoolr_table_num_rows", path, table)
}

.native_read_table <- function(path, table) {
  .Call("mdbtoolr_read_table", path, table)
}

.native_run_query <- function(path, statement) {
  .Call("mdbtoolr_run_query", path, statement)
}

.native_get_query_sql <- function(path, query_name) {
  .Call("mdbtoolr_get_query_sql", path, query_name)
}

.native_print_schema <- function(path, table = NULL, backend = NULL, namespace = NULL, export_options = NULL) {
  .Call("mdbtoolr_print_schema", path, table, backend, namespace, export_options)
}

.native_mdbtools_version <- function() {
  .Call("mdbtoolr_version")
}

.native_file_format <- function(path) {
  .Call("mdbtoolr_file_format", path)
}

.native_prop_dump <- function(path, name, propcol = NULL) {
  .Call("mdbtoolr_prop_dump", path, name, propcol)
}

.trim_sql_semicolon <- function(x) {
  x <- trimws(x)
  sub(";\\s*$", "", x)
}

.expand_saved_query_statement <- function(path, statement) {
  pattern <- "^\\s*SELECT\\s+\\*\\s+FROM\\s+\\[([^\\]]+)\\]\\s*(?:LIMIT\\s+([0-9]+))?\\s*;?\\s*$"
  m <- regexec(pattern, statement, ignore.case = TRUE, perl = TRUE)
  hit <- regmatches(statement, m)[[1]]
  if (length(hit) == 0L) {
    return(statement)
  }

  query_name <- hit[[2]]
  limit <- if (length(hit) >= 3L) hit[[3]] else ""
  queries <- .native_list_queries(path)
  if (!query_name %in% queries) {
    return(statement)
  }

  query_sql <- .native_get_query_sql(path, query_name)
  query_sql <- .trim_sql_semicolon(query_sql)
  if (nzchar(limit)) {
    query_sql <- paste0(query_sql, " LIMIT ", limit)
  }
  query_sql
}

.as_data_frame <- function(x) {
  if (length(x) == 0L) {
    return(data.frame())
  }

  as.data.frame(x, check.names = FALSE, stringsAsFactors = FALSE, optional = TRUE)
}

.MDB_TYPE <- list(
  BOOL = 0x01L,
  BYTE = 0x02L,
  INT = 0x03L,
  LONGINT = 0x04L,
  MONEY = 0x05L,
  FLOAT = 0x06L,
  DOUBLE = 0x07L,
  DATETIME = 0x08L,
  BINARY = 0x09L,
  TEXT = 0x0aL,
  OLE = 0x0bL,
  MEMO = 0x0cL,
  REPID = 0x0fL,
  NUMERIC = 0x10L,
  COMPLEX = 0x12L
)

.normalize_string_na <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x
}

.coerce_logical <- function(x) {
  out <- rep(NA, length(x))
  if (!length(x)) {
    return(out)
  }

  y <- tolower(trimws(as.character(x)))
  false_vals <- c("0", "false", "f", "no", "n")
  true_vals <- c("1", "-1", "true", "t", "yes", "y")

  out[y %in% false_vals] <- FALSE
  out[y %in% true_vals] <- TRUE
  out[y == ""] <- NA
  out
}

.coerce_datetime <- function(x) {
  y <- .normalize_string_na(x)
  out <- rep(as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC"), length(y))
  if (!length(y)) {
    return(out)
  }

  formats <- c(
    "%Y-%m-%d %H:%M:%OS",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M:%OS",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%d/%m/%Y %H:%M:%OS",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y"
  )

  pending <- !is.na(y)
  for (fmt in formats) {
    if (!any(pending)) {
      break
    }
    parsed <- as.POSIXct(y[pending], format = fmt, tz = "UTC")
    ok <- !is.na(parsed)
    if (any(ok)) {
      idx <- which(pending)[ok]
      out[idx] <- parsed[ok]
      pending[idx] <- FALSE
    }
  }

  out
}

.coerce_column_by_type <- function(x, type_code) {
  type_code <- as.integer(type_code[[1]])

  if (is.na(type_code) || !length(x)) {
    return(x)
  }

  if (type_code == .MDB_TYPE$BOOL) {
    return(.coerce_logical(x))
  }

  if (type_code %in% c(.MDB_TYPE$BYTE, .MDB_TYPE$INT, .MDB_TYPE$LONGINT)) {
    return(suppressWarnings(as.integer(.normalize_string_na(x))))
  }

  if (type_code %in% c(.MDB_TYPE$MONEY, .MDB_TYPE$FLOAT, .MDB_TYPE$DOUBLE, .MDB_TYPE$NUMERIC)) {
    return(suppressWarnings(as.numeric(.normalize_string_na(x))))
  }

  if (type_code == .MDB_TYPE$DATETIME) {
    return(.coerce_datetime(x))
  }

  x
}

.coerce_mdb_data_frame <- function(df, source) {
  type_codes <- attr(source, "mdb_col_types", exact = TRUE)
  if (is.null(type_codes) || !length(type_codes) || !ncol(df)) {
    return(df)
  }

  n <- min(length(type_codes), ncol(df))
  for (i in seq_len(n)) {
    df[[i]] <- .coerce_column_by_type(df[[i]], type_codes[[i]])
  }

  df
}
