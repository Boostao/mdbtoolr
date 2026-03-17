.mdb_normalize_path <- function(path) {
  normalizePath(path, mustWork = TRUE)
}

.mdb_connect <- function(path) {
  DBI::dbConnect(mdb(), dbname = .mdb_normalize_path(path))
}

.mdb_schema_options <- function(drop_table = FALSE, not_null = TRUE, default_values = FALSE,
                                not_empty = FALSE, comments = TRUE, indexes = TRUE,
                                relations = TRUE) {
  opts <- 0L
  if (isTRUE(drop_table)) opts <- bitwOr(opts, 1L)
  if (isTRUE(not_null)) opts <- bitwOr(opts, bitwShiftL(1L, 1L))
  if (isTRUE(not_empty)) opts <- bitwOr(opts, bitwShiftL(1L, 2L))
  if (isTRUE(comments)) opts <- bitwOr(opts, bitwShiftL(1L, 3L))
  if (isTRUE(default_values)) opts <- bitwOr(opts, bitwShiftL(1L, 4L))
  if (isTRUE(indexes)) opts <- bitwOr(opts, bitwShiftL(1L, 5L))
  if (isTRUE(relations)) opts <- bitwOr(opts, bitwShiftL(1L, 6L))
  as.integer(opts)
}

.mdb_quote_ident <- function(x) {
  x <- as.character(x)
  x <- gsub("]", "]]", x, fixed = TRUE)
  paste0("[", x, "]")
}

.mdb_field_text <- function(x, null = "", no_quote = FALSE, quote = '"', escape = NULL, escape_invisible = FALSE) {
  if (is.na(x)) {
    return(null)
  }

  value <- as.character(x)
  if (escape_invisible) {
    value <- gsub("\\\\", "\\\\\\\\", value, fixed = TRUE)
    value <- gsub("\\r", "\\\\r", value, fixed = TRUE)
    value <- gsub("\\n", "\\\\n", value, fixed = TRUE)
    value <- gsub("\\t", "\\\\t", value, fixed = TRUE)
  }

  if (isTRUE(no_quote)) {
    return(value)
  }

  if (is.null(escape)) {
    value <- gsub(quote, paste0(quote, quote), value, fixed = TRUE)
  } else {
    value <- gsub(quote, paste0(escape, quote), value, fixed = TRUE)
  }
  paste0(quote, value, quote)
}

.mdb_apply_datetime_formats <- function(df, date_format = "%Y-%m-%d", datetime_format = "%Y-%m-%d %H:%M:%S") {
  out <- df
  for (nm in names(out)) {
    if (inherits(out[[nm]], "POSIXct")) {
      out[[nm]] <- ifelse(is.na(out[[nm]]), NA_character_, format(out[[nm]], datetime_format, tz = "UTC"))
    } else if (inherits(out[[nm]], "Date")) {
      out[[nm]] <- ifelse(is.na(out[[nm]]), NA_character_, format(out[[nm]], date_format))
    }
  }
  out
}

.mdb_apply_boolean_words <- function(df, boolean_words = FALSE) {
  out <- df
  for (nm in names(out)) {
    if (is.logical(out[[nm]])) {
      if (isTRUE(boolean_words)) {
        out[[nm]] <- ifelse(is.na(out[[nm]]), NA_character_, ifelse(out[[nm]], "TRUE", "FALSE"))
      } else {
        out[[nm]] <- ifelse(is.na(out[[nm]]), NA_integer_, ifelse(out[[nm]], 1L, 0L))
      }
    }
  }
  out
}

.mdb_apply_unprintable <- function(df, no_unprintable = FALSE) {
  if (!isTRUE(no_unprintable)) {
    return(df)
  }
  out <- df
  for (nm in names(out)) {
    if (is.character(out[[nm]])) {
      out[[nm]] <- gsub("[^[:print:]\\t\\r\\n]", " ", out[[nm]], perl = TRUE)
    }
  }
  out
}

.mdb_delimited_text <- function(df, delimiter = "\t", row_delimiter = "\n", header = TRUE, null = "", no_quote = FALSE, quote = '"', escape = NULL, escape_invisible = FALSE) {
  rows <- character(0)
  if (isTRUE(header)) {
    rows <- c(rows, paste(names(df), collapse = delimiter))
  }
  if (nrow(df) > 0L) {
    body <- apply(df, 1L, function(row) {
      vals <- vapply(
        row,
        .mdb_field_text,
        FUN.VALUE = character(1),
        null = null,
        no_quote = no_quote,
        quote = quote,
        escape = escape,
        escape_invisible = escape_invisible
      )
      paste(vals, collapse = delimiter)
    })
    rows <- c(rows, unname(body))
  }
  paste(rows, collapse = row_delimiter)
}

.mdb_sql_literal <- function(x) {
  if (is.na(x)) {
    return("NULL")
  }
  if (is.numeric(x)) {
    return(as.character(x))
  }
  if (is.logical(x)) {
    return(if (isTRUE(x)) "1" else "0")
  }
  val <- as.character(x)
  val <- gsub("'", "''", val, fixed = TRUE)
  paste0("'", val, "'")
}

.mdb_query_table <- function(path, table, n = -1L) {
  table <- as.character(table[[1]])
  sql <- sprintf("SELECT * FROM %s", .mdb_quote_ident(table))
  if (!is.null(n) && is.finite(n) && n >= 0) {
    sql <- paste(sql, "LIMIT", as.integer(n))
  }
  mdb_sql(path = path, statement = sql)
}

.mdb_read_sql_input <- function(input) {
  lines <- readLines(input, warn = FALSE)
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  lines <- lines[!startsWith(lines, "#")]
  lines <- lines[tolower(lines) != "go"]
  script <- paste(lines, collapse = "\n")

  parts <- strsplit(script, ";", fixed = TRUE)[[1]]
  parts <- trimws(parts)
  parts <- parts[nzchar(parts)]
  parts
}

#' Get Listing Of Tables In An MDB Database
#'
#' `mdb-tables` is a utility program distributed with MDB Tools.
#' It produces a list of tables contained within an MDB database in a format
#' suitable for use in shell scripts.
#'
#' This wrapper keeps the same option surface where practical, but returns R
#' vectors by default and can collapse to CLI-like text with `as_text = TRUE`.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param system Logical, equivalent to `-S/--system`.
#' @param single_column Logical, equivalent to `-1/--single-column`.
#' @param delimiter Delimiter equivalent to `-d/--delimiter`.
#' @param type Object type equivalent to `-t/--type`.
#' @param show_type Logical, equivalent to `-T/--showtype`.
#' @param as_text Logical; when `TRUE`, return one delimited string.
#'
#' @return Character vector by default, or scalar character when `as_text = TRUE`.
#' @export
mdb_tables <- function(path, system = FALSE, single_column = FALSE, delimiter = NULL,
                       type = c("table", "query", "systable", "any", "all", "form", "macro", "report", "linkedtable", "module", "relationship", "dbprop"),
                       show_type = FALSE, as_text = FALSE) {
  type <- match.arg(type)
  con <- .mdb_connect(path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  tables <- DBI::dbListTables(con)
  user_tables <- tables[!grepl("^MSys", tables)]
  sys_tables <- tables[grepl("^MSys", tables)]
  queries <- .native_list_queries(.mdb_normalize_path(path))

  out <- switch(
    type,
    table = user_tables,
    query = queries,
    systable = sys_tables,
    any = c(user_tables, if (isTRUE(system)) sys_tables, queries),
    all = c(user_tables, if (isTRUE(system)) sys_tables, queries),
    {
      warning(sprintf("Type '%s' is not available in library-only mode; returning empty result.", type), call. = FALSE)
      character(0)
    }
  )

  if (type %in% c("table", "any", "all") && isFALSE(system)) {
    out <- out[!grepl("^MSys", out)]
  }

  if (isTRUE(show_type)) {
    label <- if (type %in% c("query")) "query" else "table"
    out <- paste(label, out)
  }

  delim <- if (!is.null(delimiter)) delimiter else if (isTRUE(single_column)) "\n" else "\t"
  if (isTRUE(as_text)) {
    return(paste(out, collapse = delim))
  }
  out
}

#' Get Listing Of Queries In An MDB Database
#'
#' `mdb-queries` is a utility program distributed with MDB Tools.
#' It produces a list of queries in the database, and dumps the SQL associated
#' with a specific query.
#'
#' This wrapper returns vectors by default and can collapse listings to CLI-like
#' text with `as_text = TRUE`.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param query Optional query name; when provided, SQL text is returned.
#' @param list Logical, equivalent to `-L/--list`.
#' @param newline Logical, equivalent to `-1/--newline`.
#' @param delimiter Delimiter equivalent to `-d/--delimiter` (default single space).
#' @param as_text Logical; when `TRUE`, returns delimited scalar text for listings.
#'
#' @return Character vector of query names, query SQL text, or delimited scalar text.
#' @export
mdb_queries <- function(path, query = NULL, list = TRUE, newline = FALSE, delimiter = " ", as_text = FALSE) {
  path <- .mdb_normalize_path(path)
  if (!is.null(query) && nzchar(as.character(query[[1]]))) {
    return(.native_get_query_sql(path, as.character(query[[1]])))
  }
  if (!isTRUE(list)) {
    return(character(0))
  }
  out <- .native_list_queries(path)
  delim <- if (isTRUE(newline)) "\n" else delimiter
  if (isTRUE(as_text)) {
    return(paste(out, collapse = delim))
  }
  out
}

#' SQL Interface To MDB Tools
#'
#' `mdb-sql` is a utility program distributed with MDB Tools.
#' It allows querying of an MDB database using a limited SQL subset language.
#' The supported SQL is intentionally small: single-table queries, no aggregates,
#' and limited `WHERE` support.
#'
#' In addition to single statements, this wrapper accepts `input` files similar
#' to `mdb-sql -i file`, strips `go` batch terminators, and executes the script
#' one statement at a time.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param statement SQL statement text.
#' @param no_header Logical, equivalent to `-H/--no-header` (for text mode).
#' @param no_footer Logical, equivalent to `-F/--no-footer` (for text mode).
#' @param no_pretty_print Logical, equivalent to `-p/--no-pretty-print`.
#' @param delimiter Delimiter equivalent to `-d/--delimiter` in plain text mode.
#' @param input Input file equivalent to `-i/--input`.
#' @param output Output file equivalent to `-o/--output`.
#' @param as_text Logical; when `TRUE`, returns CLI-like text output.
#'
#' @importFrom utils capture.output
#' @return `data.frame` by default, or character scalar in text mode.
#' @export
mdb_sql <- function(path, statement = NULL, no_header = FALSE, no_footer = FALSE,
                    no_pretty_print = FALSE, delimiter = "\t", input = NULL,
                    output = NULL, as_text = FALSE) {
  if (!is.null(input)) {
    statements <- .mdb_read_sql_input(input)
  } else {
    statements <- statement
  }

  if (is.null(statements) || !length(statements) || !any(nzchar(trimws(statements)))) {
    stop("Provide `statement` or `input`.", call. = FALSE)
  }

  statements <- as.character(statements)
  statements <- trimws(statements)
  statements <- statements[nzchar(statements)]

  con <- .mdb_connect(path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  text_blocks <- character(0)
  last_df <- data.frame()
  for (stmt in statements) {
    df <- DBI::dbGetQuery(con, stmt)
    last_df <- df

    if (!isTRUE(as_text) && is.null(output) && length(statements) == 1L) {
      return(df)
    }

    if (isTRUE(no_pretty_print)) {
      block <- .mdb_delimited_text(
        df,
        delimiter = delimiter,
        row_delimiter = "\n",
        header = !isTRUE(no_header),
        null = "",
        no_quote = TRUE
      )
    } else {
      lines <- capture.output(print(df, row.names = FALSE))
      if (isTRUE(no_header) && length(lines) > 0L) {
        lines <- lines[-1L]
      }
      block <- paste(lines, collapse = "\n")
    }

    if (!isTRUE(no_footer)) {
      block <- paste0(block, "\n", nrow(df), " row(s)")
    }
    text_blocks <- c(text_blocks, block)
  }

  if (!isTRUE(as_text) && is.null(output)) {
    return(last_df)
  }

  text <- paste(text_blocks, collapse = "\n")
  if (!is.null(output)) {
    writeLines(text, con = output, useBytes = TRUE)
  }
  text
}

#' Count Rows In Table
#'
#' `mdb-count` is a utility program distributed with MDB Tools.
#' It outputs the number of rows in a table.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Table name.
#' @param where Optional SQL predicate appended to `WHERE`. This is an R-side
#' extension and is not part of the CLI.
#' @param version Logical; when `TRUE`, return mdbtools version (`--version`).
#'
#' @return Integer row count or version string.
#' @export
mdb_count <- function(path, table = NULL, where = NULL, version = FALSE) {
  if (isTRUE(version)) {
    return(mdb_ver())
  }
  if (is.null(table)) {
    stop("`table` is required unless `version = TRUE`.", call. = FALSE)
  }
  table <- as.character(table[[1]])

  if (is.null(where) || !nzchar(trimws(where))) {
    return(as.integer(.native_table_num_rows(.mdb_normalize_path(path), table)))
  }

  # Read a single column to avoid materializing wide rows when only row count is needed.
  where_sql <- if (!is.null(where) && nzchar(trimws(where))) paste(" WHERE", where) else ""
  fields <- tryCatch(
    .native_list_fields(.mdb_normalize_path(path), table),
    error = function(e) character(0)
  )

  if (length(fields) > 0L && nzchar(fields[[1]])) {
    probe_sql <- sprintf(
      "SELECT %s FROM %s%s",
      .mdb_quote_ident(fields[[1]]),
      .mdb_quote_ident(table),
      where_sql
    )
    rows <- mdb_sql(path, probe_sql)
    return(as.integer(nrow(rows)))
  }

  rows <- mdb_sql(path, sprintf("SELECT * FROM %s%s", .mdb_quote_ident(table), where_sql))
  as.integer(nrow(rows))
}

#' Generate Schema Creation DDL
#'
#' `mdb-schema` is a utility program distributed with MDB Tools.
#' It produces DDL (data definition language) output for the given database.
#' This can be passed to another database to create a replica of the original
#' Access table format.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Single table option, equivalent to `-T/--table`. Default is to
#' export all user tables.
#' @param namespace Prefix identifiers with namespace, equivalent to
#' `-N/--namespace`.
#' @param backend Target DDL dialect. Supported values are `access`, `sybase`,
#' `oracle`, `postgres`, `mysql`, and `sqlite`.
#' @param drop_table Issue `DROP TABLE` statements.
#' @param not_null Include `NOT NULL` constraints.
#' @param default_values Include `DEFAULT` values.
#' @param not_empty Include `CHECK <> ''` constraints.
#' @param comments Include `COMMENT ON` statements.
#' @param indexes Export indexes.
#' @param relations Export foreign key constraints.
#'
#' @return Character scalar containing the generated DDL.
#' @export
mdb_schema <- function(path, table = NULL, namespace = NULL,
                       backend = c("access", "sybase", "oracle", "postgres", "mysql", "sqlite"),
                       drop_table = FALSE, not_null = TRUE, default_values = FALSE,
                       not_empty = FALSE, comments = TRUE, indexes = TRUE,
                       relations = TRUE) {
  backend <- match.arg(backend)
  export_options <- .mdb_schema_options(
    drop_table = drop_table,
    not_null = not_null,
    default_values = default_values,
    not_empty = not_empty,
    comments = comments,
    indexes = indexes,
    relations = relations
  )

  .native_print_schema(
    path = .mdb_normalize_path(path),
    table = if (!is.null(table)) as.character(table[[1]]) else NULL,
    backend = backend,
    namespace = if (!is.null(namespace)) as.character(namespace[[1]]) else NULL,
    export_options = export_options
  )
}

#' Return MDB File Format Or MDB Tools Version
#'
#' `mdb-ver` will return a single line of output corresponding to the program
#' that produced the file: `JET3` (for files produced by Access 97), `JET4`
#' (Access 2000, XP and 2003), `ACE12` (Access 2007), `ACE14` (Access 2010),
#' `ACE15` (Access 2013), or `ACE16` (Access 2016).
#'
#' @param path Optional database path. When omitted, the wrapper returns the
#' mdbtools package version for backward compatibility.
#' @param version Logical, equivalent to `-M/--version`.
#'
#' @return Single character string with file format or mdbtools version.
#' @export
mdb_ver <- function(path = NULL, version = FALSE) {
  if (isTRUE(version) || is.null(path)) {
    return(.native_mdbtools_version())
  }
  .native_file_format(.mdb_normalize_path(path))
}

#' Export Table As List Columns (mdb-array mimic)
#'
#' `mdb-array(1)` emits C source; this wrapper returns a named R list of columns
#' while keeping equivalent database/table inputs.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Table name.
#' @param columns Optional character vector of columns.
#' @param n Optional row limit (`LIMIT n`).
#'
#' @return Named list, one entry per selected column.
#' @export
mdb_array <- function(path, table, columns = NULL, n = -1L) {
  table <- as.character(table[[1]])
  col_sql <- if (is.null(columns) || !length(columns)) "*" else paste(.mdb_quote_ident(columns), collapse = ", ")
  sql <- sprintf("SELECT %s FROM %s", col_sql, .mdb_quote_ident(table))
  if (!is.null(n) && is.finite(n) && n >= 0) {
    sql <- paste(sql, "LIMIT", as.integer(n))
  }
  as.list(mdb_sql(path, sql))
}

#' Export Data In An MDB Table To CSV Or INSERT SQL
#'
#' `mdb-export` is a utility program distributed with MDB Tools.
#' It produces CSV output for the given table. Such output is suitable for
#' importation into databases or spreadsheets.
#'
#' Used with `insert`, it outputs backend-specific SQL `INSERT` statements.
#' Most formatting options also apply in insert mode.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Table name.
#' @param no_header Logical, equivalent to `-H/--no-header`.
#' @param delimiter Equivalent to `-d/--delimiter`.
#' @param row_delimiter Equivalent to `-R/--row-delimiter`.
#' @param no_quote Equivalent to `-Q/--no-quote`.
#' @param quote Equivalent to `-q/--quote`.
#' @param escape Equivalent to `-X/--escape`.
#' @param escape_invisible Equivalent to `-e/--escape-invisible`.
#' @param date_format Equivalent to `-D/--date-format`.
#' @param datetime_format Equivalent to `-T/--datetime-format`.
#' @param null Equivalent to `-0/--null`.
#' @param bin Binary mode (`strip`, `raw`, `octal`, `hex`) for parity.
#' @param boolean_words Equivalent to `-B/--boolean-words`.
#' @param insert Backend for `-I/--insert` mode.
#' @param namespace Equivalent to `-N/--namespace`.
#' @param batch_size Equivalent to `-S/--batch-size`.
#' @param n Optional row limit (`LIMIT n`).
#'
#' @return Character scalar containing CSV or SQL INSERT text.
#' @export
mdb_export <- function(path, table, no_header = FALSE, delimiter = ",", row_delimiter = "\n",
                       no_quote = FALSE, quote = '"', escape = NULL, escape_invisible = FALSE,
                       date_format = "%Y-%m-%d", datetime_format = "%Y-%m-%d %H:%M:%S",
                       null = "", bin = c("strip", "raw", "octal", "hex"),
                       boolean_words = FALSE, insert = NULL, namespace = NULL,
                       batch_size = 1L, n = -1L) {
  bin <- match.arg(bin)
  df <- .mdb_query_table(path, table, n = n)
  df <- .mdb_apply_datetime_formats(df, date_format = date_format, datetime_format = datetime_format)
  df <- .mdb_apply_boolean_words(df, boolean_words = boolean_words)

  if (identical(bin, "strip")) {
    for (nm in names(df)) {
      if (is.raw(df[[nm]])) {
        df[[nm]] <- NA_character_
      }
    }
  }

  if (is.null(insert)) {
    return(.mdb_delimited_text(
      df,
      delimiter = delimiter,
      row_delimiter = row_delimiter,
      header = !isTRUE(no_header),
      null = null,
      no_quote = no_quote,
      quote = quote,
      escape = escape,
      escape_invisible = escape_invisible
    ))
  }

  backend <- match.arg(insert, c("access", "sybase", "oracle", "postgres", "mysql", "sqlite"))
  batch_size <- max(1L, as.integer(batch_size[[1]]))
  target <- .mdb_quote_ident(as.character(table[[1]]))
  if (!is.null(namespace) && nzchar(namespace)) {
    target <- paste0(.mdb_quote_ident(namespace), ".", target)
  }

  cols <- paste(.mdb_quote_ident(names(df)), collapse = ", ")
  out <- character(0)
  if (nrow(df) == 0L) {
    return("")
  }

  for (i in seq(1L, nrow(df), by = batch_size)) {
    j <- min(i + batch_size - 1L, nrow(df))
    chunk <- df[i:j, , drop = FALSE]
    vals <- apply(chunk, 1L, function(row) {
      paste0("(", paste(vapply(row, .mdb_sql_literal, FUN.VALUE = character(1)), collapse = ", "), ")")
    })
    stmt <- sprintf("INSERT INTO %s (%s) VALUES %s;", target, cols, paste(vals, collapse = ", "))
    if (backend %in% c("access", "oracle", "sybase") && length(vals) > 1L) {
      stmt <- paste(sprintf("INSERT INTO %s (%s) VALUES %s;", target, cols, vals), collapse = "\n")
    }
    out <- c(out, stmt)
  }
  paste(out, collapse = "\n")
}

#' MDB Header Summary (mdb-header mimic)
#'
#' `mdb-header(1)` writes C files; this wrapper returns a structured summary.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#'
#' @return Named list with version, table names and query names.
#' @export
mdb_header <- function(path) {
  list(
    version = mdb_ver(),
    tables = mdb_tables(path, system = TRUE),
    queries = mdb_queries(path)
  )
}

#' Hexdump MDB File (mdb-hexdump mimic)
#'
#' @param path Path to file.
#' @param pagenumber Optional page index (0-based) like `mdb-hexdump file [pagenumber]`.
#' @param page_size Page size in bytes (default 4096 for modern Jet/ACE).
#' @param n Number of bytes to emit.
#'
#' @return Single hexadecimal string.
#' @export
mdb_hexdump <- function(path, pagenumber = NULL, page_size = 4096L, n = 256L) {
  con <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  if (!is.null(pagenumber)) {
    seek(con, where = as.integer(pagenumber) * as.integer(page_size), origin = "start")
  }
  bytes <- readBin(con, what = "raw", n = as.integer(n))
  paste(sprintf("%02X", as.integer(bytes)), collapse = " ")
}

#' Import CSV Into MDB (mdb-import mimic)
#'
#' `mdb-import(1)` writes to MDB files. This package is read-only, so this
#' function validates CLI-like options and then errors.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Table name.
#' @param csvfile CSV file path.
#' @param header_lines Equivalent to `-H/--header` skipped lines.
#' @param delimiter Equivalent to `-d/--delimiter`.
#'
#' @return No return; always errors in read-only mode.
#' @export
mdb_import <- function(path, table, csvfile, header_lines = 0L, delimiter = ",") {
  stop("`mdb_import()` is not available: this package currently supports read-only MDB/ACCDB operations.", call. = FALSE)
}

#' Export Data In An MDB Table To JSON
#'
#' `mdb-json` is a utility program distributed with MDB Tools.
#' It produces JSON output for the given table. Such output is suitable for
#' parsing in a variety of languages.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param table Table name.
#' @param date_format Equivalent to `-D/--date-format`.
#' @param time_format Equivalent to `-T/--time-format`.
#' @param no_unprintable Equivalent to `-U/--no-unprintable`.
#' @param n Optional row limit.
#'
#' @return JSON string.
#' @importFrom jsonlite toJSON
#' @export
mdb_json <- function(path, table, date_format = "%Y-%m-%d", time_format = "%Y-%m-%d %H:%M:%S",
                     no_unprintable = FALSE, n = -1L) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("`mdb_json()` requires the `jsonlite` package.", call. = FALSE)
  }
  df <- .mdb_query_table(path, table, n = n)
  df <- .mdb_apply_datetime_formats(df, date_format = date_format, datetime_format = time_format)
  df <- .mdb_apply_unprintable(df, no_unprintable = no_unprintable)
  jsonlite::toJSON(df, dataframe = "rows", na = "null", auto_unbox = TRUE)
}

#' Parse CSV To C Source (mdb-parsecsv mimic)
#'
#' `mdb-parsecsv(1)` converts CSV to C arrays. This wrapper returns generated C
#' source text and can optionally write it to a file.
#'
#' @param file CSV file path.
#' @param output_file Optional destination `.c` file. If `NULL`, returns text.
#' @param sep Field separator.
#' @param header Logical; whether CSV includes header row.
#'
#' @return Character scalar with generated C code (invisibly when written).
#' @export
mdb_parsecsv <- function(file, output_file = NULL, sep = ",", header = TRUE) {
  if (!file.exists(file)) {
    alt <- paste0(file, ".txt")
    if (file.exists(alt)) {
      file <- alt
    } else {
      stop("Input CSV file does not exist.", call. = FALSE)
    }
  }

  df <- utils::read.csv(file, sep = sep, header = header, stringsAsFactors = FALSE)
  base <- tools::file_path_sans_ext(basename(file))
  symbol <- gsub("[^A-Za-z0-9_]", "_", base)
  rows <- apply(df, 1L, function(row) {
    vals <- vapply(as.character(row), function(v) {
      paste0('"', gsub('"', '\\\\"', v, fixed = TRUE), '"')
    }, FUN.VALUE = character(1))
    paste0("  {", paste(vals, collapse = ", "), "}")
  })

  code <- paste(
    sprintf("/* Generated by mdb_parsecsv() from %s */", basename(file)),
    sprintf("const char *%s[%d][%d] = {", symbol, nrow(df), ncol(df)),
    paste(rows, collapse = ",\n"),
    "};",
    sep = "\n"
  )

  if (!is.null(output_file)) {
    writeLines(code, con = output_file, useBytes = TRUE)
    return(invisible(code))
  }
  code
}

#' Get Properties List From MDB Database
#'
#' `mdb-prop` prints a properties list from an MDB database.
#' `name` is the name of the table, query, or other object.
#' `propcol` is the name of the `MSysObjects` column containing properties and
#' defaults to `LvProp`.
#'
#' @param path Path to `.mdb`/`.accdb` file.
#' @param name Object name (`table`, `query`, etc.).
#' @param propcol Property column name. Defaults to `LvProp`.
#' @param version Logical; when `TRUE`, return mdbtools version.
#'
#' @return Character scalar containing the command output.
#' @export
mdb_prop <- function(path, name = NULL, propcol = "LvProp", version = FALSE) {
  if (isTRUE(version)) {
    return(mdb_ver())
  }
  if (is.null(name)) {
    stop("`name` is required unless `version = TRUE`.", call. = FALSE)
  }
  .native_prop_dump(
    .mdb_normalize_path(path),
    as.character(name[[1]]),
    if (!is.null(propcol) && nzchar(propcol)) as.character(propcol[[1]]) else NULL
  )
}
