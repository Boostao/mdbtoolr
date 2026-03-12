#' Create an mdbtoolr Driver
#'
#' @return A DBI driver for '.mdb' and '.accdb' files.
#' @export
Mdb <- function() {
  methods::new("MdbDriver")
}

#' @rdname Mdb
#' @export
mdb <- Mdb

methods::setClass("MdbDriver", contains = "DBIDriver")

methods::setClass(
  "MdbConnection",
  contains = "DBIConnection",
  slots = c(
    path = "character",
    open = "logical"
  )
)

methods::setClass(
  "MdbResult",
  contains = "DBIResult",
  slots = c(
    data = "data.frame",
    position = "integer",
    completed = "logical"
  )
)

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

.mdbtools_bin_dir <- function() {
  path <- system.file("mdbtools", "bin", package = "mdbtoolr")
  if (nzchar(path) && dir.exists(path)) {
    return(path)
  }

  ""
}

.resolve_mdbtools_command <- function(command) {
  if (grepl("/", command, fixed = TRUE)) {
    return(command)
  }

  bin_dir <- .mdbtools_bin_dir()
  cmd_path <- if (nzchar(bin_dir)) file.path(bin_dir, command) else ""
  if (nzchar(cmd_path) && file.exists(cmd_path)) {
    return(cmd_path)
  }

  stop(
    "Bundled mdbtools binary not found for command: ",
    command,
    ". Reinstall mdbtoolr to trigger in-package compilation.",
    call. = FALSE
  )
}

.run_cmd <- function(command, args = character(), input = NULL) {
  executable <- .resolve_mdbtools_command(command)
  out <- system2(
    executable,
    args = shQuote(args),
    stdout = TRUE,
    stderr = TRUE,
    input = input
  )
  status <- attr(out, "status")
  if (!is.null(status) && status != 0L) {
    stop(sprintf("`%s` failed:\n%s", command, paste(out, collapse = "\n")), call. = FALSE)
  }
  out
}

.run_sql <- function(path, statement) {
  sql <- as.character(statement[[1]])
  sql_trim <- trimws(sql)
  if (!nzchar(sql_trim)) {
    stop("`statement` must not be empty.", call. = FALSE)
  }

  if (!grepl(";\\s*$", sql_trim)) {
    sql_trim <- paste0(sql_trim, ";")
  }

  out <- .run_cmd(
    "mdb-sql",
    args = c("-d", "\t", "-P", "-F", path),
    input = sql_trim
  )

  if (length(out) == 0L) {
    return(data.frame())
  }

  txt <- paste(out, collapse = "\n")
  utils::read.delim(
    text = txt,
    sep = "\t",
    header = TRUE,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    quote = "\"",
    comment.char = ""
  )
}

methods::setMethod(
  "dbCanConnect",
  "MdbDriver",
  function(drv, ...) {
    tryCatch({
      conn <- DBI::dbConnect(drv, ...)
      on.exit(DBI::dbDisconnect(conn), add = TRUE)
      TRUE
    }, error = function(e) {
      FALSE
    })
  }
)

methods::setMethod(
  "dbConnect",
  "MdbDriver",
  function(drv, dbname, ...) {
    if (missing(dbname) || !is.character(dbname) || length(dbname) != 1L) {
      stop("`dbname` must be a single '.mdb' or '.accdb' path.", call. = FALSE)
    }

    .resolve_mdbtools_command("mdb-tables")

    path <- normalizePath(dbname, mustWork = TRUE)
    methods::new("MdbConnection", path = path, open = TRUE)
  }
)

methods::setMethod(
  "dbConnect",
  "character",
  function(drv, ...) {
    if (.is_mdb_path(drv)) {
      return(DBI::dbConnect(Mdb(), dbname = drv, ...))
    }

    stop(
      "When using a character first argument, provide an '.mdb' or '.accdb' path, ",
      "or call DBI::dbConnect(Mdb(), dbname = ...).",
      call. = FALSE
    )
  }
)

methods::setMethod(
  "dbDisconnect",
  "MdbConnection",
  function(conn, ...) {
    conn@open <- FALSE
    TRUE
  }
)

methods::setMethod(
  "dbIsValid",
  "MdbConnection",
  function(dbObj, ...) {
    isTRUE(dbObj@open) && file.exists(dbObj@path)
  }
)

methods::setMethod(
  "dbListTables",
  "MdbConnection",
  function(conn, ...) {
    .require_valid_connection(conn)
    .run_cmd("mdb-tables", c("-1", conn@path))
  }
)

methods::setMethod(
  "dbExistsTable",
  c("MdbConnection", "character"),
  function(conn, name, ...) {
    .require_valid_connection(conn)
    .as_table_name(name) %in% DBI::dbListTables(conn)
  }
)

methods::setMethod(
  "dbExistsTable",
  c("MdbConnection", "Id"),
  function(conn, name, ...) {
    DBI::dbExistsTable(conn, .as_table_name(name))
  }
)

methods::setMethod(
  "dbListFields",
  c("MdbConnection", "character"),
  function(conn, name, ...) {
    .require_valid_connection(conn)
    table_name <- .as_table_name(name)
    out <- .run_cmd("mdb-export", c(conn@path, table_name))
    if (length(out) == 0L) {
      return(character())
    }

    header <- out[[1]]
    colnames(utils::read.csv(text = header, nrows = 0L, check.names = FALSE))
  }
)

methods::setMethod(
  "dbListFields",
  c("MdbConnection", "Id"),
  function(conn, name, ...) {
    DBI::dbListFields(conn, .as_table_name(name))
  }
)

methods::setMethod(
  "dbReadTable",
  c("MdbConnection", "character"),
  function(conn, name, ...) {
    .require_valid_connection(conn)
    table_name <- .as_table_name(name)
    out <- .run_cmd("mdb-export", c("-0", "__MDB_NULL__", conn@path, table_name))

    if (length(out) == 0L) {
      return(data.frame())
    }

    utils::read.csv(
      text = paste(out, collapse = "\n"),
      check.names = FALSE,
      stringsAsFactors = FALSE,
      na.strings = "__MDB_NULL__"
    )
  }
)

methods::setMethod(
  "dbReadTable",
  c("MdbConnection", "Id"),
  function(conn, name, ...) {
    DBI::dbReadTable(conn, .as_table_name(name), ...)
  }
)

methods::setMethod(
  "dbQuoteIdentifier",
  c("MdbConnection", "character"),
  function(conn, x, ...) {
    DBI::SQL(paste0("[", gsub("]", "]]", x, fixed = TRUE), "]"))
  }
)

methods::setMethod(
  "dbSendQuery",
  c("MdbConnection", "character"),
  function(conn, statement, ...) {
    .require_valid_connection(conn)
    data <- .run_sql(conn@path, statement)
    methods::new(
      "MdbResult",
      data = data,
      position = 0L,
      completed = nrow(data) == 0L
    )
  }
)

methods::setMethod(
  "dbGetQuery",
  c("MdbConnection", "character"),
  function(conn, statement, ...) {
    res <- DBI::dbSendQuery(conn, statement, ...)
    on.exit(DBI::dbClearResult(res), add = TRUE)
    DBI::dbFetch(res, n = -1)
  }
)

methods::setMethod(
  "dbExecute",
  c("MdbConnection", "character"),
  function(conn, statement, ...) {
    .require_valid_connection(conn)
    .run_cmd(
      "mdb-sql",
      args = c("-d", "\t", "-P", "-F", conn@path),
      input = statement
    )
    0L
  }
)

methods::setMethod(
  "dbFetch",
  "MdbResult",
  function(res, n = -1, ...) {
    start <- res@position + 1L
    total <- nrow(res@data)

    if (n < 0 || is.infinite(n)) {
      end <- total
    } else {
      end <- min(total, res@position + as.integer(n))
    }

    if (start > total) {
      res@completed <- TRUE
      return(res@data[0, , drop = FALSE])
    }

    chunk <- res@data[start:end, , drop = FALSE]
    res@position <- as.integer(end)
    res@completed <- end >= total
    chunk
  }
)

methods::setMethod(
  "dbHasCompleted",
  "MdbResult",
  function(res, ...) {
    isTRUE(res@completed)
  }
)

methods::setMethod(
  "dbIsValid",
  "MdbResult",
  function(dbObj, ...) {
    isTRUE(!dbObj@completed || dbObj@position <= nrow(dbObj@data))
  }
)

methods::setMethod(
  "dbClearResult",
  "MdbResult",
  function(res, ...) {
    res@completed <- TRUE
    TRUE
  }
)
