library(mdbtoolr)

sample_accdb <- testthat::test_path("mdbtestdata", "data", "ASampleDatabase.accdb")
sample_mdb <- testthat::test_path("mdbtestdata", "data", "nwind.mdb")
sample_sql <- testthat::test_path("mdbtestdata", "sql", "nwind.sql")

read_sql_statements <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  lines <- lines[!startsWith(lines, "#")]
  lines
}

test_that("driver constructor returns DBIDriver", {
  drv <- mdb()
  expect_s4_class(drv, "MdbDriver")
})

test_that("native symbols are loaded", {
  expect_true(is.loaded("mdbtoolr_list_tables"))
  expect_true(is.loaded("mdbtoolr_list_queries"))
  expect_true(is.loaded("mdbtoolr_list_fields"))
  expect_true(is.loaded("mdbtoolr_read_table"))
  expect_true(is.loaded("mdbtoolr_run_query"))
  expect_true(is.loaded("mdbtoolr_get_query_sql"))
})

test_that("character dbConnect dispatch works for accdb path", {
  skip_if_not(file.exists(sample_accdb))

  conn <- DBI::dbConnect(sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  expect_true(DBI::dbIsValid(conn))
})

test_that("basic DBI methods operate on sample accdb", {
  skip_if_not(file.exists(sample_accdb))

  conn <- DBI::dbConnect(mdb(), dbname = sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  tables <- DBI::dbListTables(conn)
  expect_true(length(tables) > 0)
  expect_true("Asset Items" %in% tables)

  target <- "Asset Items"
  expect_true(DBI::dbExistsTable(conn, target))

  fields <- DBI::dbListFields(conn, target)
  expect_true(length(fields) >= 1)

  df <- DBI::dbReadTable(conn, target)
  expect_s3_class(df, "data.frame")
})

test_that("dbReadTable applies type coercion from MDB metadata", {
  skip_if_not(file.exists(sample_mdb))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  df <- DBI::dbReadTable(conn, "Umsätze")
  expect_s3_class(df, "data.frame")

  expect_type(df$OrderID, "integer")
  expect_type(df$Freight, "double")
  expect_s3_class(df$ShippedDate, "POSIXct")
  expect_identical(attr(df$ShippedDate, "tzone"), "UTC")

  formatted <- format(df$ShippedDate, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  expect_true(all(is.na(df$ShippedDate) | grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$", formatted)))
})

test_that("query roundtrip works", {
  skip_if_not(file.exists(sample_mdb))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  tables <- DBI::dbListTables(conn)
  expect_true("Umsätze" %in% tables)

  out <- DBI::dbGetQuery(conn, "SELECT * FROM [Umsätze] LIMIT 2;")
  expect_s3_class(out, "data.frame")
  expect_lte(nrow(out), 2)
  expect_true("OrderID" %in% names(out))
})

test_that("dbGetQuery applies type coercion from MDB metadata", {
  skip_if_not(file.exists(sample_mdb))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  out <- DBI::dbGetQuery(
    conn,
    "SELECT [OrderID], [Freight], [ShippedDate] FROM [Umsätze] LIMIT 5;"
  )

  expect_type(out$OrderID, "integer")
  expect_type(out$Freight, "double")
  expect_s3_class(out$ShippedDate, "POSIXct")
  expect_identical(attr(out$ShippedDate, "tzone"), "UTC")

  formatted <- format(out$ShippedDate, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  expect_true(all(is.na(out$ShippedDate) | grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$", formatted)))
})

test_that("dbGetQuery does not execute saved Access query names yet", {
  skip_if_not(file.exists(sample_mdb))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  expect_error(
    DBI::dbGetQuery(conn, "SELECT * FROM [Current Product List] LIMIT 3;"),
    regexp = "not a table|Got no result"
  )
})

test_that("mdb_queries lists saved queries and extracts SQL", {
  skip_if_not(file.exists(sample_mdb))

  queries <- mdb_queries(sample_mdb)
  expect_true("Current Product List" %in% queries)

  sql <- mdb_queries(sample_mdb, query = "Current Product List")
  expect_type(sql, "character")
  expect_true(grepl("^SELECT", sql, ignore.case = TRUE))
})

test_that("dbListObjects returns DBI-shaped data frame with tables and queries", {
  skip_if_not(file.exists(sample_mdb))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  objs <- DBI::dbListObjects(conn)
  expect_s3_class(objs, "data.frame")
  expect_identical(names(objs)[1:2], c("table", "is_prefix"))
  expect_true(is.list(objs$table))
  expect_type(objs$is_prefix, "logical")
  expect_true(all(!objs$is_prefix))

  obj_names <- vapply(objs$table, as.character, FUN.VALUE = character(1))
  expect_true("Products" %in% obj_names)
  expect_true("Current Product List" %in% obj_names)
})

test_that("mdb_ver and mdb_schema work without system CLI", {
  skip_if_not(file.exists(sample_mdb))

  ver <- mdb_ver()
  expect_type(ver, "character")
  expect_gt(nchar(ver), 0)

  file_ver <- mdb_ver(sample_mdb)
  expect_identical(file_ver, "JET3")

  ddl <- mdb_schema(sample_mdb, table = "Products", backend = "postgres")
  expect_type(ddl, "character")
  expect_true(grepl("CREATE TABLE", ddl, fixed = TRUE))
})

test_that("mdb-tables and mdb-queries option mimics behave", {
  skip_if_not(file.exists(sample_mdb))

  table_text <- mdb_tables(sample_mdb, single_column = TRUE, as_text = TRUE)
  expect_type(table_text, "character")
  expect_true(grepl("\n", table_text) || nzchar(table_text))

  query_names <- mdb_queries(sample_mdb, list = TRUE)
  expect_type(query_names, "character")

  query_text <- mdb_queries(sample_mdb, list = TRUE, newline = TRUE, as_text = TRUE)
  expect_type(query_text, "character")
})

test_that("mdb-export and mdb-sql option mimics return text output", {
  skip_if_not(file.exists(sample_mdb))

  sql_text <- mdb_sql(
    sample_mdb,
    "SELECT [OrderID], [Freight] FROM [Umsätze] LIMIT 2;",
    as_text = TRUE,
    no_pretty_print = TRUE,
    no_footer = TRUE,
    delimiter = "|"
  )
  expect_type(sql_text, "character")
  expect_true(grepl("\\|", sql_text) || nzchar(sql_text))

  export_text <- mdb_export(
    sample_mdb,
    "Umsätze",
    no_header = TRUE,
    delimiter = ";",
    row_delimiter = "\n",
    no_quote = TRUE,
    n = 2
  )
  expect_type(export_text, "character")
  expect_true(nchar(export_text) > 0)
})

test_that("mdb_count fallback preserves WHERE and matches projected-row counts", {
  skip_if_not(file.exists(sample_mdb))

  total <- mdb_count(sample_mdb, "Umsätze")
  filtered <- mdb_count(sample_mdb, "Umsätze", where = "[OrderID] > 11000")

  projected_total <- nrow(mdb_sql(sample_mdb, "SELECT [OrderID] FROM [Umsätze]"))
  projected_filtered <- nrow(mdb_sql(sample_mdb, "SELECT [OrderID] FROM [Umsätze] WHERE [OrderID] > 11000"))

  expect_identical(total, as.integer(projected_total))
  expect_identical(filtered, as.integer(projected_filtered))
  expect_lt(filtered, total)
})

test_that("mdb_count without WHERE uses metadata row count semantics", {
  skip_if_not(file.exists(sample_mdb))

  count <- mdb_count(sample_mdb, "Umsätze")
  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)
  table_rows <- DBI::dbReadTable(conn, "Umsätze")
  expect_identical(count, as.integer(nrow(table_rows)))
})

test_that("test_script.sh command set is covered by mimic wrappers", {
  skip_if_not(file.exists(sample_accdb))
  skip_if_not(file.exists(sample_mdb))

  json_accdb <- mdb_json(sample_accdb, "Asset Items", n = 3)
  expect_type(json_accdb, "character")
  expect_true(grepl("\\[|\\{", json_accdb))

  json_mdb <- mdb_json(sample_mdb, "Umsätze", n = 3)
  expect_type(json_mdb, "character")
  expect_true(grepl("\\[|\\{", json_mdb))

  count_accdb <- mdb_count(sample_accdb, "Asset Items")
  expect_type(count_accdb, "integer")
  expect_gte(count_accdb, 0L)

  count_mdb <- mdb_count(sample_mdb, "Umsätze")
  expect_type(count_mdb, "integer")
  expect_gte(count_mdb, 0L)

  prop_accdb <- mdb_prop(sample_accdb, name = "Asset Items")
  expect_type(prop_accdb, "character")
  expect_true(nchar(prop_accdb) > 0)

  prop_mdb <- mdb_prop(sample_mdb, name = "Umsätze")
  expect_type(prop_mdb, "character")
  expect_true(nchar(prop_mdb) > 0)

  schema_accdb <- mdb_schema(sample_accdb)
  expect_type(schema_accdb, "character")
  expect_true(grepl("CREATE TABLE", schema_accdb, fixed = TRUE))

  schema_mdb <- mdb_schema(sample_mdb)
  expect_type(schema_mdb, "character")
  expect_true(grepl("CREATE TABLE", schema_mdb, fixed = TRUE))

  tables_accdb <- mdb_tables(sample_accdb)
  expect_type(tables_accdb, "character")
  expect_true("Asset Items" %in% tables_accdb)

  tables_mdb <- mdb_tables(sample_mdb)
  expect_type(tables_mdb, "character")
  expect_true("Umsätze" %in% tables_mdb)

  ver_accdb <- mdb_ver(sample_accdb)
  ver_mdb <- mdb_ver(sample_mdb)
  expect_true(ver_accdb %in% c("JET4", "ACE12", "ACE14", "ACE15", "ACE16"))
  expect_identical(ver_mdb, "JET3")

  queries_accdb <- mdb_queries(sample_accdb)
  expect_type(queries_accdb, "character")
  if (!"qryCostsSummedByOwner" %in% queries_accdb) {
    testthat::skip("Expected query 'qryCostsSummedByOwner' not present in sample accdb.")
  }
  query_sql <- mdb_queries(sample_accdb, query = "qryCostsSummedByOwner")
  expect_type(query_sql, "character")
  expect_true(grepl("^SELECT", query_sql, ignore.case = TRUE))
})

test_that("test_sql.sh is covered by mdb_sql input mode", {
  skip_if_not(file.exists(sample_mdb))
  skip_if_not(file.exists(sample_sql))

  sql_text <- mdb_sql(
    path = sample_mdb,
    input = sample_sql,
    as_text = TRUE,
    no_pretty_print = TRUE,
    no_footer = TRUE
  )

  expect_type(sql_text, "character")
  expect_true(nchar(sql_text) > 0)
})

test_that("test_sql script is replicated in DBI context", {
  skip_if_not(file.exists(sample_mdb))
  skip_if_not(file.exists(sample_sql))

  conn <- DBI::dbConnect(sample_mdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  statements <- read_sql_statements(sample_sql)
  expect_true(length(statements) >= 1)

  out1 <- DBI::dbGetQuery(conn, statements[[1]])
  expect_s3_class(out1, "data.frame")
  expect_lte(nrow(out1), 10)
  expect_true("CustomerID" %in% names(out1))

  out2 <- DBI::dbGetQuery(conn, statements[[2]])
  expect_s3_class(out2, "data.frame")
  expect_true("City" %in% names(out2))
  expect_true(all(out2$City == "Helsinki"))

  out3 <- DBI::dbGetQuery(conn, statements[[3]])
  expect_s3_class(out3, "data.frame")
  expect_true("CompanyName" %in% names(out3))
  expect_gt(nrow(out3), 0)
})
