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
  expect_true(is.loaded("mdbtoolr_list_fields"))
  expect_true(is.loaded("mdbtoolr_read_table"))
  expect_true(is.loaded("mdbtoolr_run_query"))
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
