library(mdbtoolr)

sample_accdb <- testthat::test_path("accdb", "VPro64.accdb")

test_that("driver constructor returns DBIDriver", {
  drv <- Mdb()
  expect_s4_class(drv, "MdbDriver")
})

test_that("bundled mdbtools binaries are present", {
  bin_dir <- system.file("mdbtools", "bin", package = "mdbtoolr")
  expect_true(nzchar(bin_dir))
  expect_true(file.exists(file.path(bin_dir, "mdb-tables")))
  expect_true(file.exists(file.path(bin_dir, "mdb-sql")))
  expect_true(file.exists(file.path(bin_dir, "mdb-export")))
})

test_that("character dbConnect dispatch works for accdb path", {
  skip_if_not(file.exists(sample_accdb))

  conn <- DBI::dbConnect(sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  expect_true(DBI::dbIsValid(conn))
})

test_that("basic DBI methods operate on sample accdb", {
  skip_if_not(file.exists(sample_accdb))
  skip_if_not(nzchar(Sys.which("mdb-tables")))

  conn <- DBI::dbConnect(Mdb(), dbname = sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  tables <- DBI::dbListTables(conn)
  expect_true(length(tables) > 0)

  target <- tables[[1]]
  expect_true(DBI::dbExistsTable(conn, target))

  fields <- DBI::dbListFields(conn, target)
  expect_true(length(fields) >= 1)

  df <- DBI::dbReadTable(conn, target)
  expect_s3_class(df, "data.frame")
})

test_that("query roundtrip works", {
  skip_if_not(file.exists(sample_accdb))
  skip_if_not(nzchar(Sys.which("mdb-sql")))

  conn <- DBI::dbConnect(sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  out <- DBI::dbGetQuery(conn, "select * from LayerCode limit 2;")
  expect_s3_class(out, "data.frame")
  expect_lte(nrow(out), 2)
})

test_that("query without semicolon returns rows", {
  skip_if_not(file.exists(sample_accdb))

  conn <- DBI::dbConnect(sample_accdb)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  out <- DBI::dbGetQuery(conn, "SELECT * FROM Sample_Humus")
  expect_s3_class(out, "data.frame")
  expect_gt(nrow(out), 0)
  expect_true("PlotNumber" %in% names(out))
})
