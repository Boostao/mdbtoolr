# mdbtoolr

`mdbtoolr` is an R package that provides a DBI backend for reading Microsoft Access
`.mdb` and `.accdb` files.

The package vendors and compiles `mdbtools` C sources as part of installation, so it does
not require a separate system installation of `mdbtools` for core read/query operations.

## Features

- DBI driver constructor: `mdb()`
- Connect with `DBI::dbConnect()` using either:
  - `DBI::dbConnect(mdb(), dbname = "path/to/file.accdb")`
  - `DBI::dbConnect("path/to/file.accdb")`
- List and inspect schema:
  - `DBI::dbListTables()`
  - `DBI::dbListFields()`
  - `DBI::dbExistsTable()`
- Read/query data:
  - `DBI::dbReadTable()`
  - `DBI::dbGetQuery()`

## Installation

From a local checkout:

```r
install.packages("DBI")
install.packages(".", repos = NULL, type = "source")
```

For development installs:

```r
install.packages("devtools")
devtools::install_local(".", force = TRUE)
```

## Quick Start

```r
library(DBI)
library(mdbtoolr)

con <- dbConnect(mdb(), dbname = "path/to/database.accdb")
on.exit(dbDisconnect(con), add = TRUE)

# List tables
tables <- dbListTables(con)
print(head(tables))

# Run a SQL query
out <- dbGetQuery(con, "SELECT * FROM [Some Table] LIMIT 10;")
print(out)
```

You can also connect directly with a path:

```r
con <- dbConnect("path/to/database.mdb")
on.exit(dbDisconnect(con), add = TRUE)
```

## Notes

- The backend is read-only. `dbExecute()` is not supported.
- `mdb()` is the preferred constructor name.
- Table names with spaces or special characters should be SQL-quoted in queries,
  for example `[My Table]`.

## Development

Build and check:

```sh
R CMD build .
R CMD check mdbtoolr_0.0.0.9000.tar.gz --as-cran
```

Run tests:

```sh
Rscript -e 'testthat::test_dir("tests/testthat")'
```
