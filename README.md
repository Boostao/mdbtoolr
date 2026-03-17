# mdbtoolr

`mdbtoolr` provides a read-only DBI backend for Microsoft Access `.mdb` and
`.accdb` files, powered by vendored native `mdbtools` sources compiled into the
package.

## Install

```r
install.packages("remotes")
remotes::install_github("boostao/mdbtoolr")
```

## Quick Start

```r
library(DBI)
library(mdbtoolr)

# 1) Prefer your own Access file path.
db_path <- "path/to/your/database.accdb"

# 2) Optional: use bundled test fixture when available.
fixture <- system.file("tests", "testthat", "mdbtestdata", "data", "nwind.mdb", package = "mdbtoolr")
if (nzchar(fixture)) {
  db_path <- fixture
}

if (!file.exists(db_path)) {
  stop("Set `db_path` to a valid .mdb/.accdb file.")
}

con <- dbConnect(mdb(), dbname = db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbListTables(con)
dbListFields(con, "Products")

products <- dbReadTable(con, "Products")
head(products)

top_products <- dbGetQuery(
  con,
  "SELECT [ProductID], [ProductName] FROM [Products] LIMIT 5;"
)
top_products
```

## Helper Functions

`mdbtoolr` also includes mdbtools-style helpers such as:

- `mdb_tables()`
- `mdb_queries()`
- `mdb_schema()`
- `mdb_sql()`
- `mdb_export()`
- `mdb_json()`
- `mdb_count()`
- `mdb_prop()`

## Notes

- Runtime is read-only. `dbExecute()` is intentionally unsupported.
- Core package features do not require the system `mdbtools` CLI binaries.
- Quote table names that contain spaces or symbols, for example `[Order Details]`.
