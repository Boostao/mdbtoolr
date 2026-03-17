# mdbtoolr news

## mdbtoolr 0.0.0.9900

- Expanded DBI coverage and behavior, including `dbListObjects()` output aligned
  with DBI expectations and improved typed result coercion.
- Added mdbtools-style helper functions for listing tables/queries, schema
  rendering, JSON export, properties, and SQL workflows.
- Removed runtime reliance on external system `mdbtools` CLI commands for core
  package operations by using compiled vendored library code paths.
- Improved vendored source refresh workflow with support for local patch files
  (including PR #398 deleted-column compatibility patch).

## mdbtoolr 0.0.0.9000

- Added first project `README.md` with installation, DBI usage, and development workflows.
- Implemented and stabilized native vendored `mdbtools` integration for DBI table listing,
  table reads, and SQL queries.
- Added compatibility guards for iconv-related macros so builds work consistently across
  direct `src/Makevars` compilation and bundled `mdbtools` configure/build paths.
- Improved cleanup/ignore rules for generated object files in vendored `mdbtools` sources.
- Clarified DBI constructor naming: `mdb()` is the preferred constructor.
- Add support for macOS
