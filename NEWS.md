# mdbtoolr news

## mdbtoolr 0.0.0.9000

- Added first project `README.md` with installation, DBI usage, and development workflows.
- Implemented and stabilized native vendored `mdbtools` integration for DBI table listing,
  table reads, and SQL queries.
- Added compatibility guards for iconv-related macros so builds work consistently across
  direct `src/Makevars` compilation and bundled `mdbtools` configure/build paths.
- Improved cleanup/ignore rules for generated object files in vendored `mdbtools` sources.
- Clarified DBI constructor naming: `mdb()` is the preferred constructor.
