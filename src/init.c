#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

SEXP mdbtoolr_list_tables(SEXP path_sexp);
SEXP mdbtoolr_list_queries(SEXP path_sexp);
SEXP mdbtoolr_list_fields(SEXP path_sexp, SEXP table_sexp);
SEXP mdbtoolr_table_num_rows(SEXP path_sexp, SEXP table_sexp);
SEXP mdbtoolr_read_table(SEXP path_sexp, SEXP table_sexp);
SEXP mdbtoolr_run_query(SEXP path_sexp, SEXP statement_sexp);
SEXP mdbtoolr_get_query_sql(SEXP path_sexp, SEXP query_name_sexp);
SEXP mdbtoolr_print_schema(SEXP path_sexp, SEXP table_sexp, SEXP backend_sexp, SEXP namespace_sexp, SEXP options_sexp);
SEXP mdbtoolr_version(void);
SEXP mdbtoolr_file_format(SEXP path_sexp);
SEXP mdbtoolr_prop_dump(SEXP path_sexp, SEXP name_sexp, SEXP propcol_sexp);

static const R_CallMethodDef call_methods[] = {
  {"mdbtoolr_list_tables", (DL_FUNC) &mdbtoolr_list_tables, 1},
  {"mdbtoolr_list_queries", (DL_FUNC) &mdbtoolr_list_queries, 1},
  {"mdbtoolr_list_fields", (DL_FUNC) &mdbtoolr_list_fields, 2},
  {"mdbtoolr_table_num_rows", (DL_FUNC) &mdbtoolr_table_num_rows, 2},
  {"mdbtoolr_read_table", (DL_FUNC) &mdbtoolr_read_table, 2},
  {"mdbtoolr_run_query", (DL_FUNC) &mdbtoolr_run_query, 2},
  {"mdbtoolr_get_query_sql", (DL_FUNC) &mdbtoolr_get_query_sql, 2},
  {"mdbtoolr_print_schema", (DL_FUNC) &mdbtoolr_print_schema, 5},
  {"mdbtoolr_version", (DL_FUNC) &mdbtoolr_version, 0},
  {"mdbtoolr_file_format", (DL_FUNC) &mdbtoolr_file_format, 1},
  {"mdbtoolr_prop_dump", (DL_FUNC) &mdbtoolr_prop_dump, 3},
  {NULL, NULL, 0}
};

void R_init_mdbtoolr(DllInfo *dll) {
  R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
