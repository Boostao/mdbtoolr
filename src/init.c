#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

SEXP mdbtoolr_list_tables(SEXP path_sexp);
SEXP mdbtoolr_list_fields(SEXP path_sexp, SEXP table_sexp);
SEXP mdbtoolr_read_table(SEXP path_sexp, SEXP table_sexp);
SEXP mdbtoolr_run_query(SEXP path_sexp, SEXP statement_sexp);

static const R_CallMethodDef call_methods[] = {
  {"mdbtoolr_list_tables", (DL_FUNC) &mdbtoolr_list_tables, 1},
  {"mdbtoolr_list_fields", (DL_FUNC) &mdbtoolr_list_fields, 2},
  {"mdbtoolr_read_table", (DL_FUNC) &mdbtoolr_read_table, 2},
  {"mdbtoolr_run_query", (DL_FUNC) &mdbtoolr_run_query, 2},
  {NULL, NULL, 0}
};

void R_init_mdbtoolr(DllInfo *dll) {
  R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
