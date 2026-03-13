#include <R.h>
#include <Rinternals.h>

#include <stdlib.h>
#include <string.h>

#include "mdbtools.h"
#include "mdbsql.h"

static const char *scalar_char(SEXP x, const char *arg_name) {
  if (TYPEOF(x) != STRSXP || XLENGTH(x) != 1 || STRING_ELT(x, 0) == NA_STRING) {
    Rf_error("`%s` must be a single non-NA string.", arg_name);
  }
  return CHAR(STRING_ELT(x, 0));
}

static void free_bind_buffers(char **values, int *lens, int ncol) {
  int i;
  if (values != NULL) {
    for (i = 0; i < ncol; i++) {
      free(values[i]);
    }
    free(values);
  }
  free(lens);
}

SEXP mdbtoolr_list_tables(SEXP path_sexp) {
  const char *path = scalar_char(path_sexp, "path");
  MdbHandle *mdb = NULL;
  GPtrArray *catalog = NULL;
  SEXP out = R_NilValue;
  int i;

  mdb = mdb_open(path, MDB_NOFLAGS);
  if (mdb == NULL) {
    Rf_error("Failed to open MDB/ACCDB file: %s", path);
  }

  catalog = mdb_read_catalog(mdb, MDB_TABLE);
  if (catalog == NULL) {
    out = PROTECT(Rf_allocVector(STRSXP, 0));
    mdb_close(mdb);
    UNPROTECT(1);
    return out;
  }

  out = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) catalog->len));
  for (i = 0; i < (int) catalog->len; i++) {
    MdbCatalogEntry *entry = (MdbCatalogEntry *) g_ptr_array_index(catalog, i);
    SET_STRING_ELT(out, i, Rf_mkChar(entry->object_name));
  }

  mdb_close(mdb);
  UNPROTECT(1);
  return out;
}

SEXP mdbtoolr_list_fields(SEXP path_sexp, SEXP table_sexp) {
  const char *path = scalar_char(path_sexp, "path");
  const char *table_name = scalar_char(table_sexp, "table");
  MdbHandle *mdb = NULL;
  MdbTableDef *table = NULL;
  SEXP out = R_NilValue;
  int i;

  mdb = mdb_open(path, MDB_NOFLAGS);
  if (mdb == NULL) {
    Rf_error("Failed to open MDB/ACCDB file: %s", path);
  }

  table = mdb_read_table_by_name(mdb, (char *) table_name, MDB_TABLE);
  if (table == NULL) {
    mdb_close(mdb);
    Rf_error("Table not found: %s", table_name);
  }

  if (mdb_read_columns(table) == NULL) {
    mdb_free_tabledef(table);
    mdb_close(mdb);
    Rf_error("Failed to read columns for table: %s", table_name);
  }

  out = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) table->num_cols));
  for (i = 0; i < (int) table->num_cols; i++) {
    MdbColumn *col = (MdbColumn *) g_ptr_array_index(table->columns, i);
    SET_STRING_ELT(out, i, Rf_mkChar(col->name));
  }

  mdb_free_tabledef(table);
  mdb_close(mdb);
  UNPROTECT(1);
  return out;
}

SEXP mdbtoolr_read_table(SEXP path_sexp, SEXP table_sexp) {
  const char *path = scalar_char(path_sexp, "path");
  const char *table_name = scalar_char(table_sexp, "table");
  const size_t bind_size = 65536;
  MdbHandle *mdb = NULL;
  MdbTableDef *table = NULL;
  char **bound_values = NULL;
  int *bound_lens = NULL;
  SEXP out = R_NilValue;
  SEXP names = R_NilValue;
  int ncol;
  int i;
  int row;
  int nrow;

  mdb = mdb_open(path, MDB_NOFLAGS);
  if (mdb == NULL) {
    Rf_error("Failed to open MDB/ACCDB file: %s", path);
  }

  mdb_set_bind_size(mdb, bind_size);
  table = mdb_read_table_by_name(mdb, (char *) table_name, MDB_TABLE);
  if (table == NULL) {
    mdb_close(mdb);
    Rf_error("Table not found: %s", table_name);
  }

  if (mdb_read_columns(table) == NULL) {
    mdb_free_tabledef(table);
    mdb_close(mdb);
    Rf_error("Failed to read columns for table: %s", table_name);
  }

  ncol = (int) table->num_cols;
  bound_values = (char **) calloc((size_t) ncol, sizeof(char *));
  bound_lens = (int *) calloc((size_t) ncol, sizeof(int));
  if (bound_values == NULL || bound_lens == NULL) {
    free_bind_buffers(bound_values, bound_lens, ncol);
    mdb_free_tabledef(table);
    mdb_close(mdb);
    Rf_error("Out of memory while allocating bound column buffers.");
  }

  for (i = 0; i < ncol; i++) {
    int ret;
    bound_values[i] = (char *) calloc(bind_size, sizeof(char));
    if (bound_values[i] == NULL) {
      free_bind_buffers(bound_values, bound_lens, ncol);
      mdb_free_tabledef(table);
      mdb_close(mdb);
      Rf_error("Out of memory while allocating a column buffer.");
    }

    ret = mdb_bind_column(table, i + 1, bound_values[i], &bound_lens[i]);
    if (ret == -1) {
      free_bind_buffers(bound_values, bound_lens, ncol);
      mdb_free_tabledef(table);
      mdb_close(mdb);
      Rf_error("Failed to bind column %d in table '%s'.", i + 1, table_name);
    }
  }

  if (mdb_rewind_table(table) == -1) {
    free_bind_buffers(bound_values, bound_lens, ncol);
    mdb_free_tabledef(table);
    mdb_close(mdb);
    Rf_error("Failed to rewind table '%s'.", table_name);
  }

  nrow = 0;
  while (mdb_fetch_row(table)) {
    nrow++;
  }

  if (mdb_rewind_table(table) == -1) {
    free_bind_buffers(bound_values, bound_lens, ncol);
    mdb_free_tabledef(table);
    mdb_close(mdb);
    Rf_error("Failed to rewind table '%s' for second pass.", table_name);
  }

  out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) ncol));
  names = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) ncol));

  for (i = 0; i < ncol; i++) {
    MdbColumn *col = (MdbColumn *) g_ptr_array_index(table->columns, i);
    SEXP col_vec = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) nrow));
    SET_VECTOR_ELT(out, i, col_vec);
    UNPROTECT(1);
    SET_STRING_ELT(names, i, Rf_mkChar(col->name));
  }

  row = 0;
  while (mdb_fetch_row(table)) {
    for (i = 0; i < ncol; i++) {
      SEXP col_vec = VECTOR_ELT(out, i);
      if (bound_lens[i] == 0) {
        SET_STRING_ELT(col_vec, row, NA_STRING);
      } else {
        SET_STRING_ELT(col_vec, row, Rf_mkChar(bound_values[i]));
      }
    }
    row++;
  }

  Rf_setAttrib(out, R_NamesSymbol, names);

  free_bind_buffers(bound_values, bound_lens, ncol);
  mdb_free_tabledef(table);
  mdb_close(mdb);
  UNPROTECT(2);
  return out;
}

SEXP mdbtoolr_run_query(SEXP path_sexp, SEXP statement_sexp) {
  const char *path = scalar_char(path_sexp, "path");
  const char *statement = scalar_char(statement_sexp, "statement");
  const size_t bind_size = 65536;
  MdbSQL *sql = NULL;
  char *query = NULL;
  size_t query_len;
  SEXP out = R_NilValue;
  SEXP names = R_NilValue;
  int ncol;
  int nrow;
  int i;
  int row;

  sql = mdb_sql_init();
  if (sql == NULL) {
    Rf_error("Failed to initialize SQL engine.");
  }

  query = strdup(statement);
  if (query == NULL) {
    mdb_sql_exit(sql);
    Rf_error("Out of memory while preparing SQL statement.");
  }

  query_len = strlen(query);
  while (query_len > 0 && (query[query_len - 1] == ' ' || query[query_len - 1] == '\t' ||
         query[query_len - 1] == '\r' || query[query_len - 1] == '\n')) {
    query[query_len - 1] = '\0';
    query_len--;
  }
  if (query_len > 0 && query[query_len - 1] == ';') {
    query[query_len - 1] = '\0';
    query_len--;
  }
  while (query_len > 0 && (query[query_len - 1] == ' ' || query[query_len - 1] == '\t' ||
         query[query_len - 1] == '\r' || query[query_len - 1] == '\n')) {
    query[query_len - 1] = '\0';
    query_len--;
  }

  if (query_len == 0) {
    free(query);
    mdb_sql_exit(sql);
    Rf_error("`statement` must not be empty.");
  }

  if (mdb_sql_open(sql, (char *) path) == NULL) {
    const char *err = mdb_sql_last_error(sql);
    char msg[1024];
    snprintf(msg, sizeof(msg), "Failed to open MDB/ACCDB file: %s", path);
    if (err != NULL && err[0] != '\0') {
      snprintf(msg, sizeof(msg), "%s", err);
    }
    mdb_sql_exit(sql);
    Rf_error("%s", msg);
  }

  mdb_set_bind_size(sql->mdb, bind_size);

  if (mdb_sql_run_query(sql, query) == NULL || mdb_sql_has_error(sql)) {
    const char *err = mdb_sql_last_error(sql);
    if (err == NULL || err[0] == '\0') {
      err = "Failed to execute SQL query.";
    }
    free(query);
    mdb_sql_exit(sql);
    Rf_error("%s", err);
  }

  free(query);

  ncol = (int) sql->num_columns;
  if (ncol == 0) {
    mdb_sql_exit(sql);
    return Rf_allocVector(VECSXP, 0);
  }

  nrow = 0;
  while (mdb_sql_fetch_row(sql, sql->cur_table)) {
    nrow++;
  }

  if (mdb_rewind_table(sql->cur_table) == -1) {
    mdb_sql_exit(sql);
    Rf_error("Failed to rewind SQL result table.");
  }
  sql->row_count = 0;

  out = PROTECT(Rf_allocVector(VECSXP, (R_xlen_t) ncol));
  names = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) ncol));

  for (i = 0; i < ncol; i++) {
    MdbSQLColumn *sql_col = (MdbSQLColumn *) g_ptr_array_index(sql->columns, i);
    SEXP col_vec = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t) nrow));
    SET_VECTOR_ELT(out, i, col_vec);
    UNPROTECT(1);
    SET_STRING_ELT(names, i, Rf_mkChar(sql_col->name));
  }

  row = 0;
  while (mdb_sql_fetch_row(sql, sql->cur_table)) {
    for (i = 0; i < ncol; i++) {
      SEXP col_vec = VECTOR_ELT(out, i);
      const char *value = (const char *) g_ptr_array_index(sql->bound_values, i);
      if (value == NULL) {
        SET_STRING_ELT(col_vec, row, NA_STRING);
      } else {
        SET_STRING_ELT(col_vec, row, Rf_mkChar(value));
      }
    }
    row++;
  }

  Rf_setAttrib(out, R_NamesSymbol, names);
  mdb_sql_exit(sql);
  UNPROTECT(2);
  return out;
}
