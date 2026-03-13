#define MDBTOOLR_NO_STDIO_WRAP
#include "mdb_r_stdio.h"

#include <R_ext/Print.h>
#include <R_ext/Error.h>

#include <stdlib.h>
#include <string.h>

static int mdbtoolr_r_printv(FILE *stream, const char *format, va_list ap);

int mdbtoolr_r_vprintf(const char *format, va_list ap) {
  return mdbtoolr_r_printv(stdout, format, ap);
}

int mdbtoolr_r_printf(const char *format, ...) {
  int out;
  va_list ap;

  va_start(ap, format);
  out = mdbtoolr_r_vprintf(format, ap);
  va_end(ap);
  return out;
}

static int mdbtoolr_r_printv(FILE *stream, const char *format, va_list ap) {
  int needed;
  va_list ap_copy;
  char *buffer;

  va_copy(ap_copy, ap);
  needed = vsnprintf(NULL, 0, format, ap_copy);
  va_end(ap_copy);

  if (needed < 0) {
    return needed;
  }

  buffer = (char *) malloc((size_t) needed + 1u);
  if (buffer == NULL) {
    return -1;
  }

  va_copy(ap_copy, ap);
  (void) vsnprintf(buffer, (size_t) needed + 1u, format, ap_copy);
  va_end(ap_copy);

  if (stream == stderr) {
    REprintf("%s", buffer);
  } else {
    Rprintf("%s", buffer);
  }

  free(buffer);
  return needed;
}

int mdbtoolr_r_vfprintf(FILE *stream, const char *format, va_list ap) {
  if (stream == stdout || stream == stderr) {
    return mdbtoolr_r_printv(stream, format, ap);
  }
  return vfprintf(stream, format, ap);
}

int mdbtoolr_r_fprintf(FILE *stream, const char *format, ...) {
  int out;
  va_list ap;

  va_start(ap, format);
  out = mdbtoolr_r_vfprintf(stream, format, ap);
  va_end(ap);
  return out;
}

int mdbtoolr_r_fputs(const char *s, FILE *stream) {
  if (stream == stdout) {
    Rprintf("%s", s);
    return 1;
  }
  if (stream == stderr) {
    REprintf("%s", s);
    return 1;
  }
  return fputs(s, stream);
}

int mdbtoolr_r_puts(const char *s) {
  Rprintf("%s\n", s);
  return 1;
}

int mdbtoolr_r_fputc(int c, FILE *stream) {
  char ch[2];
  ch[0] = (char) c;
  ch[1] = '\0';

  if (stream == stdout) {
    Rprintf("%s", ch);
    return c;
  }
  if (stream == stderr) {
    REprintf("%s", ch);
    return c;
  }
  return fputc(c, stream);
}

int mdbtoolr_r_putchar(int c) {
  char ch[2];
  ch[0] = (char) c;
  ch[1] = '\0';
  Rprintf("%s", ch);
  return c;
}

int mdbtoolr_r_fflush(FILE *stream) {
  if (stream == stdout || stream == stderr) {
    return 0;
  }
  return fflush(stream);
}

void mdbtoolr_r_exit(int status) {
  Rf_error("mdbtools attempted to terminate the R process (exit status %d)", status);
}