#define MDBTOOLR_NO_STDIO_WRAP
#include "mdb_r_stdio.h"

#include <R_ext/Print.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static FILE *const MDBTOOLR_STDOUT_SENTINEL = (FILE *) (uintptr_t) 1u;
static FILE *const MDBTOOLR_STDERR_SENTINEL = (FILE *) (uintptr_t) 2u;

FILE *mdbtoolr_r_stdout(void) {
  return MDBTOOLR_STDOUT_SENTINEL;
}

FILE *mdbtoolr_r_stderr(void) {
  return MDBTOOLR_STDERR_SENTINEL;
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

  if (stream == MDBTOOLR_STDERR_SENTINEL) {
    REprintf("%s", buffer);
  } else {
    Rprintf("%s", buffer);
  }

  free(buffer);
  return needed;
}

int mdbtoolr_r_vfprintf(FILE *stream, const char *format, va_list ap) {
  if (stream == MDBTOOLR_STDOUT_SENTINEL || stream == MDBTOOLR_STDERR_SENTINEL) {
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
  if (stream == MDBTOOLR_STDOUT_SENTINEL) {
    Rprintf("%s", s);
    return 1;
  }
  if (stream == MDBTOOLR_STDERR_SENTINEL) {
    REprintf("%s", s);
    return 1;
  }
  return fputs(s, stream);
}

int mdbtoolr_r_fputc(int c, FILE *stream) {
  char ch[2];
  ch[0] = (char) c;
  ch[1] = '\0';

  if (stream == MDBTOOLR_STDOUT_SENTINEL) {
    Rprintf("%s", ch);
    return c;
  }
  if (stream == MDBTOOLR_STDERR_SENTINEL) {
    REprintf("%s", ch);
    return c;
  }
  return fputc(c, stream);
}

int mdbtoolr_r_fflush(FILE *stream) {
  if (stream == MDBTOOLR_STDOUT_SENTINEL || stream == MDBTOOLR_STDERR_SENTINEL) {
    return 0;
  }
  return fflush(stream);
}