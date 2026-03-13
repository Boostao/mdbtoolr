#ifndef MDBTOOLR_R_STDIO_H
#define MDBTOOLR_R_STDIO_H

#include <stdarg.h>
#include <stdio.h>

#ifndef MDBTOOLR_NO_STDIO_WRAP
FILE *mdbtoolr_r_stdout(void);
FILE *mdbtoolr_r_stderr(void);
int mdbtoolr_r_printf(const char *format, ...);
int mdbtoolr_r_vprintf(const char *format, va_list ap);
int mdbtoolr_r_fprintf(FILE *stream, const char *format, ...);
int mdbtoolr_r_vfprintf(FILE *stream, const char *format, va_list ap);
int mdbtoolr_r_puts(const char *s);
int mdbtoolr_r_putchar(int c);
int mdbtoolr_r_fputs(const char *s, FILE *stream);
int mdbtoolr_r_fputc(int c, FILE *stream);
int mdbtoolr_r_fflush(FILE *stream);
void mdbtoolr_r_exit(int status);

#ifdef stdout
#undef stdout
#endif
#ifdef stderr
#undef stderr
#endif

#define stdout mdbtoolr_r_stdout()
#define stderr mdbtoolr_r_stderr()

#define printf mdbtoolr_r_printf
#define vprintf mdbtoolr_r_vprintf
#define fprintf mdbtoolr_r_fprintf
#define vfprintf mdbtoolr_r_vfprintf
#define puts mdbtoolr_r_puts
#define putchar mdbtoolr_r_putchar
#define fputs mdbtoolr_r_fputs
#define fputc mdbtoolr_r_fputc
#define fflush mdbtoolr_r_fflush
#define exit mdbtoolr_r_exit
#endif

#endif