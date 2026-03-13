#ifndef MDBTOOLR_R_STDIO_H
#define MDBTOOLR_R_STDIO_H

#include <stdarg.h>
#include <stdio.h>

#ifndef MDBTOOLR_NO_STDIO_WRAP
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

#define printf(...) mdbtoolr_r_printf(__VA_ARGS__)
#define vprintf(...) mdbtoolr_r_vprintf(__VA_ARGS__)
#define fprintf(...) mdbtoolr_r_fprintf(__VA_ARGS__)
#define vfprintf(...) mdbtoolr_r_vfprintf(__VA_ARGS__)
#define puts(...) mdbtoolr_r_puts(__VA_ARGS__)
#define putchar(...) mdbtoolr_r_putchar(__VA_ARGS__)
#define fputs(...) mdbtoolr_r_fputs(__VA_ARGS__)
#define fputc(...) mdbtoolr_r_fputc(__VA_ARGS__)
#define fflush(...) mdbtoolr_r_fflush(__VA_ARGS__)
#define exit(...) mdbtoolr_r_exit(__VA_ARGS__)
#endif

#endif