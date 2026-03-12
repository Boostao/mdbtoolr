pkgname <- "mdbtoolr"
source(file.path(R.home("share"), "R", "examples-header.R"))
options(warn = 1)
library('mdbtoolr')

base::assign(".oldSearch", base::search(), pos = 'CheckExEnv')
base::assign(".old_wd", base::getwd(), pos = 'CheckExEnv')
cleanEx()
nameEx("Mdb")
### * Mdb

flush(stderr()); flush(stdout())

### Name: Mdb
### Title: Create an mdbtoolr Driver
### Aliases: Mdb mdb

### ** Examples

if (nzchar(Sys.which("mdb-tables"))) {
  drv <- Mdb()
}



### * <FOOTER>
###
cleanEx()
options(digits = 7L)
base::cat("Time elapsed: ", proc.time() - base::get("ptime", pos = 'CheckExEnv'),"\n")
grDevices::dev.off()
###
### Local variables: ***
### mode: outline-minor ***
### outline-regexp: "\\(> \\)?### [*]+" ***
### End: ***
quit('no')
