\name{Mdb}
\alias{Mdb}
\alias{mdb}
\title{Create an mdbtoolr Driver}
\description{
Create a DBI driver backed by command line utilities from
\code{mdbtools}.
}
\usage{
Mdb()

mdb()
}
\value{
An object of class \code{MdbDriver}.
}
\examples{
if (nzchar(Sys.which("mdb-tables"))) {
  drv <- Mdb()
}
}
