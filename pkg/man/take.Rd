\docType{methods}
\name{take}
\alias{take}
\alias{take,RRDD,numeric-method}
\title{Take elements from an RDD.}
\usage{
take(rrdd, num)

\S4method{take}{RRDD,numeric}(rrdd, num)
}
\arguments{
  \item{rrdd}{The RRDD to take elements from}

  \item{num}{Number of elements to take}
}
\description{
This function takes the first NUM elements in the RRDD and
returns them in a list.
}
\examples{
\dontrun{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
take(rdd, 2L) # list(1, 2)
}
}

