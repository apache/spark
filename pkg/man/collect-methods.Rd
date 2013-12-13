\docType{methods}
\name{collect}
\alias{collect}
\alias{collect,RRDD-method}
\alias{collectPartition}
\alias{collectPartition,RRDD,integer-method}
\alias{collectPartition,integer,RRDD-method}
\title{Collect elements of an RDD}
\usage{
collect(rrdd, ...)

\S4method{collect}{RRDD}(rrdd, flatten = TRUE)

collectPartition(rrdd, partitionId)

\S4method{collectPartition}{RRDD,integer}(rrdd, partitionId)
}
\arguments{
  \item{rrdd}{The RRDD to collect}

  \item{...}{Other optional arguments to collect}

  \item{flatten}{FALSE if the list should not flattened}

  \item{partitionId}{the partition to collect (starts from
  0)}
}
\value{
a list containing elements in the RRDD
}
\description{
\code{collect} returns a list that contains all of the
elements in this RRDD.

\code{collectPartition} returns a list that contains all of
the elements in the specified partition of the RDD.
}
\examples{
\dontrun{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10, 2L)
collect(rdd) # list from 1 to 10
collectPartition(rdd, 0L) # list from 1 to 5
}
}

