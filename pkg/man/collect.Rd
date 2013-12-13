\name{collect}
\alias{collect}
\alias{collectPartition}
\title{Collect elements of an RDD}
\usage{
collect(rrdd, ...)

collectPartition(rrdd, partitionId)
}
\arguments{
  \item{rrdd}{The RRDD to collect}

  \item{partitionId}{the partition to collect (starts from
  0)}
}
\value{
a list containing elements in the RRDD.
}
\description{
\code{collect} returns a list that contains all of the
elements in this RRDD.

\code{collectPartition} returns a list that contains all of
the elements in the specified partition of the RDD.
}
\examples{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10, 2L)
collect(rdd) # list from 1 to 10
collectPartition(rdd, 0L) # list from 1 to 5
}

