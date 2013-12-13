\docType{methods}
\name{lapplyPartition}
\alias{lapplyPartition}
\alias{lapplyPartition,RRDD,function-method}
\title{Apply a function to each partition of an RDD}
\usage{
lapplyPartition(X, FUN)

\S4method{lapplyPartition}{RRDD,function}(X, FUN)
}
\arguments{
  \item{X}{The RRDD to apply the transformation.}

  \item{FUN}{the transformation to apply on each
  partition.}
}
\value{
a new RRDD created by the transformation.
}
\description{
Return a new RDD by applying a function to each partition
of this RDD.
}
\examples{
\dontrun{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
partitionSum <- lapplyPartition(rdd, function(part) { Reduce("+", part) })
collect(partitionSum) # 15, 40
}
}

