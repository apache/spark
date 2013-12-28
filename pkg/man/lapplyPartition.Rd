\docType{methods}
\name{lapplyPartition}
\alias{lapplyPartition}
\alias{lapplyPartition,RDD,function-method}
\title{Apply a function to each partition of an RDD}
\usage{
lapplyPartition(X, FUN)

\S4method{lapplyPartition}{RDD,function}(X, FUN)
}
\arguments{
  \item{X}{The RDD to apply the transformation.}

  \item{FUN}{the transformation to apply on each
  partition.}
}
\value{
a new RDD created by the transformation.
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

