\docType{methods}
\name{groupByKey}
\alias{groupByKey}
\alias{groupByKey,RRDD,integer-method}
\title{Group values by key}
\usage{
groupByKey(rrdd, numPartitions)

\S4method{groupByKey}{RRDD,integer}(rrdd, numPartitions)
}
\arguments{
  \item{rrdd}{The RRDD to partition. Should be an RDD where
  each element is list(K, V).}

  \item{numPartitions}{Number of partitions to create.}
}
\value{
An RRDD where each element is list(K, list(V))
}
\description{
This function operates on RDDs where every element is of
the form list(K, V). and group values for each key in the
RDD into a single sequence.
}
\examples{
\dontrun{
sc <- sparkR.init()
pairs <- list(c(1, 2), c(1.1, 3), c(1, 4))
rdd <- parallelize(sc, pairs)
parts <- groupByKey(rdd, 2L)
grouped <- collect(parts)
grouped[[1]] # Should be a list(1, list(2, 4))
}
}
\seealso{
reduceByKey
}

