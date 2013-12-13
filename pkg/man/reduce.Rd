\name{reduce}
\alias{reduce}
\title{Reduce across elements of an RDD.}
\usage{
reduce(rrdd, func)
}
\arguments{
  \item{rrdd}{The RRDD to reduce}

  \item{func}{Commutative and associative function to apply
  on elements of the RRDD.}
}
\description{
This function reduces the elements of this RDD using the
specified commutative and associative binary operator.
}
\examples{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
reduce(rdd, "+") # 55
}

