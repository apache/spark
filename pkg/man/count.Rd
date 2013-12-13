\docType{methods}
\name{count}
\alias{count}
\alias{count,RRDD-method}
\alias{length,RRDD-method}
\title{Return the number of elements in the RDD.}
\usage{
count(rrdd)

\S4method{count}{RRDD}(rrdd)

\S4method{length}{RRDD}(x)
}
\arguments{
  \item{rrdd}{The RRDD to count}

  \item{x}{an \R object.  For replacement, a vector or
  factor.}
}
\value{
number of elements in the RRDD.
}
\description{
Return the number of elements in the RDD.

Return the number of elements in the RDD
}
\examples{
\dontrun{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
count(rdd) # 10
length(rdd) # Same as count
}
}

