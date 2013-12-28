\docType{methods}
\name{count}
\alias{count}
\alias{count,RDD-method}
\alias{length,RDD-method}
\title{Return the number of elements in the RDD.}
\usage{
count(rdd)

\S4method{count}{RDD}(rdd)

\S4method{length}{RDD}(x)
}
\arguments{
  \item{rdd}{The RDD to count}

  \item{x}{an \R object.  For replacement, a vector or
  factor.}
}
\value{
number of elements in the RDD.
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

