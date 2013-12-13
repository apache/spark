\docType{methods}
\name{lapply,RRDD,function-method}
\alias{lapply,RRDD,function-method}
\alias{map}
\title{Apply a function to all elements}
\usage{
\S4method{lapply}{RRDD,function}(X, FUN)

map(X, FUN)
}
\arguments{
  \item{X}{The RRDD to apply the transformation.}

  \item{FUN}{the transformation to apply on each element}
}
\value{
a new RRDD created by the transformation.
}
\description{
This function creates a new RRDD by applying the given
transformation to all elements of the given RDD
}
\examples{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
multiplyByTwo <- lapply(rdd, function(x) { x * 2 })
collect(multiplyByTwo) # 2,4,6...
}

