\name{flatMap}
\alias{flatMap}
\title{Flatten results after apply a function to all elements}
\usage{
flatMap(X, FUN)
}
\arguments{
  \item{X}{The RRDD to apply the transformation.}

  \item{FUN}{the transformation to apply on each element}
}
\value{
a new RRDD created by the transformation.
}
\description{
This function return a new RDD by first applying a function
to all elements of this RDD, and then flattening the
results.
}
\examples{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
multiplyByTwo <- flatMap(rdd, function(x) { list(x*2, x*10) })
collect(multiplyByTwo) # 2,20,4,40,6,60...
}

