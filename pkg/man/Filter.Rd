\docType{methods}
\name{Filter}
\alias{Filter}
\alias{Filter,function,RDD}
\alias{Filter,function,RDD-method}
\title{Extract the elements that satisfy a filter}
\usage{
Filter(f, x)

\S4method{Filter}{function,RDD}(f,rdd)
}
\arguments{
  \item{f}{A unary predicate function.}
  \item{x}{The RDD to be filtered.}
}
\description{
This function returns a new RDD containing only the elements that satisfy 
a predicate (i.e. returning TRUE in a given logical function). 
The same as `filter()' in Spark.
}
\examples{
\dontrun{
sc <- sparkR.init()
rdd <- parallelize(sc, 1:10)
unlist(collect(Filter(function (x) { x < 3 }, rdd))) # c(1, 2)
}
}

