\docType{methods}
\name{cache}
\alias{cache}
\alias{cache,RRDD-method}
\title{Persist an RDD}
\usage{
cache(rrdd)

\S4method{cache}{RRDD}(rrdd)
}
\arguments{
  \item{rrdd}{The RRDD to cache}
}
\description{
Persist this RDD with the default storage level
(MEMORY_ONLY).
}
\examples{
\dontrun{
sc <- sparkR.init()
rrdd <- parallelize(sc, 1:10, 2L)
cache(rrdd)
}
}

