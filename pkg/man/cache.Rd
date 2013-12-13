\name{cache}
\alias{cache}
\title{Persist an RDD}
\usage{
cache(rrdd)
}
\arguments{
  \item{rrdd}{The RRDD to cache}
}
\description{
Persist this RDD with the default storage level
(MEMORY_ONLY).
}
\examples{
sc <- sparkR.init()
rrdd <- parallelize(sc, 1:10, 2L)
cache(rrdd)
}

