\docType{class}
\name{RRDD-class}
\alias{RRDD}
\alias{RRDD-class}
\title{S4 class that represents an RDD}
\usage{
RRDD(jrdd, serialized = TRUE)
}
\arguments{
  \item{jrdd}{Java object reference to the backing JavaRDD}

  \item{serialized}{TRUE if the JavaRDD contains serialized
  R objects}
}
\description{
RRDD can be created using functions like
\code{parallelize}, \code{textFile} etc.
}
\seealso{
parallelize, textFile
}

