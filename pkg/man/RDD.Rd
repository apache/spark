\docType{class}
\name{RDD-class}
\alias{RDD}
\alias{RDD-class}
\title{S4 class that represents an RDD}
\usage{
RDD(jrdd, serialized = TRUE)
}
\arguments{
  \item{jrdd}{Java object reference to the backing JavaRDD}

  \item{serialized}{TRUE if the JavaRDD contains serialized
  R objects}
}
\description{
RDD can be created using functions like \code{parallelize},
\code{textFile} etc.
}
\seealso{
parallelize, textFile
}

