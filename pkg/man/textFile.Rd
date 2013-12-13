\name{textFile}
\alias{textFile}
\title{Create an RDD from a text file.}
\usage{
textFile(sc, path, minSplits = NULL)
}
\arguments{
  \item{sc}{SparkContext to use}

  \item{path}{Path of file to read}

  \item{minSplits}{Minimum number of splits to be created.
  If NULL, the default value is chosen based on available
  parallelism.}
}
\value{
RRDD where each item is of type \code{character}
}
\description{
This function reads a text file from HDFS, a local file
system (available on all nodes), or any Hadoop-supported
file system URI, and creates an RDD of strings from it.
}
\examples{
\dontrun{
 sc <- sparkR.init()
 lines <- textFile(sc, "myfile.txt")
}
}

