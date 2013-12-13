\name{sparkR.init}
\alias{sparkR.init}
\title{Initialize a new Spark Context.}
\usage{
sparkR.init(master = "local", appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"))
}
\arguments{
  \item{master}{The Spark master URL.}

  \item{appName}{Application name to register with cluster
  manager}

  \item{sparkHome}{Spark Home directory}
}
\description{
This function initializes a new SparkContext.
}
\examples{
\dontrun{
sparkR.init("local[2]", "SparkR", "/home/spark")
}
}

