# Read a text file from HDFS, a local file system (available on all
# nodes), or any Hadoop-supported file system URI, and return it as an
# RDD of Strings.
"textFile" <- function(jsc, name, minSplits=NULL) {
  if (is.null(minSplits)) {
    sc <- .jcall(jsc, "Lorg/apache/spark/SparkContext;", "sc")
    defaultParallelism <- .jcall(sc, "I", "defaultParallelism")
    minSplits <- min(defaultParallelism, 2)
  }
  jrdd <- .jcall(jsc, "Lorg/apache/spark/api/java/JavaRDD;", "textFile", name, as.integer(minSplits))
  # TODO: wrap RRDD around it
}

