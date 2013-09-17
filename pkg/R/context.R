# context.R: SparkContext driven functions

# Read a text file from HDFS, a local file system (available on all
# nodes), or any Hadoop-supported file system URI, and return it as an
# RRDD of Strings.
textFile <- function(jsc, name, minSplits=NULL) {
  # FIXME: if execute into this if block, errors:
  # Error in .jcall(jsc, "sc", c()) : RcallMethod: invalid method name
  if (is.null(minSplits)) {
    sc <- .jcall(jsc, "Lorg/apache/spark/SparkContext;", "sc")
    defaultParallelism <- .jcall(sc, "I", "defaultParallelism")
    minSplits <- min(defaultParallelism, 2)
  }
  jrdd <- .jcall(jsc, "Lorg/apache/spark/api/java/JavaRDD;", "textFile", name, as.integer(minSplits))
  RRDD(jrdd)
}


# Distribute a local R collection (list/vector) to form an RRDD.
# FIXME: bound/safeguard numSlices
parallelize <- function(jsc, coll, numSlices) {
  sliceLen <- length(coll) / numSlices
  slices <- split(coll, rep(1:(numSlices + 1), each = sliceLen)[1:length(coll)])
  serializedSlices <- lapply(slices, serialize, connection = NULL)
}
