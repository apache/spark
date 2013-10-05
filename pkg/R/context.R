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
  jrdd <- .jcall(jsc, "Lorg/apache/spark/api/java/JavaRDD;", "textFile", name,
                 as.integer(minSplits))
  RRDD(jrdd, FALSE)
}

# Distribute a local R homogeneous list to form an RRDD[Array[Byte]]. If a
# vector is passed as `coll', as.list() will be called on it to convert it to a
# list.
# TODO: bound/safeguard numSlices
# TODO: unit tests for if the split works for all primitives
# TODO: support matrix, data frame, etc
parallelize <- function(jsc, coll, numSlices = 1) {
  if (!is.list(coll)) {
    if (!is.vector(coll)) {
      message(paste("context.R: parallelize() currently only supports lists and vectors.",
                    "Calling as.list() to coerce coll into a list."))
    }
    coll = as.list(coll)
  }

  if (numSlices > length(coll)) {
    message(paste("context.R: parallelize: numSlices larger than coll's length",
                  "; defaulting numSlices to the length.", sep=""))
    numSlices = length(coll)
  }

  sliceLen <- length(coll) %/% numSlices
  slices <- split(coll, rep(1:(numSlices + 1), each = sliceLen)[1:length(coll)])

  # vector of raws
  serializedSlices <- lapply(slices, serialize, connection = NULL)
  # a java array of byte[]; in Scala, viewed as Array[Array[Byte]]
  javaSerializedSlices <- .jarray(lapply(serializedSlices, .jarray), contents.class = "[B")
  # JavaRDD[Array[Byte]]
  jrdd <- .jcall("org/apache/spark/api/r/RRDD",
                 "Lorg/apache/spark/api/java/JavaRDD;",
                 "createRDDFromArray",
                 jsc,
                 javaSerializedSlices)

  RRDD(jrdd, TRUE)
}
