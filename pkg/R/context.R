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
# list. Use pairwise == TRUE if coll is a collection of homogeneous key-val
# pairs (first slot will be treated as the key, second slot the value).
parallelize <- function(jsc, coll, numSlices = 1, pairwise = FALSE) {
  # TODO: bound/safeguard numSlices
  # TODO: unit tests for if the split works for all primitives
  # TODO: support matrix, data frame, etc
  if (!is.list(coll)) {
    if (!is.vector(coll)) {
      message(paste("context.R: parallelize() currently only supports lists and vectors.",
                    "Calling as.list() to coerce coll into a list."))
    }
    coll <- as.list(coll)
  }

  if (numSlices > length(coll))
    numSlices <- length(coll)

  sliceLen <- length(coll) %/% numSlices
  slices <- split(coll, rep(1:(numSlices + 1), each = sliceLen)[1:length(coll)])

  # Serialize each slice: obtain a list of raws, or a list of lists (slices) of
  # 2-tuples of raws
  serializedSlices <- if (!pairwise) {
    lapply(slices, serialize, connection = NULL)
  } else {
    tupleSerialize <- function(tuple) {
      keyRaw <- serialize(tuple[[1]], NULL)
      valRaw <- serialize(tuple[[2]], NULL)
      list(keyRaw, valRaw)
    }
    sliceSerialize <- function(slice) {
      lapply(slice, tupleSerialize)
    }
    lapply(slices, sliceSerialize)
  }

  # If !pairwise, Array[Array[Byte]]; otherwise, _two_ more nested layers.
  javaSerializedSlices <- if (!pairwise) {
    .jarray(lapply(serializedSlices, .jarray), contents.class = "[B")
  } else {
    tupleJArray <- function(tuple) {
      keyByteJArray <- .jarray(tuple[[1]], contents.class = "B")
      valByteJArray <- .jarray(tuple[[2]], contents.class = "B")
      .jarray(list(keyByteJArray, valByteJArray), contents.class = "[B")
    }
    sliceJArray <- function(slice) {
      .jarray(lapply(slice, tupleJArray), contents.class = "[[B")
    }
    .jarray(lapply(serializedSlices, sliceJArray), contents.class = "[[[B")
  }

  jrddType = if (!pairwise)
    "Lorg/apache/spark/api/java/JavaRDD;"
  else
    "Lorg/apache/spark/api/java/JavaPairRDD;"

  jrdd <- .jcall("org/apache/spark/api/r/RRDD",
                 jrddType,
                 "createRDDFromArray",
                 jsc,
                 javaSerializedSlices)

  RRDD(jrdd, TRUE)
}
