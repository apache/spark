# RRDD (RDD in R) class implemented in S4 OO system.

#setOldClass("jobjRef")

setClass("RRDD", slots = list(jrdd = "jobjRef",
                              serialized = "logical"))

setValidity("RRDD",
            function(object) {
              cls <- object@jrdd$getClass()
              className <- cls$getName()
              if (grep("spark.api.java.*RDD*", className) == 1) {
                TRUE
              } else {
                paste("Invalid RDD class ", className)
              }
            })

# Constructor of the RRDD class.
RRDD <- function(jrdd, serialized = TRUE) {
  new("RRDD", jrdd = jrdd, serialized = serialized)
}


setGeneric("cache", function(rrdd) { standardGeneric("cache") })
setMethod("cache",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            .jcall(rrdd@jrdd, "Lorg/apache/spark/api/java/JavaRDD;", "cache")
            rrdd
          })


# collect(): Return a list that contains all of the elements in this RRDD.
# NOTE: supports only RRDD[Array[Byte]] and RRDD[primitive java type] for now.
setGeneric("collect", function(rrdd, ...) { standardGeneric("collect") })
setMethod("collect",
          signature(rrdd = "RRDD"),
          function(rrdd, flatten = TRUE) {
            # Assumes a pairwise RRDD is backed by a JavaPairRDD.
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            convertJListToRList(collected, flatten)
          })


setGeneric("count", function(rrdd) { standardGeneric("count") })
setMethod("count",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            valsRDD <- lapplyPartition(rrdd, countPartition)
            vals <- collect(valsRDD)
            sum(as.integer(vals))
          })

setMethod("length",
          signature(x = "RRDD"),
          function(x) {
            count(x)
          })


setMethod("lapply",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              lapply(part, FUN)
            }

            lapplyPartition(X, partitionFunc)
          })

setGeneric("map", function(X, FUN) {
           standardGeneric("map") })
setMethod("map",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

setGeneric("flatMap", function(X, FUN) {
           standardGeneric("flatMap") })
setMethod("flatMap",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              unlist(
                lapply(part, FUN)
              )
            }
            lapplyPartition(X, partitionFunc)
          })


setGeneric("lapplyPartition", function(X, FUN) {
           standardGeneric("lapplyPartition") })
setMethod("lapplyPartition",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(part) {
              FUN(part)
            }
            serializedFunc <- serialize("computeFunc",
                                        connection = NULL, ascii = TRUE)
            serializedFuncArr <- .jarray(serializedFunc)

            depsBin <- getDependencies(computeFunc)
            depsBinArr <- .jarray(depsBin)
            rrddRef <- new(J("org.apache.spark.api.r.RRDD"),
                           X@jrdd$rdd(),
                           serializedFuncArr,
                           X@serialized,
                           depsBinArr,
                           X@jrdd$classManifest())
            jrdd <- rrddRef$asJavaRDD()
            RRDD(jrdd, TRUE)
          })

setGeneric("reduce", function(rrdd, func) { standardGeneric("reduce") })
setMethod("reduce",
          signature(rrdd = "RRDD", func = "ANY"),
          function(rrdd, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collect(lapplyPartition(rrdd, reducePartition),
                                     flatten=FALSE)
            Reduce(func, partitionList)
          })

# Take the first NUM elements in the RRDD and returns them in a list.
setGeneric("take", function(rrdd, num) { standardGeneric("take") })
setMethod("take",
          signature(rrdd = "RRDD", num = "numeric"),
          function(rrdd, num) {
            resList <- list()
            index <- -1
            numPartitions <- .jcall(rrdd@jrdd, "I", "numPartitions")
            while (TRUE) {
              index <- index + 1

              if (length(resList) >= num || index >= numPartitions)
                break

              # a JList of byte arrays
              partition <- .jcall(rrdd@jrdd,
                                  "Ljava/util/List;",
                                  "collectPartition",
                                  as.integer(index))
              elems <- convertJListToRList(partition, flatten = TRUE)
              # TODO: Check if this append is O(n^2)?
              resList <- append(resList, head(elems, n = num - length(resList)))
            }
            resList
          })

############ Shuffle Functions ############

setGeneric("partitionBy",
           function(rrdd, numPartitions, ...) {
             standardGeneric("partitionBy")
           })
setMethod("partitionBy",
          signature(rrdd = "RRDD", numPartitions = "integer"),
          function(rrdd, numPartitions, partitionFunc = hashCode) {

            #if (missing(partitionFunc)) {
            #  partitionFunc <- hashCode
            #}

            depsBin <- getDependencies(partitionFunc)
            depsBinArr <- .jarray(depsBin)

            serializedHashFunc <- serialize(as.character(substitute(partitionFunc)),
                                            connection = NULL,
                                            ascii = TRUE)
            serializedHashFuncBytes <- .jarray(serializedHashFunc)

            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs). 
            pairwiseRRDD <- new(J("org.apache.spark.api.r.PairwiseRRDD"),
                                rrdd@jrdd$rdd(),
                                as.integer(numPartitions),
                                serializedHashFuncBytes,
                                rrdd@serialized,
                                depsBinArr,
                                rrdd@jrdd$classManifest())

            # Create a corresponding partitioner.
            rPartitioner <- new(J("org.apache.spark.HashPartitioner"),
                                as.integer(numPartitions))

            # Call partitionBy on the obtained PairwiseRDD.
            javaPairRDD <- pairwiseRRDD$asJavaPairRDD()$partitionBy(rPartitioner)

            # Call .values() on the result to get back the final result, the
            # shuffled acutal content key-val pairs.
            r <- javaPairRDD$values()

            RRDD(r, serialized=TRUE)
          })

setGeneric("groupByKey",
           function(rrdd, numPartitions) {
             standardGeneric("groupByKey")
           })
setMethod("groupByKey",
          signature(rrdd = "RRDD", numPartitions = "integer"),
          function(rrdd, numPartitions) {
            shuffled <- partitionBy(rrdd, numPartitions)
            groupVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              # Each item in the partition is list of (K, V)
              lapply(part,
                     function(item) {
                       hashVal <- as.character(hashCode(item[[1]]))
                       if (exists(hashVal, vals)) {
                         acc <- vals[[hashVal]]
                         acc[[length(acc) + 1]] <- item[[2]]
                         vals[[hashVal]] <- acc
                       } else {
                         vals[[hashVal]] <- list(item[[2]])
                         keys[[hashVal]] <- item[[1]]
                       }
                     })
              # Every key in the environment contains a list
              # Convert that to list(K, Seq[V])
              grouped <- lapply(ls(vals),
                                function(name) {
                                  list(keys[[name]], vals[[name]])
                                })
              grouped
            }
            lapplyPartition(shuffled, groupVals)
          })

setGeneric("reduceByKey",
           function(rrdd, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })
setMethod("reduceByKey",
          signature(rrdd = "RRDD", combineFunc = "ANY", numPartitions = "integer"),
          function(rrdd, combineFunc, numPartitions) {
            # TODO: Implement map-side combine 
            shuffled <- partitionBy(rrdd, numPartitions)
            reduceVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       hashVal <- as.character(hashCode(item[[1]]))
                       if (exists(hashVal, vals)) {
                         vals[[hashVal]] <- do.call(
                           combineFunc, list(vals[[hashVal]], item[[2]]))
                       } else {
                         vals[[hashVal]] <- item[[2]]
                         keys[[hashVal]] <- item[[1]]
                       }
                     })
              combined <- lapply(ls(vals),
                                  function(name) {
                                    list(keys[[name]], vals[[name]])
                                  })
              combined
            }
            lapplyPartition(shuffled, reduceVals)
          })

setGeneric("collectPartition",
           function(rrdd, partitionId) {
             standardGeneric("collectPartition")
           })
setMethod("collectPartition",
          signature(rrdd = "RRDD", partitionId = "integer"),
          function(rrdd, partitionId) {
            jList <- .jcall(rrdd@jrdd,
                            "Ljava/util/List;",
                            "collectPartition",
                            as.integer(partitionId))
            convertJListToRList(jList, flatten = TRUE)
          })


wrapInt <- function(value) {
  if (value > .Machine$integer.max) {
    value <- value - 2 * .Machine$integer.max - 2
  } else if (value < -1 * .Machine$integer.max) {
    value <- 2 * .Machine$integer.max + value + 2
  }
  value
}

mult31AndAdd <- function(val, addVal) {
  vec <- c(bitwShiftL(val, c(4,3,2,1,0)), addVal)
  Reduce(function(a, b) {
          wrapInt(as.numeric(a) + as.numeric(b))
         },
         vec)
}

hashCode <- function(key) {
  if (class(key) == "integer") {
    as.integer(key[[1]])
  } else if (class(key) == "numeric") {
    # Convert the double to long and then calculate the hash code
    rawVec <- writeBin(key[[1]], con=raw())
    intBits <- packBits(rawToBits(rawVec), "integer")
    as.integer(bitwXor(intBits[2], intBits[1]))
  } else if (class(key) == "character") {
    n <- nchar(key)
    if (n == 0) {
      0L
    } else {
      asciiVals <- sapply(charToRaw(key), function(x) { strtoi(x, 16L) })
      hashC <- 0
      for (k in 1:length(asciiVals)) {
        hashC <- mult31AndAdd(hashC, asciiVals[k])
      }
      as.integer(hashC)
    }
  } else {
    warning(paste("Could not hash object, returning 0", sep=""))
    as.integer(0)
  }
}
