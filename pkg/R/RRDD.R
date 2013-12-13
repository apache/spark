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

#' Persist an RDD
#'
#' Persist this RDD with the default storage level (MEMORY_ONLY).
#'
#' @param rrdd The RRDD to cache
#' @examples
#' sc <- sparkR.init()
#' rrdd <- parallelize(sc, 1:10, 2L)
#' cache(rrdd)
setGeneric("cache", function(rrdd) { standardGeneric("cache") })
setMethod("cache",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            .jcall(rrdd@jrdd, "Lorg/apache/spark/api/java/JavaRDD;", "cache")
            rrdd
          })


#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RRDD.
#'
#' @param rrdd The RRDD to collect
#' @return a list containing elements in the RRDD.
#' @rdname collect
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' collect(rdd) # list from 1 to 10
#' collectPartition(rdd, 0L) # list from 1 to 5
setGeneric("collect", function(rrdd, ...) { standardGeneric("collect") })
setMethod("collect",
          signature(rrdd = "RRDD"),
          function(rrdd, flatten = TRUE) {
            # Assumes a pairwise RRDD is backed by a JavaPairRDD.
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            convertJListToRList(collected, flatten)
          })

#' @rdname collect
#' @description
#' \code{collectPartition} returns a list that contains all of the elements 
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
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


#' Return the number of elements in the RDD.
#'
#' @param rrdd The RRDD to count
#' @return number of elements in the RRDD.
#' @rdname count
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' count(rdd) # 10
#' length(rdd) # Same as count
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

#' Return the number of elements in the RDD.
#' @rdname count
setMethod("length",
          signature(x = "RRDD"),
          function(x) {
            count(x)
          })


#' Apply a function to all elements
#'
#' This function creates a new RRDD by applying the given transformation to all
#' elements of the given RDD
#'
#' @param X The RRDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RRDD created by the transformation.
#' @rdname lapply
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- lapply(rdd, function(x) { x * 2 })  
#' collect(multiplyByTwo) # 2,4,6...
setMethod("lapply",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              lapply(part, FUN)
            }

            lapplyPartition(X, partitionFunc)
          })

#' @rdname lapply
setGeneric("map", function(X, FUN) {
           standardGeneric("map") })
setMethod("map",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' Flatten results after apply a function to all elements
#'
#' This function return a new RDD by first applying a function to all 
#' elements of this RDD, and then flattening the results.
#'
#' @param X The RRDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RRDD created by the transformation.
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- flatMap(rdd, function(x) { list(x*2, x*10) })
#' collect(multiplyByTwo) # 2,20,4,40,6,60...
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

# Return a new RDD by applying a function to each partition of this RDD.
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
            packageNamesArr <- .jarray(serialize(.sparkREnv[[".packages"]],
                                                 connection = NULL,
                                                 ascii = TRUE))

            depsBin <- getDependencies(computeFunc)
            depsBinArr <- .jarray(depsBin)
            rrddRef <- new(J("org.apache.spark.api.r.RRDD"),
                           X@jrdd$rdd(),
                           serializedFuncArr,
                           X@serialized,
                           depsBinArr,
                           packageNamesArr,
                           X@jrdd$classManifest())
            jrdd <- rrddRef$asJavaRDD()
            RRDD(jrdd, TRUE)
          })

#' Reduce across elements of an RDD. 
#'
#' This function reduces the elements of this RDD using the 
#' specified commutative and associative binary operator.
#' 
#' @param rrdd The RRDD to reduce
#' @param func Commutative and associative function to apply on elements 
#'             of the RRDD.
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' reduce(rdd, "+") # 55
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

#' Take elements from an RDD.
#'
#' This function takes the first NUM elements in the RRDD and 
#' returns them in a list.
#'
#' @param rrdd The RRDD to take elements from
#' @param num Number of elements to take
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' take(rdd, 2L) # list(1, 2)
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

# Return a copy of the RDD partitioned using the specified partitioner
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

            packageNamesArr <- .jarray(serialize(.sparkREnv[[".packages"]],
                                                 connection = NULL,
                                                 ascii = TRUE))

            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs). 
            pairwiseRRDD <- new(J("org.apache.spark.api.r.PairwiseRRDD"),
                                rrdd@jrdd$rdd(),
                                as.integer(numPartitions),
                                serializedHashFuncBytes,
                                rrdd@serialized,
                                depsBinArr,
                                packageNamesArr,
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

# Group the values for each key in the RDD into a single sequence.
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

# Merge the values for each key using an associative reduce function.
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

