# RRDD (RDD in R) class implemented in S4 OO system.

#setOldClass("jobjRef")

#' @title S4 class that represents an RDD
#' @description RRDD can be created using functions like
#'              \code{parallelize}, \code{textFile} etc.
#' @rdname RRDD 
#' @seealso parallelize, textFile
#'
#' @param jrdd Java object reference to the backing JavaRDD
#' @param serialized TRUE if the JavaRDD contains serialized R objects
#' @export
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

#' @rdname RRDD 
#' @export
RRDD <- function(jrdd, serialized = TRUE) {
  new("RRDD", jrdd = jrdd, serialized = serialized)
}

#' Persist an RDD
#'
#' Persist this RDD with the default storage level (MEMORY_ONLY).
#'
#' @param rrdd The RRDD to cache
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rrdd <- parallelize(sc, 1:10, 2L)
#' cache(rrdd)
#'}
setGeneric("cache", function(rrdd) { standardGeneric("cache") })

#' @rdname cache-methods
#' @aliases cache,RRDD-method
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
#' @param ... Other optional arguments to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RRDD
#' @rdname collect-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' collect(rdd) # list from 1 to 10
#' collectPartition(rdd, 0L) # list from 1 to 5
#'}
setGeneric("collect", function(rrdd, ...) { standardGeneric("collect") })

#' @rdname collect-methods
#' @aliases collect,RRDD-method
setMethod("collect",
          signature(rrdd = "RRDD"),
          function(rrdd, flatten = TRUE) {
            # Assumes a pairwise RRDD is backed by a JavaPairRDD.
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            convertJListToRList(collected, flatten)
          })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectPartition} returns a list that contains all of the elements 
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
setGeneric("collectPartition",
           function(rrdd, partitionId) {
             standardGeneric("collectPartition")
           })

#' @rdname collect-methods
#' @aliases collectPartition,integer,RRDD-method
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
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' count(rdd) # 10
#' length(rdd) # Same as count
#'}
setGeneric("count", function(rrdd) { standardGeneric("count") })

#' @rdname count
#' @aliases count,RRDD-method
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

#' Return the number of elements in the RDD
#' @export
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
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- lapply(rdd, function(x) { x * 2 })  
#' collect(multiplyByTwo) # 2,4,6...
#'}
setMethod("lapply",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              lapply(part, FUN)
            }

            lapplyPartition(X, partitionFunc)
          })

#' @rdname lapply
#' @export
setGeneric("map", function(X, FUN) {
           standardGeneric("map") })

#' @rdname lapply
#' @aliases map,RRDD,function-method
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
#' @rdname flatMap
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- flatMap(rdd, function(x) { list(x*2, x*10) })
#' collect(multiplyByTwo) # 2,20,4,40,6,60...
#'}
setGeneric("flatMap", function(X, FUN) {
           standardGeneric("flatMap") })

#' @rdname flatMap
#' @aliases flatMap,RRDD,function-method
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

#' Apply a function to each partition of an RDD
#'
#' Return a new RDD by applying a function to each partition of this RDD.
#'
#' @param X The RRDD to apply the transformation.
#' @param FUN the transformation to apply on each partition.
#' @return a new RRDD created by the transformation.
#' @rdname lapplyPartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' partitionSum <- lapplyPartition(rdd, function(part) { Reduce("+", part) })
#' collect(partitionSum) # 15, 40
#'}
setGeneric("lapplyPartition", function(X, FUN) {
           standardGeneric("lapplyPartition") })

#' @rdname lapplyPartition
#' @aliases lapplyPartition,RRDD,function-method
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
                           X@jrdd$classTag())
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
#' @rdname reduce
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' reduce(rdd, "+") # 55
#'}
setGeneric("reduce", function(rrdd, func) { standardGeneric("reduce") })

#' @rdname reduce
#' @aliases reduce,RRDD,ANY-method
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
#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' take(rdd, 2L) # list(1, 2)
#'}
setGeneric("take", function(rrdd, num) { standardGeneric("take") })

#' @rdname take
#' @aliases take,RRDD,numeric-method
setMethod("take",
          signature(rrdd = "RRDD", num = "numeric"),
          function(rrdd, num) {
            resList <- list()
            index <- -1
            partitions <- .jcall(rrdd@jrdd, "Ljava/util/List;", "splits")
            numPartitions <- .jcall(partitions, "I", "size")
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

#' Partition an RDD by key 
#'
#' This function operates on RDDs where every element is of the form list(K, V).
#' For each element of this RDD, the partitioner is used to compute a hash
#' function and the RDD is partitioned using this hash value.
#'
#' @param rrdd The RRDD to partition. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @param ... Other optional arguments to partitionBy.
#'
#' @param partitionFunc The partition function to use. Uses a default hashCode
#'                      function if not provided
#' @return An RRDD partitioned using the specified partitioner.
#' @rdname partitionBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 2), c(1.1, 3), c(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- partitionBy(rdd, 2L) 
#' collectPartition(parts, 0L) # First partition should contain c(1,2) and c(1,3)
#'}
setGeneric("partitionBy",
           function(rrdd, numPartitions, ...) {
             standardGeneric("partitionBy")
           })

#' @rdname partitionBy
#' @aliases partitionBy,RRDD,integer-method
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
                                rrdd@jrdd$classTag())

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

#' Group values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V).
#' and group values for each key in the RDD into a single sequence.
#'
#' @param rrdd The RRDD to group. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return An RRDD where each element is list(K, list(V))
#' @seealso reduceByKey
#' @rdname groupByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 2), c(1.1, 3), c(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- groupByKey(rdd, 2L) 
#' grouped <- collect(parts) 
#' grouped[[1]] # Should be a list(1, list(2, 4))
#'}
setGeneric("groupByKey",
           function(rrdd, numPartitions) {
             standardGeneric("groupByKey")
           })

#' @rdname groupByKey
#' @aliases groupByKey,RRDD,integer-method
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

#' Merge values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V).
#' and merges the values for each key using an associative reduce function.
#'
#' @param rrdd The RRDD to reduce by key. Should be an RDD where each element is
#'             list(K, V).
#' @param combineFunc The associative reduce function to use.
#' @param numPartitions Number of partitions to create.
#' @return An RRDD where each element is list(K, V') where V' is the merged
#'         value
#' @rdname reduceByKey
#' @seealso groupByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 2), c(1.1, 3), c(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- reduceByKey(rdd, "+", 2L)
#' reduced <- collect(parts) 
#' reduced[[1]] # Should be a list(1, 6)
#'}
setGeneric("reduceByKey",
           function(rrdd, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })

#' @rdname reduceByKey
#' @aliases reduceByKey,RRDD,integer-method
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

#' Combine values by key
#'
#' Generic function to combine the elements for each key using a custom set of
#' aggregation functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)],
#' for a "combined type" C. Note that V and C can be different -- for example, one
#' might group an RDD of type (Int, Int) into an RDD of type (Int, Seq[Int]). 

#' Users provide three functions:
#' \itemize{
#'   \item createCombiner, which turns a V into a C (e.g., creates a one-element list)
#'   \item mergeValue, to merge a V into a C (e.g., adds it to the end of a list) -
#'   \item mergeCombiners, to combine two C's into a single one (e.g., concatentates
#'    two lists).
#' }
#'
#' @param rrdd The RRDD to combine. Should be an RDD where each element is
#'             list(K, V).
#' @param createCombiner Create a combiner (C) given a value (V)
#' @param mergeValue Merge the given value (V) with an existing combiner (C)
#' @param mergeCombiners Merge two combiners and return a new combiner
#' @param numPartitions Number of partitions to create.
#' @return An RRDD where each element is list(K, C) where C is the combined type
#'
#' @rdname combineByKey
#' @seealso groupByKey, reduceByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 2), c(1.1, 3), c(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- combineByKey(rdd, function(x) { x }, "+", "+", 2L)
#' combined <- collect(parts)
#' combined[[1]] # Should be a list(1, 6)
#'}
setGeneric("combineByKey",
           function(rrdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

#' @rdname combineByKey
#' @aliases combineByKey,RRDD,ANY,ANY,ANY,integer-method
setMethod("combineByKey",
          signature(rrdd = "RRDD", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "integer"),
          function(rrdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
            combineLocally <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       k <- as.character(item[[1]])
                       if (!exists(k, keys)) {
                         combiners[[k]] <- do.call(createCombiner,
                                                   list(item[[2]]))
                         keys[[k]] <- item[[1]]
                       } else {
                         combiners[[k]] <- do.call(mergeValue,
                                                   list(combiners[[k]],
                                                        item[[2]]))
                       }
                     })
              lapply(ls(keys), function(k) {
                      list(keys[[k]], combiners[[k]])
                     })
            }
            locallyCombined <- lapplyPartition(rrdd, combineLocally)
            shuffled <- partitionBy(locallyCombined, numPartitions)
            mergeAfterShuffle <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       k <- as.character(item[[1]])
                       if (!exists(k, combiners)) {
                         combiners[[k]] <- item[[2]]
                         keys[[k]] <- item[[1]]
                       } else {
                         combiners[[k]] <- do.call(mergeCombiners,
                                                   list(combiners[[k]],
                                                        item[[2]]))
                       }
                     })
              lapply(ls(keys), function(k) {
                      list(keys[[k]], combiners[[k]])
                     })
            }
            combined <-lapplyPartition(shuffled, mergeAfterShuffle)
            combined
          })
