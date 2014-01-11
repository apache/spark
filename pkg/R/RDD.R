# RDD in R implemented in S4 OO system.

#setOldClass("jobjRef")

#' @title S4 class that represents an RDD
#' @description RDD can be created using functions like
#'              \code{parallelize}, \code{textFile} etc.
#' @rdname RDD
#' @seealso parallelize, textFile
#'
#' @param jrdd Java object reference to the backing JavaRDD
#' @param serialized TRUE if the JavaRDD contains serialized R objects
#' @export
setClass("RDD", slots = list(jrdd = "jobjRef", serialized = "logical"))

setValidity("RDD",
            function(object) {
              cls <- object@jrdd$getClass()
              className <- cls$getName()
              if (grep("spark.api.java.*RDD*", className) == 1) {
                TRUE
              } else {
                paste("Invalid RDD class ", className)
              }
            })

#' @rdname RDD
#' @export
RDD <- function(jrdd, serialized = TRUE) {
  new("RDD", jrdd = jrdd, serialized = serialized)
}

#' Persist an RDD
#'
#' Persist this RDD with the default storage level (MEMORY_ONLY).
#'
#' @param rdd The RDD to cache
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd)
#'}
setGeneric("cache", function(rdd) { standardGeneric("cache") })

#' @rdname cache-methods
#' @aliases cache,RDD-method
setMethod("cache",
          signature(rdd = "RDD"),
          function(rdd) {
            .jcall(rdd@jrdd, "Lorg/apache/spark/api/java/JavaRDD;", "cache")
            rdd
          })


#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @param rdd The RDD to collect
#' @param ... Other optional arguments to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RDD
#' @rdname collect-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' collect(rdd) # list from 1 to 10
#' collectPartition(rdd, 0L) # list from 1 to 5
#'}
setGeneric("collect", function(rdd, ...) { standardGeneric("collect") })

#' @rdname collect-methods
#' @aliases collect,RDD-method
setMethod("collect",
          signature(rdd = "RDD"),
          function(rdd, flatten = TRUE) {
            # Assumes a pairwise RDD is backed by a JavaPairRDD.
            collected <- .jcall(rdd@jrdd, "Ljava/util/List;", "collect")
            convertJListToRList(collected, flatten)
          })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectPartition} returns a list that contains all of the elements
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
setGeneric("collectPartition",
           function(rdd, partitionId) {
             standardGeneric("collectPartition")
           })

#' @rdname collect-methods
#' @aliases collectPartition,integer,RDD-method
setMethod("collectPartition",
          signature(rdd = "RDD", partitionId = "integer"),
          function(rdd, partitionId) {
            jPartitionsList <- .jcall(rdd@jrdd,
                                      "[Ljava/util/List;",
                                      "collectPartitions",
                                      .jarray(as.integer(partitionId)))

            jList <- jPartitionsList[[1]]
            convertJListToRList(jList, flatten = TRUE)
          })


#' Return the number of elements in the RDD.
#'
#' @param rdd The RDD to count
#' @return number of elements in the RDD.
#' @rdname count
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' count(rdd) # 10
#' length(rdd) # Same as count
#'}
setGeneric("count", function(rdd) { standardGeneric("count") })

#' @rdname count
#' @aliases count,RDD-method
setMethod("count",
          signature(rdd = "RDD"),
          function(rdd) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            valsRDD <- lapplyPartition(rdd, countPartition)
            vals <- collect(valsRDD)
            sum(as.integer(vals))
          })

#' Return the number of elements in the RDD
#' @export
#' @rdname count
setMethod("length",
          signature(x = "RDD"),
          function(x) {
            count(x)
          })


#' Apply a function to all elements
#'
#' This function creates a new RDD by applying the given transformation to all
#' elements of the given RDD
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
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
          signature(X = "RDD", FUN = "function"),
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
#' @aliases map,RDD,function-method
setMethod("map",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' Flatten results after apply a function to all elements
#'
#' This function return a new RDD by first applying a function to all
#' elements of this RDD, and then flattening the results.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
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
#' @aliases flatMap,RDD,function-method
setMethod("flatMap",
          signature(X = "RDD", FUN = "function"),
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
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition.
#' @return a new RDD created by the transformation.
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
#' @aliases lapplyPartition,RDD,function-method
setMethod("lapplyPartition",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })


#' Return a new RDD by applying a function to each partition of this RDD, while
#' tracking the index of the original partition.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition; takes the partition
#'        index and a list of elements in the particular partition.
#' @return a new RDD created by the transformation.
#' @rdname lapplyPartitionsWithIndex
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 5L)
#' prod <- lapplyPartitionsWithIndex(rdd, function(split, part) {
#'                                          split * Reduce("+", part) })
#' collect(prod, flatten = FALSE) # 0, 7, 22, 45, 76
#'}
setGeneric("lapplyPartitionsWithIndex", function(X, FUN) {
           standardGeneric("lapplyPartitionsWithIndex") })

#' @rdname lapplyPartitionsWithIndex
#' @aliases lapplyPartitionsWithIndex,RDD,function-method
setMethod("lapplyPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(split, part) {
              FUN(split, part)
            }
            serializedFunc <- serialize("computeFunc",
                                        connection = NULL, ascii = TRUE)
            serializedFuncArr <- .jarray(serializedFunc)
            packageNamesArr <- .jarray(serialize(.sparkREnv[[".packages"]],
                                                 connection = NULL,
                                                 ascii = TRUE))
            refs <- lapply(ls(.broadcastNames), function(name) {
                           get(name, .broadcastNames) })
            broadcastArr <- .jarray(refs,
                                    "org/apache/spark/broadcast/Broadcast")


            depsBin <- getDependencies(computeFunc)
            depsBinArr <- .jarray(depsBin)
            rddRef <- new(J("sparkr.RRDD"),
                           X@jrdd$rdd(),
                           serializedFuncArr,
                           X@serialized,
                           depsBinArr,
                           packageNamesArr,
                           as.character(.sparkREnv[["libname"]]),
                           broadcastArr,
                           X@jrdd$classTag())
            jrdd <- rddRef$asJavaRDD()
            RDD(jrdd, TRUE)
          })


#' @rdname lapplyPartitionsWithIndex
#' @export
setGeneric("mapPartitionsWithIndex", function(X, FUN) {
           standardGeneric("mapPartitionsWithIndex") })

#' @rdname lapplyPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,RDD,function-method
setMethod("mapPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, FUN)
          })


#' Reduce across elements of an RDD.
#'
#' This function reduces the elements of this RDD using the
#' specified commutative and associative binary operator.
#'
#' @param rdd The RDD to reduce
#' @param func Commutative and associative function to apply on elements
#'             of the RDD.
#' @export
#' @rdname reduce
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' reduce(rdd, "+") # 55
#'}
setGeneric("reduce", function(rdd, func) { standardGeneric("reduce") })

#' @rdname reduce
#' @aliases reduce,RDD,ANY-method
setMethod("reduce",
          signature(rdd = "RDD", func = "ANY"),
          function(rdd, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collect(lapplyPartition(rdd, reducePartition),
                                     flatten=FALSE)
            Reduce(func, partitionList)
          })

#' Take elements from an RDD.
#'
#' This function takes the first NUM elements in the RDD and
#' returns them in a list.
#'
#' @param rdd The RDD to take elements from
#' @param num Number of elements to take
#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' take(rdd, 2L) # list(1, 2)
#'}
setGeneric("take", function(rdd, num) { standardGeneric("take") })

#' @rdname take
#' @aliases take,RDD,numeric-method
setMethod("take",
          signature(rdd = "RDD", num = "numeric"),
          function(rdd, num) {
            resList <- list()
            index <- -1
            partitions <- .jcall(rdd@jrdd, "Ljava/util/List;", "splits")
            numPartitions <- .jcall(partitions, "I", "size")
            # TODO(shivaram): Collect more than one partition based on size
            # estimates similar to the scala version of `take`.
            while (TRUE) {
              index <- index + 1

              if (length(resList) >= num || index >= numPartitions)
                break

              # a JList of byte arrays
              partitionArr <- .jcall(rdd@jrdd,
                                     "[Ljava/util/List;",
                                     "collectPartitions",
                                     .jarray(as.integer(index)))
              partition <- partitionArr[[1]]
              elems <- convertJListToRList(partition, flatten = TRUE)
              # TODO: Check if this append is O(n^2)?
              resList <- append(resList, head(elems, n = num - length(resList)))
            }
            resList
          })


#' Return an RDD that is a sampled subset of the given RDD.
#'
#' The same as `sample()' in Spark. (We rename it due to signature
#' inconsistencies with the `sample()' function in R's base package.)
#'
#' @param rdd The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#' @rdname sampleRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10) # ensure each num is in its own split
#' collect(sampleRDD(rdd, FALSE, 0.5, 1618L)) # ~5 distinct elements
#' collect(sampleRDD(rdd, TRUE, 0.5, 9L)) # ~5 elements possibly with duplicates
#'}
setGeneric("sampleRDD",
           function(rdd, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

#' @rdname sampleRDD
#' @aliases sampleRDD,RDD
setMethod("sampleRDD",
          signature(rdd = "RDD", withReplacement = "logical",
                    fraction = "numeric", seed = "integer"),
          function(rdd, withReplacement, fraction, seed) {

            # The sampler: takes a partition and returns its sampled version.
            samplingFunc <- function(split, part) {
              set.seed(seed)
              res <- vector("list", length(part))
              len <- 0

              # Discards some random values to ensure each partition has a
              # different random seed.
              runif(split)

              for (elem in part) {
                if (withReplacement) {
                  count <- rpois(1, fraction)
                  if (count > 0) {
                    res[(len + 1):(len + count)] <- rep(list(elem), count)
                    len <- len + count
                  }
                } else {
                  if (runif(1) < fraction) {
                    len <- len + 1
                    res[[len]] <- elem
                  }
                }
              }

              # TODO(zongheng): look into the performance of the current
              # implementation. Look into some iterator package? Note that
              # Scala avoids many calls to creating an empty list and PySpark
              # similarly achieves this using `yield'.
              if (len > 0)
                res[1:len]
              else
                list()
            }

            lapplyPartitionsWithIndex(rdd, samplingFunc)
          })


#' Return a list of the elements that are a sampled subset of the given RDD.
#'
#' @param rdd The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param num Number of elements to return
#' @param seed Randomness seed value
#' @rdname takeSample
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:100)
#' # exactly 5 elements sampled, which may not be distinct
#' takeSample(rdd, TRUE, 5L, 1618L)
#' # exactly 5 distinct elements sampled
#' takeSample(rdd, FALSE, 5L, 16181618L)
#'}
setGeneric("takeSample",
           function(rdd, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })
#' @rdname takeSample
#' @aliases takeSample,RDD
setMethod("takeSample", signature(rdd = "RDD", withReplacement = "logical",
                                  num = "integer", seed = "integer"),
          function(rdd, withReplacement, num, seed) {
            # This function is ported from RDD.scala.
            fraction <- 0.0
            total <- 0
            multiplier <- 3.0
            initialCount <- count(rdd)
            maxSelected <- 0
            MAXINT <- .Machine$integer.max

            if (num < 0)
              stop(paste("Negative number of elements requested"))

            if (initialCount > MAXINT - 1) {
              maxSelected <- MAXINT - 1
            } else {
              maxSelected <- initialCount
            }

            if (num > initialCount && !withReplacement) {
              total <- maxSelected
              fraction <- multiplier * (maxSelected + 1) / initialCount
            } else {
              total <- num
              fraction <- multiplier * (num + 1) / initialCount
            }

            set.seed(seed)
            samples <- collect(sampleRDD(rdd, withReplacement, fraction,
                                         as.integer(ceiling(runif(1,
                                                                  -MAXINT,
                                                                  MAXINT)))))
            # If the first sample didn't turn out large enough, keep trying to
            # take samples; this shouldn't happen often because we use a big
            # multiplier for thei initial size
            while (length(samples) < total)
              samples <- collect(sampleRDD(rdd, withReplacement, fraction,
                                           as.integer(ceiling(runif(1,
                                                                    -MAXINT,
                                                                    MAXINT)))))

            # TODO(zongheng): investigate if this call is an in-place shuffle?
            sample(samples)[1:total]
          })


############ Shuffle Functions ############

#' Partition an RDD by key
#'
#' This function operates on RDDs where every element is of the form list(K, V).
#' For each element of this RDD, the partitioner is used to compute a hash
#' function and the RDD is partitioned using this hash value.
#'
#' @param rdd The RDD to partition. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @param ... Other optional arguments to partitionBy.
#'
#' @param partitionFunc The partition function to use. Uses a default hashCode
#'                      function if not provided
#' @return An RDD partitioned using the specified partitioner.
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
           function(rdd, numPartitions, ...) {
             standardGeneric("partitionBy")
           })

#' @rdname partitionBy
#' @aliases partitionBy,RDD,integer-method
setMethod("partitionBy",
          signature(rdd = "RDD", numPartitions = "integer"),
          function(rdd, numPartitions, partitionFunc = hashCode) {

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
            refs <- lapply(ls(.broadcastNames), function(name) {
                           get(name, .broadcastNames) })
            broadcastArr <- .jarray(refs,
                                    "org/apache/spark/broadcast/Broadcast")


            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs).
            pairwiseRRDD <- new(J("sparkr.PairwiseRRDD"),
                                rdd@jrdd$rdd(),
                                as.integer(numPartitions),
                                serializedHashFuncBytes,
                                rdd@serialized,
                                depsBinArr,
                                packageNamesArr,
                                as.character(.sparkREnv[["libname"]]),
                                broadcastArr,
                                rdd@jrdd$classTag())

            # Create a corresponding partitioner.
            rPartitioner <- new(J("org.apache.spark.HashPartitioner"),
                                as.integer(numPartitions))

            # Call partitionBy on the obtained PairwiseRDD.
            javaPairRDD <- pairwiseRRDD$asJavaPairRDD()$partitionBy(rPartitioner)

            # Call .values() on the result to get back the final result, the
            # shuffled acutal content key-val pairs.
            r <- javaPairRDD$values()

            RDD(r, serialized=TRUE)
          })

#' Group values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V).
#' and group values for each key in the RDD into a single sequence.
#'
#' @param rdd The RDD to group. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, list(V))
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
           function(rdd, numPartitions) {
             standardGeneric("groupByKey")
           })

#' @rdname groupByKey
#' @aliases groupByKey,RDD,integer-method
setMethod("groupByKey",
          signature(rdd = "RDD", numPartitions = "integer"),
          function(rdd, numPartitions) {
            shuffled <- partitionBy(rdd, numPartitions)
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
#' @param rdd The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V).
#' @param combineFunc The associative reduce function to use.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, V') where V' is the merged
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
           function(rdd, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })

#' @rdname reduceByKey
#' @aliases reduceByKey,RDD,integer-method
setMethod("reduceByKey",
          signature(rdd = "RDD", combineFunc = "ANY", numPartitions = "integer"),
          function(rdd, combineFunc, numPartitions) {
            # TODO: Implement map-side combine
            shuffled <- partitionBy(rdd, numPartitions)
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
#' @param rdd The RDD to combine. Should be an RDD where each element is
#'             list(K, V).
#' @param createCombiner Create a combiner (C) given a value (V)
#' @param mergeValue Merge the given value (V) with an existing combiner (C)
#' @param mergeCombiners Merge two combiners and return a new combiner
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, C) where C is the combined type
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
           function(rdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

#' @rdname combineByKey
#' @aliases combineByKey,RDD,ANY,ANY,ANY,integer-method
setMethod("combineByKey",
          signature(rdd = "RDD", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "integer"),
          function(rdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
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
            locallyCombined <- lapplyPartition(rdd, combineLocally)
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

