# Operations supported on RDDs contains pairs (i.e key, value)

############ Actions and Transformations ############

#' Look up elements of a key in an RDD
#'
#' @description
#' \code{lookup} returns a list of values in this RDD for key key.
#'
#' @param rdd The RDD to collect
#' @param key The key to look up for
#' @return a list of values in this RDD for key key
#' @rdname lookup
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 1), c(2, 2), c(1, 3))
#' rdd <- parallelize(sc, pairs)
#' lookup(rdd, 1) # list(1, 3)
#'}
setGeneric("lookup", function(rdd, key) { standardGeneric("lookup") })

#' @rdname lookup
#' @aliases lookup,RDD-method
setMethod("lookup",
          signature(rdd = "RDD", key = "ANY"),
          function(rdd, key) {
            partitionFunc <- function(part) {
              filtered <- part[unlist(lapply(part, function(x) { identical(key, x[[1]]) }))]
              lapply(filtered, function(x) { x[[2]] })
            }
            valsRDD <- lapplyPartition(rdd, partitionFunc)
            collect(valsRDD)
          })

#' Count the number of elements for each key, and return the result to the
#' master as lists of (key, count) pairs.
#'
#' Same as countByKey in Spark.
#'
#' @param rdd The RDD to count keys.
#' @return list of (key, count) pairs, where count is number of each key in rdd.
#' @rdname countByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(c("a", 1), c("b", 1), c("a", 1)))
#' countByKey(rdd) # ("a", 2L), ("b", 1L)
#'}
setGeneric("countByKey", function(rdd) { standardGeneric("countByKey") })

#' @rdname countByKey
#' @aliases countByKey,RDD-method
setMethod("countByKey",
          signature(rdd = "RDD"),
          function(rdd) {
            keys <- lapply(rdd, function(item) { item[[1]] })
            countByValue(keys)
          })

#' Return an RDD with the keys of each tuple.
#'
#' @param rdd The RDD from which the keys of each tuple is returned.
#' @rdname keys
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(keys(rdd)) # list(1, 3)
#'}
setGeneric("keys", function(rdd) { standardGeneric("keys") })

#' @rdname keys
#' @aliases keys,RDD
setMethod("keys",
          signature(rdd = "RDD"),
          function(rdd) {
            func <- function(x) {
              x[[1]]
            }
            lapply(rdd, func)
          })

#' Return an RDD with the values of each tuple.
#'
#' @param rdd The RDD from which the values of each tuple is returned.
#' @rdname values
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(values(rdd)) # list(2, 4)
#'}
setGeneric("values", function(rdd) { standardGeneric("values") })

#' @rdname values
#' @aliases values,RDD
setMethod("values",
          signature(rdd = "RDD"),
          function(rdd) {
            func <- function(x) {
              x[[2]]
            }
            lapply(rdd, func)
          })

#' Applies a function to all values of the elements, without modifying the keys.
#'
#' The same as `mapValues()' in Spark.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on the value of each element.
#' @return a new RDD created by the transformation.
#' @rdname mapValues
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' makePairs <- lapply(rdd, function(x) { list(x, x) })
#' collect(mapValues(makePairs, function(x) { x * 2) })
#' Output: list(list(1,2), list(2,4), list(3,6), ...)
#'}
setGeneric("mapValues", function(X, FUN) { standardGeneric("mapValues") })

#' @rdname mapValues
#' @aliases mapValues,RDD,function-method
setMethod("mapValues",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            func <- function(x) {
              list(x[[1]], FUN(x[[2]]))
            }
            lapply(X, func)
          })

#' Pass each value in the key-value pair RDD through a flatMap function without
#' changing the keys; this also retains the original RDD's partitioning.
#'
#' The same as 'flatMapValues()' in Spark.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on the value of each element.
#' @return a new RDD created by the transformation.
#' @rdname flatMapValues
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, c(1,2)), list(2, c(3,4))))
#' collect(flatMapValues(rdd, function(x) { x }))
#' Output: list(list(1,1), list(1,2), list(2,3), list(2,4))
#'}
setGeneric("flatMapValues", function(X, FUN) { standardGeneric("flatMapValues") })

#' @rdname flatMapValues
#' @aliases flatMapValues,RDD,function-method
setMethod("flatMapValues",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            flatMapFunc <- function(x) {
              lapply(FUN(x[[2]]), function(v) { list(x[[1]], v) })
            }
            flatMap(X, flatMapFunc)
          })

############ Shuffle Functions ############

#' Partition an RDD by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' For each element of this RDD, the partitioner is used to compute a hash
#' function and the RDD is partitioned using this hash value.
#'
#' @param rdd The RDD to partition. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
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
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- partitionBy(rdd, 2L)
#' collectPartition(parts, 0L) # First partition should contain list(1, 2) and list(1, 4)
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

            depsBinArr <- getDependencies(partitionFunc)

            serializedHashFuncBytes <- serialize(as.character(substitute(partitionFunc)),
                                                 connection = NULL,
                                                 ascii = TRUE)

            packageNamesArr <- serialize(.sparkREnv$.packages,
                                         connection = NULL,
                                         ascii = TRUE)
            broadcastArr <- lapply(ls(.broadcastNames), function(name) {
                                   get(name, .broadcastNames) })
            jrdd <- getJRDD(rdd)

            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs).
            pairwiseRRDD <- newJObject("edu.berkeley.cs.amplab.sparkr.PairwiseRRDD",
                                       callJMethod(jrdd, "rdd"),
                                       as.integer(numPartitions),
                                       serializedHashFuncBytes,
                                       getSerializedMode(rdd),
                                       depsBinArr,
                                       packageNamesArr,
                                       as.character(.sparkREnv$libname),
                                       broadcastArr,
                                       callJMethod(jrdd, "classTag"))

            # Create a corresponding partitioner.
            rPartitioner <- newJObject("org.apache.spark.HashPartitioner",
                                       as.integer(numPartitions))

            # Call partitionBy on the obtained PairwiseRDD.
            javaPairRDD <- callJMethod(pairwiseRRDD, "asJavaPairRDD")
            javaPairRDD <- callJMethod(javaPairRDD, "partitionBy", rPartitioner)

            # Call .values() on the result to get back the final result, the
            # shuffled acutal content key-val pairs.
            r <- callJMethod(javaPairRDD, "values")

            RDD(r, serializedMode = "byte")
          })

#' Group values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and group values for each key in the RDD into a single sequence.
#'
#' @param rdd The RDD to group. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, list(V))
#' @seealso reduceByKey
#' @rdname groupByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
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
              pred <- function(item) exists(item$hash, keys)
              appendList <- function(acc, x) {
                addItemToAccumulator(acc, x)
                acc
              }
              makeList <- function(x) {
                acc <- initAccumulator()
                addItemToAccumulator(acc, x)
                acc
              }
              # Each item in the partition is list of (K, V)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred,
                                          appendList, makeList)
                     })
              # extract out data field
              vals <- eapply(vals,
                             function(x) {
                               length(x$data) <- x$counter
                               x$data
                             })
              # Every key in the environment contains a list
              # Convert that to list(K, Seq[V])
              convertEnvsToList(keys, vals)
            }
            lapplyPartition(shuffled, groupVals)
          })

#' Merge values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function.
#'
#' @param rdd The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
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
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
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
            reduceVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred, combineFunc, identity)
                     })
              convertEnvsToList(keys, vals)
            }
            locallyReduced <- lapplyPartition(rdd, reduceVals)
            shuffled <- partitionBy(locallyReduced, numPartitions)
            lapplyPartition(shuffled, reduceVals)
          })

#' Merge values by key locally
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function, but return the
#' results immediately to the driver as an R list.
#'
#' @param rdd The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param combineFunc The associative reduce function to use.
#' @return A list of elements of type list(K, V') where V' is the merged value for each key
#' @rdname reduceByKeyLocally
#' @seealso reduceByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' reduced <- reduceByKeyLocally(rdd, "+")
#' reduced # list(list(1, 6), list(1.1, 3))
#'}
setGeneric("reduceByKeyLocally",
           function(rdd, combineFunc) {
             standardGeneric("reduceByKeyLocally")
           })

#' @rdname reduceByKeyLocally
#' @aliases reduceByKeyLocally,RDD,integer-method
setMethod("reduceByKeyLocally",
          signature(rdd = "RDD", combineFunc = "ANY"),
          function(rdd, combineFunc) {
            reducePart <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred, combineFunc, identity)
                     })
              list(list(keys, vals)) # return hash to avoid re-compute in merge
            }
            mergeParts <- function(accum, x) {
              pred <- function(item) {
                exists(item$hash, accum[[1]])
              }
              lapply(ls(x[[1]]),
                     function(name) {
                       item <- list(x[[1]][[name]], x[[2]][[name]])
                       item$hash <- name
                       updateOrCreatePair(item, accum[[1]], accum[[2]], pred, combineFunc, identity)
                     })
              accum
            }
            reduced <- mapPartitions(rdd, reducePart)
            merged <- reduce(reduced, mergeParts)
            convertEnvsToList(merged[[1]], merged[[2]])
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
#'             list(K, V) or c(K, V).
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
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
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
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(item[[1]])
                       updateOrCreatePair(item, keys, combiners, pred, mergeValue, createCombiner)
                     })
              convertEnvsToList(keys, combiners)
            }
            locallyCombined <- lapplyPartition(rdd, combineLocally)
            shuffled <- partitionBy(locallyCombined, numPartitions)
            mergeAfterShuffle <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(item[[1]])
                       updateOrCreatePair(item, keys, combiners, pred, mergeCombiners, identity)
                     })
              convertEnvsToList(keys, combiners)
            }
            lapplyPartition(shuffled, mergeAfterShuffle)
          })

############ Binary Functions #############

#' Join two RDDs
#'
#' @description
#' \code{join} This function joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with matching keys in
#'         two input RDDs.
#' @rdname join-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' join(rdd1, rdd2, 2L) # list(list(1, list(1, 2)), list(1, list(1, 3))
#'}
setGeneric("join", function(rdd1, rdd2, numPartitions) { standardGeneric("join") })

#' @rdname join-methods
#' @aliases join,RDD,RDD-method
setMethod("join",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Left outer join two RDDs
#'
#' @description
#' \code{leftouterjoin} This function left-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in rdd1, the resulting RDD will either contain 
#'         all pairs (k, (v, w)) for (k, w) in rdd2, or the pair (k, (v, NULL)) 
#'         if no elements in rdd2 have key k.
#' @rdname join-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' leftOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
#'}
setGeneric("leftOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("leftOuterJoin") })

#' @rdname join-methods
#' @aliases leftOuterJoin,RDD,RDD-method
setMethod("leftOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, TRUE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Right outer join two RDDs
#'
#' @description
#' \code{rightouterjoin} This function right-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, w) in rdd2, the resulting RDD will either contain
#'         all pairs (k, (v, w)) for (k, v) in rdd1, or the pair (k, (NULL, w))
#'         if no elements in rdd1 have key k.
#' @rdname join-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rightOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
#'}
setGeneric("rightOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("rightOuterJoin") })

#' @rdname join-methods
#' @aliases rightOuterJoin,RDD,RDD-method
setMethod("rightOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Full outer join two RDDs
#'
#' @description
#' \code{fullouterjoin} This function full-outer-joins two RDDs where every element is of the form list(K, V). 
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in rdd1 and (k, w) in rdd2, the resulting RDD
#'         will contain all pairs (k, (v, w)) for both (k, v) in rdd1 and and
#'         (k, w) in rdd2, or the pair (k, (NULL, w))/(k, (v, NULL)) if no elements 
#'         in rdd1/rdd2 have key k.
#' @rdname join-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3), list(3, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' fullOuterJoin(rdd1, rdd2, 2L) # list(list(1, list(2, 1)),
#'                               #      list(1, list(3, 1)),
#'                               #      list(2, list(NULL, 4)))
#'                               #      list(3, list(3, NULL)),
#'}
setGeneric("fullOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("fullOuterJoin") })

#' @rdname join-methods
#' @aliases fullOuterJoin,RDD,RDD-method

setMethod("fullOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, TRUE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' For each key k in several RDDs, return a resulting RDD that
#' whose values are a list of values for the key in all RDDs.
#'
#' @param ... Several RDDs.
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with values in a list
#' in all RDDs.
#' @rdname cogroup
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' cogroup(rdd1, rdd2, numPartitions = 2L) 
#' # list(list(1, list(1, list(2, 3))), list(2, list(list(4), list()))
#'}
setGeneric("cogroup", 
           function(..., numPartitions) { standardGeneric("cogroup") },
           signature = "...")

#' @rdname cogroup
#' @aliases cogroup,RDD-method
setMethod("cogroup",
          "RDD",
          function(..., numPartitions) {
            rdds <- list(...)
            rddsLen <- length(rdds)
            for (i in 1:rddsLen) {
              rdds[[i]] <- lapply(rdds[[i]], 
                                  function(x) { list(x[[1]], list(i, x[[2]])) })
              # TODO(hao): As issue [SparkR-142] mentions, the right value of i
              # will not be captured into UDF if getJRDD is not invoked.
              # It should be resolved together with that issue.
              getJRDD(rdds[[i]])  # Capture the closure.
            }
            union.rdd <- Reduce(unionRDD, rdds)
            group.func <- function(vlist) {
              res <- list()
              length(res) <- rddsLen
              for (x in vlist) {
                i <- x[[1]]
                acc <- res[[i]]
                # Create an accumulator.
                if (is.null(acc)) {
                  acc <- SparkR:::initAccumulator()
                }
                SparkR:::addItemToAccumulator(acc, x[[2]])
                res[[i]] <- acc
              }
              lapply(res, function(acc) {
                if (is.null(acc)) {
                  list()
                } else {
                  acc$data
                }
              })
            }
            cogroup.rdd <- mapValues(groupByKey(union.rdd, numPartitions), 
                                     group.func)
          })

#' Sort a (k, v) pair RDD by k.
#'
#' @param rdd A (k, v) pair RDD to be sorted.
#' @param ascending A flag to indicate whether the sorting is ascending or descending.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where all (k, v) pair elements are sorted.
#' @rdname sortByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(3, 1), list(2, 2), list(1, 3)))
#' collect(sortByKey(rdd)) # list (list(1, 3), list(2, 2), list(3, 1))
#'}
setGeneric("sortByKey", function(rdd,
                                 ascending = TRUE,
                                 numPartitions = 1L) {
                          standardGeneric("sortByKey")
                        })

#' @rdname sortByKey
#' @aliases sortByKey,RDD,RDD-method
setMethod("sortByKey",
          signature(rdd = "RDD"),
          function(rdd, ascending = TRUE, numPartitions = SparkR::numPartitions(rdd)) {
            rangeBounds <- list()
            
            if (numPartitions > 1) {
              rddSize <- count(rdd)
              # constant from Spark's RangePartitioner
              maxSampleSize <- numPartitions * 20
              fraction <- min(maxSampleSize / max(rddSize, 1), 1.0)
              
              samples <- collect(keys(sampleRDD(rdd, FALSE, fraction, 1L)))
              
              # Note: the built-in R sort() function only works on atomic vectors
              samples <- sort(unlist(samples, recursive = FALSE), decreasing = !ascending)
              
              if (length(samples) > 0) {
                rangeBounds <- lapply(seq_len(numPartitions - 1),
                                      function(i) {
                                        j <- ceiling(length(samples) * i / numPartitions)
                                        samples[j]
                                      })
              }
            }

            rangePartitionFunc <- function(key) {
              partition <- 0
              
              # TODO: Use binary search instead of linear search, similar with Spark
              while (partition < length(rangeBounds) && key > rangeBounds[[partition + 1]]) {
                partition <- partition + 1
              }
              
              if (ascending) {
                partition
              } else {
                numPartitions - partition - 1
              }
            }
            
            partitionFunc <- function(part) {
              sortKeyValueList(part, decreasing = !ascending)
            }
            
            newRDD <- partitionBy(rdd, numPartitions, rangePartitionFunc)
            lapplyPartition(newRDD, partitionFunc)
          })
          
