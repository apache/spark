#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Operations supported on RDDs contains pairs (i.e key, value)
#' @include generics.R jobj.R RDD.R
NULL

############ Actions and Transformations ############

#' Look up elements of a key in an RDD
#'
#' @description
#' \code{lookup} returns a list of values in this RDD for key key.
#'
#' @param x The RDD to collect
#' @param key The key to look up for
#' @return a list of values in this RDD for key key
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 1), c(2, 2), c(1, 3))
#' rdd <- parallelize(sc, pairs)
#' lookup(rdd, 1) # list(1, 3)
#'}
# nolint end
#' @rdname lookup
#' @aliases lookup,RDD-method
#' @noRd
setMethod("lookup",
          signature(x = "RDD", key = "ANY"),
          function(x, key) {
            partitionFunc <- function(part) {
              filtered <- part[unlist(lapply(part, function(i) { identical(key, i[[1]]) }))]
              lapply(filtered, function(i) { i[[2]] })
            }
            valsRDD <- lapplyPartition(x, partitionFunc)
            collectRDD(valsRDD)
          })

#' Count the number of elements for each key, and return the result to the
#' master as lists of (key, count) pairs.
#'
#' Same as countByKey in Spark.
#'
#' @param x The RDD to count keys.
#' @return list of (key, count) pairs, where count is number of each key in rdd.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(c("a", 1), c("b", 1), c("a", 1)))
#' countByKey(rdd) # ("a", 2L), ("b", 1L)
#'}
# nolint end
#' @rdname countByKey
#' @aliases countByKey,RDD-method
#' @noRd
setMethod("countByKey",
          signature(x = "RDD"),
          function(x) {
            keys <- lapply(x, function(item) { item[[1]] })
            countByValue(keys)
          })

#' Return an RDD with the keys of each tuple.
#'
#' @param x The RDD from which the keys of each tuple is returned.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collectRDD(keys(rdd)) # list(1, 3)
#'}
# nolint end
#' @rdname keys
#' @aliases keys,RDD
#' @noRd
setMethod("keys",
          signature(x = "RDD"),
          function(x) {
            func <- function(k) {
              k[[1]]
            }
            lapply(x, func)
          })

#' Return an RDD with the values of each tuple.
#'
#' @param x The RDD from which the values of each tuple is returned.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collectRDD(values(rdd)) # list(2, 4)
#'}
# nolint end
#' @rdname values
#' @aliases values,RDD
#' @noRd
setMethod("values",
          signature(x = "RDD"),
          function(x) {
            func <- function(v) {
              v[[2]]
            }
            lapply(x, func)
          })

#' Applies a function to all values of the elements, without modifying the keys.
#'
#' The same as `mapValues()' in Spark.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on the value of each element.
#' @return a new RDD created by the transformation.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' makePairs <- lapply(rdd, function(x) { list(x, x) })
#' collectRDD(mapValues(makePairs, function(x) { x * 2) })
#' Output: list(list(1,2), list(2,4), list(3,6), ...)
#'}
#' @rdname mapValues
#' @aliases mapValues,RDD,function-method
#' @noRd
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
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, c(1,2)), list(2, c(3,4))))
#' collectRDD(flatMapValues(rdd, function(x) { x }))
#' Output: list(list(1,1), list(1,2), list(2,3), list(2,4))
#'}
#' @rdname flatMapValues
#' @aliases flatMapValues,RDD,function-method
#' @noRd
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
#' @param x The RDD to partition. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @param ... Other optional arguments to partitionBy.
#'
#' @param partitionFunc The partition function to use. Uses a default hashCode
#'                      function if not provided
#' @return An RDD partitioned using the specified partitioner.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- partitionByRDD(rdd, 2L)
#' collectPartition(parts, 0L) # First partition should contain list(1, 2) and list(1, 4)
#'}
#' @rdname partitionBy
#' @aliases partitionBy,RDD,integer-method
#' @noRd
setMethod("partitionByRDD",
          signature(x = "RDD"),
          function(x, numPartitions, partitionFunc = hashCode) {
            stopifnot(is.numeric(numPartitions))

            partitionFunc <- cleanClosure(partitionFunc)
            serializedHashFuncBytes <- serialize(partitionFunc, connection = NULL)

            packageNamesArr <- serialize(.sparkREnv$.packages,
                                         connection = NULL)
            broadcastArr <- lapply(ls(.broadcastNames),
                                   function(name) { get(name, .broadcastNames) })
            jrdd <- getJRDD(x)

            # We create a PairwiseRRDD that extends RDD[(Int, Array[Byte])],
            # where the key is the target partition number, the value is
            # the content (key-val pairs).
            pairwiseRRDD <- newJObject("org.apache.spark.api.r.PairwiseRRDD",
                                       callJMethod(jrdd, "rdd"),
                                       numToInt(numPartitions),
                                       serializedHashFuncBytes,
                                       getSerializedMode(x),
                                       packageNamesArr,
                                       broadcastArr,
                                       callJMethod(jrdd, "classTag"))

            # Create a corresponding partitioner.
            rPartitioner <- newJObject("org.apache.spark.HashPartitioner",
                                       numToInt(numPartitions))

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
#' @param x The RDD to group. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, list(V))
#' @seealso reduceByKey
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- groupByKey(rdd, 2L)
#' grouped <- collectRDD(parts)
#' grouped[[1]] # Should be a list(1, list(2, 4))
#'}
#' @rdname groupByKey
#' @aliases groupByKey,RDD,integer-method
#' @noRd
setMethod("groupByKey",
          signature(x = "RDD", numPartitions = "numeric"),
          function(x, numPartitions) {
            shuffled <- partitionByRDD(x, numPartitions)
            groupVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              appendList <- function(acc, i) {
                addItemToAccumulator(acc, i)
                acc
              }
              makeList <- function(i) {
                acc <- initAccumulator()
                addItemToAccumulator(acc, i)
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
                             function(i) {
                               length(i$data) <- i$counter
                               i$data
                             })
              # Every key in the environment contains a list
              # Convert that to list(K, Seq[V])
              convertEnvsToList(keys, vals)
            }
            lapplyPartition(shuffled, groupVals)
          })

#'  Merge values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative and commutative reduce function.
#'
#' @param x The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param combineFunc The associative and commutative reduce function to use.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, V') where V' is the merged
#'         value
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- reduceByKey(rdd, "+", 2L)
#' reduced <- collectRDD(parts)
#' reduced[[1]] # Should be a list(1, 6)
#'}
#' @rdname reduceByKey
#' @aliases reduceByKey,RDD,integer-method
#' @noRd
setMethod("reduceByKey",
          signature(x = "RDD", combineFunc = "ANY", numPartitions = "numeric"),
          function(x, combineFunc, numPartitions) {
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
            locallyReduced <- lapplyPartition(x, reduceVals)
            shuffled <- partitionByRDD(locallyReduced, numToInt(numPartitions))
            lapplyPartition(shuffled, reduceVals)
          })

#' Merge values by key locally
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative and commutative reduce function, but
#' return the results immediately to the driver as an R list.
#'
#' @param x The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param combineFunc The associative and commutative reduce function to use.
#' @return A list of elements of type list(K, V') where V' is the merged value for each key
#' @seealso reduceByKey
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' reduced <- reduceByKeyLocally(rdd, "+")
#' reduced # list(list(1, 6), list(1.1, 3))
#'}
# nolint end
#' @rdname reduceByKeyLocally
#' @aliases reduceByKeyLocally,RDD,integer-method
#' @noRd
setMethod("reduceByKeyLocally",
          signature(x = "RDD", combineFunc = "ANY"),
          function(x, combineFunc) {
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
            reduced <- mapPartitions(x, reducePart)
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
#' @param x The RDD to combine. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param createCombiner Create a combiner (C) given a value (V)
#' @param mergeValue Merge the given value (V) with an existing combiner (C)
#' @param mergeCombiners Merge two combiners and return a new combiner
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, C) where C is the combined type
#' @seealso groupByKey, reduceByKey
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- combineByKey(rdd, function(x) { x }, "+", "+", 2L)
#' combined <- collectRDD(parts)
#' combined[[1]] # Should be a list(1, 6)
#'}
# nolint end
#' @rdname combineByKey
#' @aliases combineByKey,RDD,ANY,ANY,ANY,integer-method
#' @noRd
setMethod("combineByKey",
          signature(x = "RDD", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "numeric"),
          function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
            combineLocally <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, combiners, pred, mergeValue, createCombiner)
                     })
              convertEnvsToList(keys, combiners)
            }
            locallyCombined <- lapplyPartition(x, combineLocally)
            shuffled <- partitionByRDD(locallyCombined, numToInt(numPartitions))
            mergeAfterShuffle <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, combiners, pred, mergeCombiners, identity)
                     })
              convertEnvsToList(keys, combiners)
            }
            lapplyPartition(shuffled, mergeAfterShuffle)
          })

#' Aggregate a pair RDD by each key.
#'
#' Aggregate the values of each key in an RDD, using given combine functions
#' and a neutral "zero value". This function can return a different result type,
#' U, than the type of the values in this RDD, V. Thus, we need one operation
#' for merging a V into a U and one operation for merging two U's, The former
#' operation is used for merging values within a partition, and the latter is
#' used for merging values between partitions. To avoid memory allocation, both
#' of these functions are allowed to modify and return their first argument
#' instead of creating a new U.
#'
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param seqOp A function to aggregate the values of each key. It may return
#'              a different result type from the type of the values.
#' @param combOp A function to aggregate results of seqOp.
#' @return An RDD containing the aggregation result.
#' @seealso foldByKey, combineByKey
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))
#' zeroValue <- list(0, 0)
#' seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
#' combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
#' aggregateByKey(rdd, zeroValue, seqOp, combOp, 2L)
#'   # list(list(1, list(3, 2)), list(2, list(7, 2)))
#'}
# nolint end
#' @rdname aggregateByKey
#' @aliases aggregateByKey,RDD,ANY,ANY,ANY,integer-method
#' @noRd
setMethod("aggregateByKey",
          signature(x = "RDD", zeroValue = "ANY", seqOp = "ANY",
                    combOp = "ANY", numPartitions = "numeric"),
          function(x, zeroValue, seqOp, combOp, numPartitions) {
            createCombiner <- function(v) {
              do.call(seqOp, list(zeroValue, v))
            }

            combineByKey(x, createCombiner, seqOp, combOp, numPartitions)
          })

#' Fold a pair RDD by each key.
#'
#' Aggregate the values of each key in an RDD, using an associative function "func"
#' and a neutral "zero value" which may be added to the result an arbitrary
#' number of times, and must not change the result (e.g., 0 for addition, or
#' 1 for multiplication.).
#'
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param func An associative function for folding values of each key.
#' @return An RDD containing the aggregation result.
#' @seealso aggregateByKey, combineByKey
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))
#' foldByKey(rdd, 0, "+", 2L) # list(list(1, 3), list(2, 7))
#'}
# nolint end
#' @rdname foldByKey
#' @aliases foldByKey,RDD,ANY,ANY,integer-method
#' @noRd
setMethod("foldByKey",
          signature(x = "RDD", zeroValue = "ANY",
                    func = "ANY", numPartitions = "numeric"),
          function(x, zeroValue, func, numPartitions) {
            aggregateByKey(x, zeroValue, func, func, numPartitions)
          })

############ Binary Functions #############

#' Join two RDDs
#'
#' @description
#' \code{join} This function joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with matching keys in
#'         two input RDDs.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' joinRDD(rdd1, rdd2, 2L) # list(list(1, list(1, 2)), list(1, list(1, 3))
#'}
# nolint end
#' @rdname join-methods
#' @aliases join,RDD,RDD-method
#' @noRd
setMethod("joinRDD",
          signature(x = "RDD", y = "RDD"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, FALSE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions),
                                    doJoin)
          })

#' Left outer join two RDDs
#'
#' @description
#' \code{leftouterjoin} This function left-outer-joins two RDDs where every element is of
#' the form list(K, V). The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in x, the resulting RDD will either contain
#'         all pairs (k, (v, w)) for (k, w) in rdd2, or the pair (k, (v, NULL))
#'         if no elements in rdd2 have key k.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' leftOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
#'}
# nolint end
#' @rdname join-methods
#' @aliases leftOuterJoin,RDD,RDD-method
#' @noRd
setMethod("leftOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "numeric"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, TRUE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' Right outer join two RDDs
#'
#' @description
#' \code{rightouterjoin} This function right-outer-joins two RDDs where every element is of
#' the form list(K, V). The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, w) in y, the resulting RDD will either contain
#'         all pairs (k, (v, w)) for (k, v) in x, or the pair (k, (NULL, w))
#'         if no elements in x have key k.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rightOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
#'}
# nolint end
#' @rdname join-methods
#' @aliases rightOuterJoin,RDD,RDD-method
#' @noRd
setMethod("rightOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "numeric"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, FALSE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' Full outer join two RDDs
#'
#' @description
#' \code{fullouterjoin} This function full-outer-joins two RDDs where every element is of
#' the form list(K, V). The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in x and (k, w) in y, the resulting RDD
#'         will contain all pairs (k, (v, w)) for both (k, v) in x and
#'         (k, w) in y, or the pair (k, (NULL, w))/(k, (v, NULL)) if no elements
#'         in x/y have key k.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3), list(3, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' fullOuterJoin(rdd1, rdd2, 2L) # list(list(1, list(2, 1)),
#'                               #      list(1, list(3, 1)),
#'                               #      list(2, list(NULL, 4)))
#'                               #      list(3, list(3, NULL)),
#'}
# nolint end
#' @rdname join-methods
#' @aliases fullOuterJoin,RDD,RDD-method
#' @noRd
setMethod("fullOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "numeric"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, TRUE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' For each key k in several RDDs, return a resulting RDD that
#' whose values are a list of values for the key in all RDDs.
#'
#' @param ... Several RDDs.
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with values in a list
#' in all RDDs.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' cogroup(rdd1, rdd2, numPartitions = 2L)
#' # list(list(1, list(1, list(2, 3))), list(2, list(list(4), list()))
#'}
# nolint end
#' @rdname cogroup
#' @aliases cogroup,RDD-method
#' @noRd
setMethod("cogroup",
          "RDD",
          function(..., numPartitions) {
            rdds <- list(...)
            rddsLen <- length(rdds)
            for (i in 1:rddsLen) {
              rdds[[i]] <- lapply(rdds[[i]],
                                  function(x) { list(x[[1]], list(i, x[[2]])) })
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
                  acc <- initAccumulator()
                }
                addItemToAccumulator(acc, x[[2]])
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
#' @param x A (k, v) pair RDD to be sorted.
#' @param ascending A flag to indicate whether the sorting is ascending or descending.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where all (k, v) pair elements are sorted.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(3, 1), list(2, 2), list(1, 3)))
#' collectRDD(sortByKey(rdd)) # list (list(1, 3), list(2, 2), list(3, 1))
#'}
# nolint end
#' @rdname sortByKey
#' @aliases sortByKey,RDD,RDD-method
#' @noRd
setMethod("sortByKey",
          signature(x = "RDD"),
          function(x, ascending = TRUE, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            rangeBounds <- list()

            if (numPartitions > 1) {
              rddSize <- countRDD(x)
              # constant from Spark's RangePartitioner
              maxSampleSize <- numPartitions * 20
              fraction <- min(maxSampleSize / max(rddSize, 1), 1.0)

              samples <- collectRDD(keys(sampleRDD(x, FALSE, fraction, 1L)))

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

            newRDD <- partitionByRDD(x, numPartitions, rangePartitionFunc)
            lapplyPartition(newRDD, partitionFunc)
          })

#' Subtract a pair RDD with another pair RDD.
#'
#' Return an RDD with the pairs from x whose keys are not in other.
#'
#' @param x An RDD.
#' @param other An RDD.
#' @param numPartitions Number of the partitions in the result RDD.
#' @return An RDD with the pairs from x whose keys are not in other.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list("a", 1), list("b", 4),
#'                              list("b", 5), list("a", 2)))
#' rdd2 <- parallelize(sc, list(list("a", 3), list("c", 1)))
#' collectRDD(subtractByKey(rdd1, rdd2))
#' # list(list("b", 4), list("b", 5))
#'}
# nolint end
#' @rdname subtractByKey
#' @aliases subtractByKey,RDD
#' @noRd
setMethod("subtractByKey",
          signature(x = "RDD", other = "RDD"),
          function(x, other, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            filterFunction <- function(elem) {
              iters <- elem[[2]]
              (length(iters[[1]]) > 0) && (length(iters[[2]]) == 0)
            }

            flatMapValues(filterRDD(cogroup(x,
                                            other,
                                            numPartitions = numPartitions),
                                    filterFunction),
                          function (v) { v[[1]] })
          })

#' Return a subset of this RDD sampled by key.
#'
#' @description
#' \code{sampleByKey} Create a sample of this RDD using variable sampling rates
#' for different keys as specified by fractions, a key to sampling rate map.
#'
#' @param x The RDD to sample elements by key, where each element is
#'             list(K, V) or c(K, V).
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3000)
#' pairs <- lapply(rdd, function(x) { if (x %% 3 == 0) list("a", x)
#'                                    else { if (x %% 3 == 1) list("b", x) else list("c", x) }})
#' fractions <- list(a = 0.2, b = 0.1, c = 0.3)
#' sample <- sampleByKey(pairs, FALSE, fractions, 1618L)
#' 100 < length(lookup(sample, "a")) && 300 > length(lookup(sample, "a")) # TRUE
#' 50 < length(lookup(sample, "b")) && 150 > length(lookup(sample, "b")) # TRUE
#' 200 < length(lookup(sample, "c")) && 400 > length(lookup(sample, "c")) # TRUE
#' lookup(sample, "a")[which.min(lookup(sample, "a"))] >= 0 # TRUE
#' lookup(sample, "a")[which.max(lookup(sample, "a"))] <= 2000 # TRUE
#' lookup(sample, "b")[which.min(lookup(sample, "b"))] >= 0 # TRUE
#' lookup(sample, "b")[which.max(lookup(sample, "b"))] <= 2000 # TRUE
#' lookup(sample, "c")[which.min(lookup(sample, "c"))] >= 0 # TRUE
#' lookup(sample, "c")[which.max(lookup(sample, "c"))] <= 2000 # TRUE
#' fractions <- list(a = 0.2, b = 0.1, c = 0.3, d = 0.4)
#' sample <- sampleByKey(pairs, FALSE, fractions, 1618L) # Key "d" will be ignored
#' fractions <- list(a = 0.2, b = 0.1)
#' sample <- sampleByKey(pairs, FALSE, fractions, 1618L) # KeyError: "c"
#'}
#' @rdname sampleByKey
#' @aliases sampleByKey,RDD-method
#' @noRd
setMethod("sampleByKey",
          signature(x = "RDD", withReplacement = "logical",
                    fractions = "vector", seed = "integer"),
          function(x, withReplacement, fractions, seed) {

            for (elem in fractions) {
              if (elem < 0.0) {
                stop(paste("Negative fraction value ", fractions[which(fractions == elem)]))
              }
            }

            # The sampler: takes a partition and returns its sampled version.
            samplingFunc <- function(partIndex, part) {
              set.seed(bitwXor(seed, partIndex))
              res <- vector("list", length(part))
              len <- 0

              # mixing because the initial seeds are close to each other
              stats::runif(10)

              for (elem in part) {
                if (elem[[1]] %in% names(fractions)) {
                  frac <- as.numeric(fractions[which(elem[[1]] == names(fractions))])
                  if (withReplacement) {
                    count <- stats::rpois(1, frac)
                    if (count > 0) {
                      res[ (len + 1) : (len + count) ] <- rep(list(elem), count)
                      len <- len + count
                    }
                  } else {
                    if (stats::runif(1) < frac) {
                      len <- len + 1
                      res[[len]] <- elem
                    }
                  }
                } else {
                  stop("KeyError: \"", elem[[1]], "\"")
                }
              }

              # TODO(zongheng): look into the performance of the current
              # implementation. Look into some iterator package? Note that
              # Scala avoids many calls to creating an empty list and PySpark
              # similarly achieves this using `yield'. (duplicated from sampleRDD)
              if (len > 0) {
                res[1:len]
              } else {
                list()
              }
            }

            lapplyPartitionsWithIndex(x, samplingFunc)
          })
