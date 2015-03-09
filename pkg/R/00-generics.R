############ RDD Actions and Transformations ############

#' Persist an RDD
#'
#' Persist this RDD with the default storage level (MEMORY_ONLY).
#'
#' @param x The RDD to cache
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd)
#'}
setGeneric("cache", function(x) { standardGeneric("cache") })

#' Persist an RDD
#'
#' Persist this RDD with the specified storage level. For details of the
#' supported storage levels, refer to
#' http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence.
#'
#' @param x The RDD to persist
#' @param newLevel The new storage level to be assigned
#' @rdname persist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' persist(rdd, "MEMORY_AND_DISK")
#'}
setGeneric("persist", function(x, newLevel) { standardGeneric("persist") })

#' Unpersist an RDD
#'
#' Mark the RDD as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param rdd The RDD to unpersist
#' @rdname unpersist-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd) # rdd@@env$isCached == TRUE
#' unpersist(rdd) # rdd@@env$isCached == FALSE
#'}
setGeneric("unpersist", function(x) { standardGeneric("unpersist") })

#' Checkpoint an RDD
#'
#' Mark this RDD for checkpointing. It will be saved to a file inside the
#' checkpoint directory set with setCheckpointDir() and all references to its
#' parent RDDs will be removed. This function must be called before any job has
#' been executed on this RDD. It is strongly recommended that this RDD is
#' persisted in memory, otherwise saving it on a file will require recomputation.
#'
#' @param rdd The RDD to checkpoint
#' @rdname checkpoint-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' setCheckpointDir(sc, "checkpoints")
#' rdd <- parallelize(sc, 1:10, 2L)
#' checkpoint(rdd)
#'}
setGeneric("checkpoint", function(x) { standardGeneric("checkpoint") })

#' Gets the number of partitions of an RDD
#'
#' @param x A RDD.
#' @return the number of partitions of rdd as an integer.
#' @rdname numPartitions
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' numPartitions(rdd)  # 2L
#'}
setGeneric("numPartitions", function(x) { standardGeneric("numPartitions") })

#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @param x The RDD to collect
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
setGeneric("collect", function(x, ...) { standardGeneric("collect") })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectPartition} returns a list that contains all of the elements
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
setGeneric("collectPartition",
           function(x, partitionId) {
             standardGeneric("collectPartition")
           })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectAsMap} returns a named list as a map that contains all of the elements
#' in a key-value pair RDD. 
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)), 2L)
#' collectAsMap(rdd) # list(`1` = 2, `3` = 4)
#'}
setGeneric("collectAsMap", function(x) { standardGeneric("collectAsMap") })

#' Return the number of elements in the RDD.
#'
#' @param x The RDD to count
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
setGeneric("count", function(x) { standardGeneric("count") })

#' Return the count of each unique value in this RDD as a list of
#' (value, count) pairs.
#'
#' Same as countByValue in Spark.
#'
#' @param x The RDD to count
#' @return list of (value, count) pairs, where count is number of each unique
#' value in rdd.
#' @rdname countByValue
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,3,2,1))
#' countByValue(rdd) # (1,2L), (2,2L), (3,1L)
#'}
setGeneric("countByValue", function(x) { standardGeneric("countByValue") })

#' @rdname lapply
#' @export
setGeneric("map", function(X, FUN) {
  standardGeneric("map") })

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

#' mapPartitions is the same as lapplyPartition.
#'
#' @rdname lapplyPartition
#' @export
setGeneric("mapPartitions", function(X, FUN) {
  standardGeneric("mapPartitions") })

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
#' @export
setGeneric("mapPartitionsWithIndex", function(X, FUN) {
  standardGeneric("mapPartitionsWithIndex") })

#' This function returns a new RDD containing only the elements that satisfy
#' a predicate (i.e. returning TRUE in a given logical function).
#' The same as `filter()' in Spark.
#'
#' @param x The RDD to be filtered.
#' @param f A unary predicate function.
#' @rdname filterRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' unlist(collect(filterRDD(rdd, function (x) { x < 3 }))) # c(1, 2)
#'}
setGeneric("filterRDD", 
           function(x, f) { standardGeneric("filterRDD") })

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
setGeneric("reduce", function(x, func) { standardGeneric("reduce") })

#' Get the maximum element of an RDD.
#'
#' @param x The RDD to get the maximum element from
#' @export
#' @rdname maximum
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' maximum(rdd) # 10
#'}
setGeneric("maximum", function(x) { standardGeneric("maximum") })

#' Get the minimum element of an RDD.
#'
#' @param x The RDD to get the minimum element from
#' @export
#' @rdname minimum
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' minimum(rdd) # 1
#'}
setGeneric("minimum", function(x) { standardGeneric("minimum") })

#' Applies a function to all elements in an RDD, and force evaluation.
#'
#' @param x The RDD to apply the function
#' @param func The function to be applied.
#' @return invisible NULL.
#' @export
#' @rdname foreach
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreach(rdd, function(x) { save(x, file=...) })
#'}
setGeneric("foreach", function(x, func) { standardGeneric("foreach") })

#' Applies a function to each partition in an RDD, and force evaluation.
#'
#' @export
#' @rdname foreach
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreachPartition(rdd, function(part) { save(part, file=...); NULL })
#'}
setGeneric("foreachPartition", 
           function(x, func) { standardGeneric("foreachPartition") })

#' Take elements from an RDD.
#'
#' This function takes the first NUM elements in the RDD and
#' returns them in a list.
#'
#' @param x The RDD to take elements from
#' @param num Number of elements to take
#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' take(rdd, 2L) # list(1, 2)
#'}
setGeneric("take", function(x, num) { standardGeneric("take") })

#' Removes the duplicates from RDD.
#'
#' This function returns a new RDD containing the distinct elements in the
#' given RDD. The same as `distinct()' in Spark.
#'
#' @param x The RDD to remove duplicates from.
#' @param numPartitions Number of partitions to create.
#' @rdname distinct
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,2,3,3,3))
#' sort(unlist(collect(distinct(rdd)))) # c(1, 2, 3)
#'}
setGeneric("distinct",
           function(x, numPartitions) { standardGeneric("distinct") })

#' Return an RDD that is a sampled subset of the given RDD.
#'
#' The same as `sample()' in Spark. (We rename it due to signature
#' inconsistencies with the `sample()' function in R's base package.)
#'
#' @param x The RDD to sample elements from
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
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

#' Return a list of the elements that are a sampled subset of the given RDD.
#'
#' @param x The RDD to sample elements from
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
           function(x, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })

#' Creates tuples of the elements in this RDD by applying a function.
#'
#' @param x The RDD.
#' @param func The function to be applied.
#' @rdname keyBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3))
#' collect(keyBy(rdd, function(x) { x*x })) # list(list(1, 1), list(4, 2), list(9, 3))
#'}
setGeneric("keyBy", function(x, func) { standardGeneric("keyBy") })

#' Return a new RDD that has exactly numPartitions partitions.
#' Can increase or decrease the level of parallelism in this RDD. Internally,
#' this uses a shuffle to redistribute data.
#' If you are decreasing the number of partitions in this RDD, consider using
#' coalesce, which can avoid performing a shuffle.
#'
#' @param x The RDD.
#' @param numPartitions Number of partitions to create.
#' @rdname repartition
#' @seealso coalesce
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5, 6, 7), 4L)
#' numPartitions(rdd)                   # 4
#' numPartitions(repartition(rdd, 2L))  # 2
#'}
setGeneric("repartition", function(x, numPartitions) { standardGeneric("repartition") })

#' Return a new RDD that is reduced into numPartitions partitions.
#'
#' @param x The RDD.
#' @param numPartitions Number of partitions to create.
#' @rdname coalesce
#' @seealso repartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5), 3L)
#' numPartitions(rdd)               # 3
#' numPartitions(coalesce(rdd, 1L)) # 1
#'}
setGeneric("coalesce", function(x, numPartitions, ...) { standardGeneric("coalesce") })

#' Save this RDD as a SequenceFile of serialized objects.
#'
#' @param x The RDD to save
#' @param path The directory where the file is saved
#' @rdname saveAsObjectFile
#' @seealso objectFile
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsObjectFile(rdd, "/tmp/sparkR-tmp")
#'}
setGeneric("saveAsObjectFile", function(x, path) { standardGeneric("saveAsObjectFile") })

#' Save this RDD as a text file, using string representations of elements.
#'
#' @param x The RDD to save
#' @param path The directory where the splits of the text file are saved
#' @rdname saveAsTextFile
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsTextFile(rdd, "/tmp/sparkR-tmp")
#'}
setGeneric("saveAsTextFile", function(x, path) { standardGeneric("saveAsTextFile") })

#' Sort an RDD by the given key function.
#'
#' @param x An RDD to be sorted.
#' @param func A function used to compute the sort key for each element.
#' @param ascending A flag to indicate whether the sorting is ascending or descending.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where all elements are sorted.
#' @rdname sortBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(3, 2, 1))
#' collect(sortBy(rdd, function(x) { x })) # list (1, 2, 3)
#'}
setGeneric("sortBy", function(x,
                              func,
                              ascending = TRUE,
                              numPartitions = 1L) {
  standardGeneric("sortBy")
})

#' Returns the first N elements from an RDD in ascending order.
#'
#' @param x An RDD.
#' @param num Number of elements to return.
#' @return The first N elements from the RDD in ascending order.
#' @rdname takeOrdered
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' takeOrdered(rdd, 6L) # list(1, 2, 3, 4, 5, 6)
#'}
setGeneric("takeOrdered", function(x, num) { standardGeneric("takeOrdered") })

#' Returns the top N elements from an RDD.
#'
#' @param x An RDD.
#' @param num Number of elements to return.
#' @return The top N elements from the RDD.
#' @rdname top
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' top(rdd, 6L) # list(10, 9, 7, 6, 5, 4)
#'}
setGeneric("top", function(x, num) { standardGeneric("top") })

#' Fold an RDD using a given associative function and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using a given associative function and a neutral "zero value".
#' 
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param op An associative function for the folding operation.
#' @return The folding result.
#' @rdname fold
#' @seealso reduce
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5))
#' fold(rdd, 0, "+") # 15
#'}
setGeneric("fold", function(x, zeroValue, op) { standardGeneric("fold") })

#' Aggregate an RDD using the given combine functions and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using given combine functions and a neutral "zero value".
#' 
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param seqOp A function to aggregate the RDD elements. It may return a different
#'              result type from the type of the RDD elements.
#' @param combOp A function to aggregate results of seqOp.
#' @return The aggregation result.
#' @rdname aggregateRDD
#' @seealso reduce
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4))
#' zeroValue <- list(0, 0)
#' seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
#' combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
#' aggregateRDD(rdd, zeroValue, seqOp, combOp) # list(10, 4)
#'}
setGeneric("aggregateRDD", function(x, zeroValue, seqOp, combOp) { standardGeneric("aggregateRDD") })

#' Pipes elements to a forked external process.
#'
#' The same as 'pipe()' in Spark.
#'
#' @param x The RDD whose elements are piped to the forked external process.
#' @param command The command to fork an external process.
#' @param env A named list to set environment variables of the external process.
#' @return A new RDD created by piping all elements to a forked external process.
#' @rdname pipeRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' collect(pipeRDD(rdd, "more")
#' Output: c("1", "2", ..., "10")
#'}
setGeneric("pipeRDD", function(x, command, env = list()) { 
  standardGeneric("pipeRDD") 
})

# TODO: Consider caching the name in the RDD's environment
#' Return an RDD's name.
#'
#' @param x The RDD whose name is returned.
#' @rdname name
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' name(rdd) # NULL (if not set before)
#'}
setGeneric("name", function(x) { standardGeneric("name") })

#' Set an RDD's name.
#'
#' @param x The RDD whose name is to be set.
#' @param name The RDD name to be set.
#' @return a new RDD renamed.
#' @rdname setName
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' setName(rdd, "myRDD")
#' name(rdd) # "myRDD"
#'}
setGeneric("setName", function(x, name) { standardGeneric("setName") })

#' Zip an RDD with generated unique Long IDs.
#'
#' Items in the kth partition will get ids k, n+k, 2*n+k, ..., where
#' n is the number of partitions. So there may exist gaps, but this
#' method won't trigger a spark job, which is different from
#' zipWithIndex.
#'
#' @param x An RDD to be zipped.
#' @return An RDD with zipped items.
#' @rdname zipWithUniqueId
#' @seealso zipWithIndex
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
#' collect(zipWithUniqueId(rdd)) 
#' # list(list("a", 0), list("b", 3), list("c", 1), list("d", 4), list("e", 2))
#'}
setGeneric("zipWithUniqueId", function(x) { standardGeneric("zipWithUniqueId") })

#' Zip an RDD with its element indices.
#'
#' The ordering is first based on the partition index and then the
#' ordering of items within each partition. So the first item in
#' the first partition gets index 0, and the last item in the last
#' partition receives the largest index.
#'
#' This method needs to trigger a Spark job when this RDD contains
#' more than one partition.
#'
#' @param x An RDD to be zipped.
#' @return An RDD with zipped items.
#' @rdname zipWithIndex
#' @seealso zipWithUniqueId
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
#' collect(zipWithIndex(rdd))
#' # list(list("a", 0), list("b", 1), list("c", 2), list("d", 3), list("e", 4))
#'}
setGeneric("zipWithIndex", function(x) { standardGeneric("zipWithIndex") })


############ Binary Functions #############


#' Return the union RDD of two RDDs.
#' The same as union() in Spark.
#'
#' @param x An RDD.
#' @param y An RDD.
#' @return a new RDD created by performing the simple union (witout removing
#' duplicates) of two input RDDs.
#' @rdname unionRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' unionRDD(rdd, rdd) # 1, 2, 3, 1, 2, 3
#'}
setGeneric("unionRDD", function(x, y) { standardGeneric("unionRDD") })

#' Look up elements of a key in an RDD
#'
#' @description
#' \code{lookup} returns a list of values in this RDD for key key.
#'
#' @param x The RDD to collect
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
setGeneric("lookup", function(x, key) { standardGeneric("lookup") })

#' Count the number of elements for each key, and return the result to the
#' master as lists of (key, count) pairs.
#'
#' Same as countByKey in Spark.
#'
#' @param x The RDD to count keys.
#' @return list of (key, count) pairs, where count is number of each key in rdd.
#' @rdname countByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(c("a", 1), c("b", 1), c("a", 1)))
#' countByKey(rdd) # ("a", 2L), ("b", 1L)
#'}
setGeneric("countByKey", function(x) { standardGeneric("countByKey") })

#' Return an RDD with the keys of each tuple.
#'
#' @param x The RDD from which the keys of each tuple is returned.
#' @rdname keys
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(keys(rdd)) # list(1, 3)
#'}
setGeneric("keys", function(x) { standardGeneric("keys") })

#' Return an RDD with the values of each tuple.
#'
#' @param x The RDD from which the values of each tuple is returned.
#' @rdname values
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(values(rdd)) # list(2, 4)
#'}
setGeneric("values", function(x) { standardGeneric("values") })

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
           function(x, numPartitions, ...) {
             standardGeneric("partitionBy")
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
           function(x, numPartitions) {
             standardGeneric("groupByKey")
           })

#' Merge values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function.
#'
#' @param x The RDD to reduce by key. Should be an RDD where each element is
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
           function(x, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })

#' Merge values by key locally
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function, but return the
#' results immediately to the driver as an R list.
#'
#' @param x The RDD to reduce by key. Should be an RDD where each element is
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
           function(x, combineFunc) {
             standardGeneric("reduceByKeyLocally")
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
           function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
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
#' @rdname aggregateByKey
#' @seealso foldByKey, combineByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))
#' zeroValue <- list(0, 0)
#' seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
#' combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
#' aggregateByKey(rdd, zeroValue, seqOp, combOp, 2L) 
#'   # list(list(1, list(3, 2)), list(2, list(7, 2)))
#'}
setGeneric("aggregateByKey",
           function(x, zeroValue, seqOp, combOp, numPartitions) {
             standardGeneric("aggregateByKey")
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
#' @rdname foldByKey
#' @seealso aggregateByKey, combineByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))
#' foldByKey(rdd, 0, "+", 2L) # list(list(1, 3), list(2, 7))
#'}
setGeneric("foldByKey",
           function(x, zeroValue, func, numPartitions) {
             standardGeneric("foldByKey")
           })

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
#' @rdname join-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' join(rdd1, rdd2, 2L) # list(list(1, list(1, 2)), list(1, list(1, 3))
#'}
setGeneric("join", function(x, y, numPartitions) { standardGeneric("join") })

#' Left outer join two RDDs
#'
#' @description
#' \code{leftouterjoin} This function left-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in x, the resulting RDD will either contain 
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
setGeneric("leftOuterJoin", function(x, y, numPartitions) { standardGeneric("leftOuterJoin") })

#' Right outer join two RDDs
#'
#' @description
#' \code{rightouterjoin} This function right-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param x An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param y An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, w) in y, the resulting RDD will either contain
#'         all pairs (k, (v, w)) for (k, v) in x, or the pair (k, (NULL, w))
#'         if no elements in x have key k.
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
setGeneric("rightOuterJoin", function(x, y, numPartitions) { standardGeneric("rightOuterJoin") })

#' Full outer join two RDDs
#'
#' @description
#' \code{fullouterjoin} This function full-outer-joins two RDDs where every element is of the form list(K, V). 
#' The key types of the two RDDs should be the same.
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
setGeneric("fullOuterJoin", function(x, y, numPartitions) { standardGeneric("fullOuterJoin") })

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

#' Sort a (k, v) pair RDD by k.
#'
#' @param x A (k, v) pair RDD to be sorted.
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
setGeneric("sortByKey", function(x,
                                 ascending = TRUE,
                                 numPartitions = 1L) {
  standardGeneric("sortByKey")
})

