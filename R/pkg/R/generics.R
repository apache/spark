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

############ RDD Actions and Transformations ############

# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })

#' @rdname cache-methods
#' @export
setGeneric("cache", function(x) { standardGeneric("cache") })

#' @rdname persist
#' @export
setGeneric("persist", function(x, newLevel) { standardGeneric("persist") })

#' @rdname unpersist-methods
#' @export
setGeneric("unpersist", function(x, ...) { standardGeneric("unpersist") })

#' @rdname checkpoint-methods
#' @export
setGeneric("checkpoint", function(x) { standardGeneric("checkpoint") })

#' @rdname numPartitions
#' @export
setGeneric("numPartitions", function(x) { standardGeneric("numPartitions") })

#' @rdname collect-methods
#' @export
setGeneric("collect", function(x, ...) { standardGeneric("collect") })

#' @rdname collect-methods
#' @export
setGeneric("collectPartition",
           function(x, partitionId) {
             standardGeneric("collectPartition")
           })

#' @rdname collect-methods
#' @export
setGeneric("collectAsMap", function(x) { standardGeneric("collectAsMap") })

#' @rdname count
#' @export
setGeneric("count", function(x) { standardGeneric("count") })

#' @rdname countByValue
#' @export
setGeneric("countByValue", function(x) { standardGeneric("countByValue") })

#' @rdname lapply
#' @export
setGeneric("map", function(X, FUN) {
  standardGeneric("map") })


#' @rdname flatMap
#' @export
setGeneric("flatMap", function(X, FUN) {
  standardGeneric("flatMap") })

#' @rdname lapplyPartition
#' @export
setGeneric("lapplyPartition", function(X, FUN) {
  standardGeneric("lapplyPartition") })

#' @rdname lapplyPartition
#' @export
setGeneric("mapPartitions", function(X, FUN) {
  standardGeneric("mapPartitions") })

#' @rdname lapplyPartitionsWithIndex
#' @export
setGeneric("lapplyPartitionsWithIndex", function(X, FUN) {
  standardGeneric("lapplyPartitionsWithIndex") })

#' @rdname lapplyPartitionsWithIndex
#' @export
setGeneric("mapPartitionsWithIndex", function(X, FUN) {
  standardGeneric("mapPartitionsWithIndex") })

#' @rdname filterRDD
#' @export
setGeneric("filterRDD", 
           function(x, f) { standardGeneric("filterRDD") })

#' @rdname reduce
#' @export
setGeneric("reduce", function(x, func) { standardGeneric("reduce") })

#' @rdname maximum
#' @export
setGeneric("maximum", function(x) { standardGeneric("maximum") })

#' @rdname minimum
#' @export
setGeneric("minimum", function(x) { standardGeneric("minimum") })

#' @rdname foreach
#' @export
setGeneric("foreach", function(x, func) { standardGeneric("foreach") })

#' @rdname foreach
#' @export
setGeneric("foreachPartition", 
           function(x, func) { standardGeneric("foreachPartition") })

#' @rdname take
#' @export
setGeneric("take", function(x, num) { standardGeneric("take") })

#' @rdname distinct
#' @export
setGeneric("distinct",
           function(x, numPartitions = 1L) { standardGeneric("distinct") })

#' @rdname sampleRDD
#' @export
setGeneric("sampleRDD",
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

#' @rdname takeSample
#' @export
setGeneric("takeSample",
           function(x, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })

#' @rdname keyBy
#' @export
setGeneric("keyBy", function(x, func) { standardGeneric("keyBy") })

#' @rdname repartition
#' @seealso coalesce
#' @export
setGeneric("repartition", function(x, numPartitions) { standardGeneric("repartition") })

#' @rdname coalesce
#' @seealso repartition
#' @export
setGeneric("coalesce", function(x, numPartitions, ...) { standardGeneric("coalesce") })

#' @rdname saveAsObjectFile
#' @seealso objectFile
#' @export
setGeneric("saveAsObjectFile", function(x, path) { standardGeneric("saveAsObjectFile") })

#' @rdname saveAsTextFile
#' @export
setGeneric("saveAsTextFile", function(x, path) { standardGeneric("saveAsTextFile") })

#' @rdname sortBy
#' @export
setGeneric("sortBy", function(x,
                              func,
                              ascending = TRUE,
                              numPartitions = 1L) {
  standardGeneric("sortBy")
})

#' @rdname takeOrdered
#' @export
setGeneric("takeOrdered", function(x, num) { standardGeneric("takeOrdered") })

#' @rdname top
#' @export
setGeneric("top", function(x, num) { standardGeneric("top") })

#' @rdname fold
#' @seealso reduce
#' @export
setGeneric("fold", function(x, zeroValue, op) { standardGeneric("fold") })

#' @rdname aggregateRDD
#' @seealso reduce
#' @export
setGeneric("aggregateRDD", function(x, zeroValue, seqOp, combOp) { standardGeneric("aggregateRDD") })

#' @rdname pipeRDD
#' @export
setGeneric("pipeRDD", function(x, command, env = list()) { 
  standardGeneric("pipeRDD") 
})

#' @rdname name
#' @export
setGeneric("name", function(x) { standardGeneric("name") })

#' @rdname setName
#' @export
setGeneric("setName", function(x, name) { standardGeneric("setName") })

#' @rdname zipRDD
#' @export
setGeneric("zipRDD", function(x, other) { standardGeneric("zipRDD") })

#' @rdname zipWithUniqueId
#' @seealso zipWithIndex
#' @export
setGeneric("zipWithUniqueId", function(x) { standardGeneric("zipWithUniqueId") })

#' @rdname zipWithIndex
#' @seealso zipWithUniqueId
#' @export
setGeneric("zipWithIndex", function(x) { standardGeneric("zipWithIndex") })


############ Binary Functions #############


#' @rdname unionRDD
#' @export
setGeneric("unionRDD", function(x, y) { standardGeneric("unionRDD") })

#' @rdname lookup
#' @export
setGeneric("lookup", function(x, key) { standardGeneric("lookup") })

#' @rdname countByKey
#' @export
setGeneric("countByKey", function(x) { standardGeneric("countByKey") })

#' @rdname keys
#' @export
setGeneric("keys", function(x) { standardGeneric("keys") })

#' @rdname values
#' @export
setGeneric("values", function(x) { standardGeneric("values") })

#' @rdname mapValues
#' @export
setGeneric("mapValues", function(X, FUN) { standardGeneric("mapValues") })

#' @rdname flatMapValues
#' @export
setGeneric("flatMapValues", function(X, FUN) { standardGeneric("flatMapValues") })


############ Shuffle Functions ############


#' @rdname partitionBy
#' @export
setGeneric("partitionBy",
           function(x, numPartitions, ...) {
             standardGeneric("partitionBy")
           })

#' @rdname groupByKey
#' @seealso reduceByKey
#' @export
setGeneric("groupByKey",
           function(x, numPartitions) {
             standardGeneric("groupByKey")
           })

#' @rdname reduceByKey
#' @seealso groupByKey
#' @export
setGeneric("reduceByKey",
           function(x, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })

#' @rdname reduceByKeyLocally
#' @seealso reduceByKey
#' @export
setGeneric("reduceByKeyLocally",
           function(x, combineFunc) {
             standardGeneric("reduceByKeyLocally")
           })

#' @rdname combineByKey
#' @seealso groupByKey, reduceByKey
#' @export
setGeneric("combineByKey",
           function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

#' @rdname aggregateByKey
#' @seealso foldByKey, combineByKey
#' @export
setGeneric("aggregateByKey",
           function(x, zeroValue, seqOp, combOp, numPartitions) {
             standardGeneric("aggregateByKey")
           })

#' @rdname foldByKey
#' @seealso aggregateByKey, combineByKey
#' @export
setGeneric("foldByKey",
           function(x, zeroValue, func, numPartitions) {
             standardGeneric("foldByKey")
           })

#' @rdname join-methods
#' @export
setGeneric("join", function(x, y, ...) { standardGeneric("join") })

#' @rdname join-methods
#' @export
setGeneric("leftOuterJoin", function(x, y, numPartitions) { standardGeneric("leftOuterJoin") })

#' @rdname join-methods
#' @export
setGeneric("rightOuterJoin", function(x, y, numPartitions) { standardGeneric("rightOuterJoin") })

#' @rdname join-methods
#' @export
setGeneric("fullOuterJoin", function(x, y, numPartitions) { standardGeneric("fullOuterJoin") })

#' @rdname cogroup
#' @export
setGeneric("cogroup", 
           function(..., numPartitions) { standardGeneric("cogroup") },
           signature = "...")

#' @rdname sortByKey
#' @export
setGeneric("sortByKey", function(x,
                                 ascending = TRUE,
                                 numPartitions = 1L) {
  standardGeneric("sortByKey")
})



############ Broadcast Variable Methods ############


#' @rdname broadcast
#' @export
setGeneric("value", function(bcast) { standardGeneric("value") })


################### Column Methods ########################

#' @rdname column
#' @export
setGeneric("asc", function(x) { standardGeneric("asc") })

#' @rdname column
#' @export
setGeneric("desc", function(x) { standardGeneric("desc") })

#' @rdname column
#' @export
setGeneric("avg", function(x, ...) { standardGeneric("avg") })

#' @rdname column
#' @export
setGeneric("last", function(x) { standardGeneric("last") })

#' @rdname column
#' @export
setGeneric("lower", function(x) { standardGeneric("lower") })

#' @rdname column
#' @export
setGeneric("upper", function(x) { standardGeneric("upper") })

#' @rdname column
#' @export
setGeneric("like", function(x, ...) { standardGeneric("like") })

#' @rdname column
#' @export
setGeneric("rlike", function(x, ...) { standardGeneric("rlike") })

#' @rdname column
#' @export
setGeneric("startsWith", function(x, ...) { standardGeneric("startsWith") })

#' @rdname column
#' @export
setGeneric("endsWith", function(x, ...) { standardGeneric("endsWith") })

#' @rdname column
#' @export
setGeneric("getField", function(x, ...) { standardGeneric("getField") })

#' @rdname column
#' @export
setGeneric("getItem", function(x, ...) { standardGeneric("getItem") })

#' @rdname column
#' @export
setGeneric("contains", function(x, ...) { standardGeneric("contains") })

#' @rdname column
#' @export
setGeneric("isNull", function(x) { standardGeneric("isNull") })

#' @rdname column
#' @export
setGeneric("isNotNull", function(x) { standardGeneric("isNotNull") })

#' @rdname column
#' @export
setGeneric("sumDistinct", function(x) { standardGeneric("sumDistinct") })

#' @rdname column
#' @export
setGeneric("cast", function(x, dataType) { standardGeneric("cast") })

#' @rdname column
#' @export
setGeneric("approxCountDistinct", function(x, ...) { standardGeneric("approxCountDistinct") })

#' @rdname column
#' @export
setGeneric("countDistinct", function(x, ...) { standardGeneric("countDistinct") })


