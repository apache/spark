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

# @rdname aggregateRDD
# @seealso reduce
setGeneric("aggregateRDD",
           function(x, zeroValue, seqOp, combOp) { standardGeneric("aggregateRDD") })

setGeneric("cacheRDD", function(x) { standardGeneric("cacheRDD") })

# @rdname coalesce
# @seealso repartition
setGeneric("coalesceRDD", function(x, numPartitions, ...) { standardGeneric("coalesceRDD") })

# @rdname checkpoint-methods
setGeneric("checkpointRDD", function(x) { standardGeneric("checkpointRDD") })

setGeneric("collectRDD", function(x, ...) { standardGeneric("collectRDD") })

# @rdname collect-methods
setGeneric("collectAsMap", function(x) { standardGeneric("collectAsMap") })

# @rdname collect-methods
setGeneric("collectPartition",
           function(x, partitionId) {
             standardGeneric("collectPartition")
           })

setGeneric("countRDD", function(x) { standardGeneric("countRDD") })

setGeneric("lengthRDD", function(x) { standardGeneric("lengthRDD") })

# @rdname countByValue
setGeneric("countByValue", function(x) { standardGeneric("countByValue") })

# @rdname crosstab
setGeneric("crosstab", function(x, col1, col2) { standardGeneric("crosstab") })

# @rdname freqItems
setGeneric("freqItems", function(x, cols, support = 0.01) { standardGeneric("freqItems") })

# @rdname approxQuantile
setGeneric("approxQuantile",
           function(x, cols, probabilities, relativeError) {
             standardGeneric("approxQuantile")
           })

setGeneric("distinctRDD", function(x, numPartitions = 1) { standardGeneric("distinctRDD") })

# @rdname filterRDD
setGeneric("filterRDD", function(x, f) { standardGeneric("filterRDD") })

setGeneric("firstRDD", function(x, ...) { standardGeneric("firstRDD") })

# @rdname flatMap
setGeneric("flatMap", function(X, FUN) { standardGeneric("flatMap") })

# @rdname fold
# @seealso reduce
setGeneric("fold", function(x, zeroValue, op) { standardGeneric("fold") })

setGeneric("foreach", function(x, func) { standardGeneric("foreach") })

setGeneric("foreachPartition", function(x, func) { standardGeneric("foreachPartition") })

# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })

# @rdname glom
setGeneric("glom", function(x) { standardGeneric("glom") })

# @rdname histogram
setGeneric("histogram", function(df, col, nbins=10) { standardGeneric("histogram") })

setGeneric("joinRDD", function(x, y, ...) { standardGeneric("joinRDD") })

# @rdname keyBy
setGeneric("keyBy", function(x, func) { standardGeneric("keyBy") })

setGeneric("lapplyPartition", function(X, FUN) { standardGeneric("lapplyPartition") })

setGeneric("lapplyPartitionsWithIndex",
           function(X, FUN) {
             standardGeneric("lapplyPartitionsWithIndex")
           })

setGeneric("map", function(X, FUN) { standardGeneric("map") })

setGeneric("mapPartitions", function(X, FUN) { standardGeneric("mapPartitions") })

setGeneric("mapPartitionsWithIndex",
           function(X, FUN) { standardGeneric("mapPartitionsWithIndex") })

# @rdname maximum
setGeneric("maximum", function(x) { standardGeneric("maximum") })

# @rdname minimum
setGeneric("minimum", function(x) { standardGeneric("minimum") })

# @rdname sumRDD
setGeneric("sumRDD", function(x) { standardGeneric("sumRDD") })

# @rdname name
setGeneric("name", function(x) { standardGeneric("name") })

# @rdname getNumPartitionsRDD
setGeneric("getNumPartitionsRDD", function(x) { standardGeneric("getNumPartitionsRDD") })

# @rdname getNumPartitions
setGeneric("numPartitions", function(x) { standardGeneric("numPartitions") })

setGeneric("persistRDD", function(x, newLevel) { standardGeneric("persistRDD") })

# @rdname pipeRDD
setGeneric("pipeRDD", function(x, command, env = list()) { standardGeneric("pipeRDD")})

# @rdname pivot
setGeneric("pivot", function(x, colname, values = list()) { standardGeneric("pivot") })

# @rdname reduce
setGeneric("reduce", function(x, func) { standardGeneric("reduce") })

setGeneric("repartitionRDD", function(x, ...) { standardGeneric("repartitionRDD") })

# @rdname sampleRDD
setGeneric("sampleRDD",
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

# @rdname saveAsObjectFile
# @seealso objectFile
setGeneric("saveAsObjectFile", function(x, path) { standardGeneric("saveAsObjectFile") })

# @rdname saveAsTextFile
setGeneric("saveAsTextFile", function(x, path) { standardGeneric("saveAsTextFile") })

# @rdname setName
setGeneric("setName", function(x, name) { standardGeneric("setName") })

setGeneric("showRDD", function(object, ...) { standardGeneric("showRDD") })

# @rdname sortBy
setGeneric("sortBy",
           function(x, func, ascending = TRUE, numPartitions = 1) {
             standardGeneric("sortBy")
           })

setGeneric("takeRDD", function(x, num) { standardGeneric("takeRDD") })

# @rdname takeOrdered
setGeneric("takeOrdered", function(x, num) { standardGeneric("takeOrdered") })

# @rdname takeSample
setGeneric("takeSample",
           function(x, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })

# @rdname top
setGeneric("top", function(x, num) { standardGeneric("top") })

# @rdname unionRDD
setGeneric("unionRDD", function(x, y) { standardGeneric("unionRDD") })

setGeneric("unpersistRDD", function(x, ...) { standardGeneric("unpersistRDD") })

# @rdname zipRDD
setGeneric("zipRDD", function(x, other) { standardGeneric("zipRDD") })

# @rdname zipRDD
setGeneric("zipPartitions", function(..., func) { standardGeneric("zipPartitions") },
           signature = "...")

# @rdname zipWithIndex
# @seealso zipWithUniqueId
setGeneric("zipWithIndex", function(x) { standardGeneric("zipWithIndex") })

# @rdname zipWithUniqueId
# @seealso zipWithIndex
setGeneric("zipWithUniqueId", function(x) { standardGeneric("zipWithUniqueId") })


############ Binary Functions #############

# @rdname cartesian
setGeneric("cartesian", function(x, other) { standardGeneric("cartesian") })

# @rdname countByKey
setGeneric("countByKey", function(x) { standardGeneric("countByKey") })

# @rdname flatMapValues
setGeneric("flatMapValues", function(X, FUN) { standardGeneric("flatMapValues") })

# @rdname intersection
setGeneric("intersection",
           function(x, other, numPartitions = 1) {
             standardGeneric("intersection")
           })

# @rdname keys
setGeneric("keys", function(x) { standardGeneric("keys") })

# @rdname lookup
setGeneric("lookup", function(x, key) { standardGeneric("lookup") })

# @rdname mapValues
setGeneric("mapValues", function(X, FUN) { standardGeneric("mapValues") })

# @rdname sampleByKey
setGeneric("sampleByKey",
           function(x, withReplacement, fractions, seed) {
             standardGeneric("sampleByKey")
           })

# @rdname values
setGeneric("values", function(x) { standardGeneric("values") })


############ Shuffle Functions ############

# @rdname aggregateByKey
# @seealso foldByKey, combineByKey
setGeneric("aggregateByKey",
           function(x, zeroValue, seqOp, combOp, numPartitions) {
             standardGeneric("aggregateByKey")
           })

# @rdname cogroup
setGeneric("cogroup",
           function(..., numPartitions) {
             standardGeneric("cogroup")
           },
           signature = "...")

# @rdname combineByKey
# @seealso groupByKey, reduceByKey
setGeneric("combineByKey",
           function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

# @rdname foldByKey
# @seealso aggregateByKey, combineByKey
setGeneric("foldByKey",
           function(x, zeroValue, func, numPartitions) {
             standardGeneric("foldByKey")
           })

# @rdname join-methods
setGeneric("fullOuterJoin", function(x, y, numPartitions) { standardGeneric("fullOuterJoin") })

# @rdname groupByKey
# @seealso reduceByKey
setGeneric("groupByKey", function(x, numPartitions) { standardGeneric("groupByKey") })

# @rdname join-methods
setGeneric("join", function(x, y, ...) { standardGeneric("join") })

# @rdname join-methods
setGeneric("leftOuterJoin", function(x, y, numPartitions) { standardGeneric("leftOuterJoin") })

setGeneric("partitionByRDD", function(x, ...) { standardGeneric("partitionByRDD") })

# @rdname reduceByKey
# @seealso groupByKey
setGeneric("reduceByKey", function(x, combineFunc, numPartitions) { standardGeneric("reduceByKey")})

# @rdname reduceByKeyLocally
# @seealso reduceByKey
setGeneric("reduceByKeyLocally",
           function(x, combineFunc) {
             standardGeneric("reduceByKeyLocally")
           })

# @rdname join-methods
setGeneric("rightOuterJoin", function(x, y, numPartitions) { standardGeneric("rightOuterJoin") })

# @rdname sortByKey
setGeneric("sortByKey",
           function(x, ascending = TRUE, numPartitions = 1) {
             standardGeneric("sortByKey")
           })

# @rdname subtract
setGeneric("subtract",
           function(x, other, numPartitions = 1) {
             standardGeneric("subtract")
           })

# @rdname subtractByKey
setGeneric("subtractByKey",
           function(x, other, numPartitions = 1) {
             standardGeneric("subtractByKey")
           })


################### Broadcast Variable Methods #################

# @rdname broadcast
setGeneric("value", function(bcast) { standardGeneric("value") })


####################  SparkDataFrame Methods ########################

#' @param x a SparkDataFrame or GroupedData.
#' @param ... further arguments to be passed to or from other methods.
#' @return A SparkDataFrame.
#' @rdname summarize
setGeneric("agg", function(x, ...) { standardGeneric("agg") })

#' alias
#'
#' Returns a new SparkDataFrame or a Column with an alias set. Equivalent to SQL "AS" keyword.
#'
#' @name alias
#' @rdname alias
#' @param object x a SparkDataFrame or a Column
#' @param data new name to use
#' @return a SparkDataFrame or a Column
NULL

#' @rdname arrange
setGeneric("arrange", function(x, col, ...) { standardGeneric("arrange") })

#' @rdname as.data.frame
setGeneric("as.data.frame",
           function(x, row.names = NULL, optional = FALSE, ...) {
             standardGeneric("as.data.frame")
           })

# Do not document the generic because of signature changes across R versions
#' @noRd
setGeneric("attach")

#' @rdname cache
setGeneric("cache", function(x) { standardGeneric("cache") })

#' @rdname checkpoint
setGeneric("checkpoint", function(x, eager = TRUE) { standardGeneric("checkpoint") })

#' @rdname coalesce
#' @param x a SparkDataFrame.
#' @param ... additional argument(s).
setGeneric("coalesce", function(x, ...) { standardGeneric("coalesce") })

#' @rdname collect
setGeneric("collect", function(x, ...) { standardGeneric("collect") })

#' @param do.NULL currently not used.
#' @param prefix currently not used.
#' @rdname columns
setGeneric("colnames", function(x, do.NULL = TRUE, prefix = "col") { standardGeneric("colnames") })

#' @rdname columns
setGeneric("colnames<-", function(x, value) { standardGeneric("colnames<-") })

#' @rdname coltypes
setGeneric("coltypes", function(x) { standardGeneric("coltypes") })

#' @rdname coltypes
setGeneric("coltypes<-", function(x, value) { standardGeneric("coltypes<-") })

#' @rdname columns
setGeneric("columns", function(x) {standardGeneric("columns") })

#' @param x a GroupedData or Column.
#' @rdname count
setGeneric("count", function(x) { standardGeneric("count") })

#' @rdname cov
#' @param x a Column or a SparkDataFrame.
#' @param ... additional argument(s). If \code{x} is a Column, a Column
#'        should be provided. If \code{x} is a SparkDataFrame, two column names should
#'        be provided.
setGeneric("cov", function(x, ...) {standardGeneric("cov") })

#' @rdname corr
#' @param x a Column or a SparkDataFrame.
#' @param ... additional argument(s). If \code{x} is a Column, a Column
#'        should be provided. If \code{x} is a SparkDataFrame, two column names should
#'        be provided.
setGeneric("corr", function(x, ...) {standardGeneric("corr") })

#' @rdname cov
setGeneric("covar_samp", function(col1, col2) {standardGeneric("covar_samp") })

#' @rdname cov
setGeneric("covar_pop", function(col1, col2) {standardGeneric("covar_pop") })

#' @rdname createOrReplaceTempView
setGeneric("createOrReplaceTempView",
           function(x, viewName) {
             standardGeneric("createOrReplaceTempView")
           })

# @rdname crossJoin
setGeneric("crossJoin", function(x, y) { standardGeneric("crossJoin") })

#' @rdname cube
setGeneric("cube", function(x, ...) { standardGeneric("cube") })

#' @rdname dapply
setGeneric("dapply", function(x, func, schema) { standardGeneric("dapply") })

#' @rdname dapplyCollect
setGeneric("dapplyCollect", function(x, func) { standardGeneric("dapplyCollect") })

#' @param x a SparkDataFrame or GroupedData.
#' @param ... additional argument(s) passed to the method.
#' @rdname gapply
setGeneric("gapply", function(x, ...) { standardGeneric("gapply") })

#' @param x a SparkDataFrame or GroupedData.
#' @param ... additional argument(s) passed to the method.
#' @rdname gapplyCollect
setGeneric("gapplyCollect", function(x, ...) { standardGeneric("gapplyCollect") })

# @rdname getNumPartitions
setGeneric("getNumPartitions", function(x) { standardGeneric("getNumPartitions") })

#' @rdname describe
setGeneric("describe", function(x, col, ...) { standardGeneric("describe") })

#' @rdname distinct
setGeneric("distinct", function(x) { standardGeneric("distinct") })

#' @rdname drop
setGeneric("drop", function(x, ...) { standardGeneric("drop") })

#' @rdname dropDuplicates
setGeneric("dropDuplicates", function(x, ...) { standardGeneric("dropDuplicates") })

#' @rdname nafunctions
setGeneric("dropna",
           function(x, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
             standardGeneric("dropna")
           })

#' @rdname nafunctions
setGeneric("na.omit",
           function(object, ...) {
             standardGeneric("na.omit")
           })

#' @rdname dtypes
setGeneric("dtypes", function(x) { standardGeneric("dtypes") })

#' @rdname explain
#' @param x a SparkDataFrame or a StreamingQuery.
#' @param extended Logical. If extended is FALSE, prints only the physical plan.
#' @param ... further arguments to be passed to or from other methods.
setGeneric("explain", function(x, ...) { standardGeneric("explain") })

#' @rdname except
setGeneric("except", function(x, y) { standardGeneric("except") })

#' @rdname exceptAll
setGeneric("exceptAll", function(x, y) { standardGeneric("exceptAll") })

#' @rdname nafunctions
setGeneric("fillna", function(x, value, cols = NULL) { standardGeneric("fillna") })

#' @rdname filter
setGeneric("filter", function(x, condition) { standardGeneric("filter") })

#' @rdname first
setGeneric("first", function(x, ...) { standardGeneric("first") })

#' @rdname groupBy
setGeneric("group_by", function(x, ...) { standardGeneric("group_by") })

#' @rdname groupBy
setGeneric("groupBy", function(x, ...) { standardGeneric("groupBy") })

#' @rdname hint
setGeneric("hint", function(x, name, ...) { standardGeneric("hint") })

#' @rdname insertInto
setGeneric("insertInto", function(x, tableName, ...) { standardGeneric("insertInto") })

#' @rdname intersect
setGeneric("intersect", function(x, y) { standardGeneric("intersect") })

#' @rdname intersectAll
setGeneric("intersectAll", function(x, y) { standardGeneric("intersectAll") })

#' @rdname isLocal
setGeneric("isLocal", function(x) { standardGeneric("isLocal") })

#' @rdname isStreaming
setGeneric("isStreaming", function(x) { standardGeneric("isStreaming") })

#' @rdname limit
setGeneric("limit", function(x, num) {standardGeneric("limit") })

#' @rdname localCheckpoint
setGeneric("localCheckpoint", function(x, eager = TRUE) { standardGeneric("localCheckpoint") })

#' @rdname merge
setGeneric("merge")

#' @rdname mutate
setGeneric("mutate", function(.data, ...) {standardGeneric("mutate") })

#' @rdname orderBy
setGeneric("orderBy", function(x, col, ...) { standardGeneric("orderBy") })

#' @rdname persist
setGeneric("persist", function(x, newLevel) { standardGeneric("persist") })

#' @rdname printSchema
setGeneric("printSchema", function(x) { standardGeneric("printSchema") })

#' @rdname rename
setGeneric("rename", function(x, ...) { standardGeneric("rename") })

#' @rdname repartition
setGeneric("repartition", function(x, ...) { standardGeneric("repartition") })

#' @rdname repartitionByRange
setGeneric("repartitionByRange", function(x, ...) { standardGeneric("repartitionByRange") })

#' @rdname sample
setGeneric("sample",
           function(x, withReplacement = FALSE, fraction, seed) {
             standardGeneric("sample")
           })

#' @rdname rollup
setGeneric("rollup", function(x, ...) { standardGeneric("rollup") })

#' @rdname sample
setGeneric("sample_frac",
           function(x, withReplacement = FALSE, fraction, seed) { standardGeneric("sample_frac") })

#' @rdname sampleBy
setGeneric("sampleBy", function(x, col, fractions, seed) { standardGeneric("sampleBy") })

#' @rdname saveAsTable
setGeneric("saveAsTable", function(df, tableName, source = NULL, mode = "error", ...) {
  standardGeneric("saveAsTable")
})

setGeneric("str")

#' @rdname take
setGeneric("take", function(x, num) { standardGeneric("take") })

#' @rdname mutate
setGeneric("transform", function(`_data`, ...) {standardGeneric("transform") })

#' @rdname write.df
setGeneric("write.df", function(df, path = NULL, source = NULL, mode = "error", ...) {
  standardGeneric("write.df")
})

#' @rdname write.df
setGeneric("saveDF", function(df, path, source = NULL, mode = "error", ...) {
  standardGeneric("saveDF")
})

#' @rdname write.jdbc
setGeneric("write.jdbc", function(x, url, tableName, mode = "error", ...) {
  standardGeneric("write.jdbc")
})

#' @rdname write.json
setGeneric("write.json", function(x, path, ...) { standardGeneric("write.json") })

#' @rdname write.orc
setGeneric("write.orc", function(x, path, ...) { standardGeneric("write.orc") })

#' @rdname write.parquet
setGeneric("write.parquet", function(x, path, ...) {
  standardGeneric("write.parquet")
})

#' @rdname write.stream
setGeneric("write.stream", function(df, source = NULL, outputMode = NULL, ...) {
  standardGeneric("write.stream")
})

#' @rdname write.text
setGeneric("write.text", function(x, path, ...) { standardGeneric("write.text") })

#' @rdname schema
setGeneric("schema", function(x) { standardGeneric("schema") })

#' @rdname select
setGeneric("select", function(x, col, ...) { standardGeneric("select") })

#' @rdname selectExpr
setGeneric("selectExpr", function(x, expr, ...) { standardGeneric("selectExpr") })

#' @rdname showDF
setGeneric("showDF", function(x, ...) { standardGeneric("showDF") })

# @rdname storageLevel
setGeneric("storageLevel", function(x) { standardGeneric("storageLevel") })

#' @rdname subset
setGeneric("subset", function(x, ...) { standardGeneric("subset") })

#' @rdname summarize
setGeneric("summarize", function(x, ...) { standardGeneric("summarize") })

#' @rdname summary
setGeneric("summary", function(object, ...) { standardGeneric("summary") })

setGeneric("toJSON", function(x, ...) { standardGeneric("toJSON") })

setGeneric("toRDD", function(x) { standardGeneric("toRDD") })

#' @rdname union
setGeneric("union", function(x, y) { standardGeneric("union") })

#' @rdname unionAll
setGeneric("unionAll", function(x, y) { standardGeneric("unionAll") })

#' @rdname unionByName
setGeneric("unionByName", function(x, y) { standardGeneric("unionByName") })

#' @rdname unpersist
setGeneric("unpersist", function(x, ...) { standardGeneric("unpersist") })

#' @rdname filter
setGeneric("where", function(x, condition) { standardGeneric("where") })

#' @rdname with
setGeneric("with")

#' @rdname withColumn
setGeneric("withColumn", function(x, colName, col) { standardGeneric("withColumn") })

#' @rdname rename
setGeneric("withColumnRenamed",
           function(x, existingCol, newCol) { standardGeneric("withColumnRenamed") })

#' @rdname withWatermark
setGeneric("withWatermark", function(x, eventTime, delayThreshold) {
  standardGeneric("withWatermark")
})

#' @rdname write.df
setGeneric("write.df", function(df, path = NULL, ...) { standardGeneric("write.df") })

#' @rdname randomSplit
setGeneric("randomSplit", function(x, weights, seed) { standardGeneric("randomSplit") })

#' @rdname broadcast
setGeneric("broadcast", function(x) { standardGeneric("broadcast") })

###################### Column Methods ##########################

#' @rdname columnfunctions
setGeneric("asc", function(x) { standardGeneric("asc") })

#' @rdname between
setGeneric("between", function(x, bounds) { standardGeneric("between") })

#' @rdname cast
setGeneric("cast", function(x, dataType) { standardGeneric("cast") })

#' @rdname columnfunctions
#' @param x a Column object.
#' @param ... additional argument(s).
setGeneric("contains", function(x, ...) { standardGeneric("contains") })

#' @rdname columnfunctions
setGeneric("desc", function(x) { standardGeneric("desc") })

#' @rdname endsWith
setGeneric("endsWith", function(x, suffix) { standardGeneric("endsWith") })

#' @rdname columnfunctions
setGeneric("getField", function(x, ...) { standardGeneric("getField") })

#' @rdname columnfunctions
setGeneric("getItem", function(x, ...) { standardGeneric("getItem") })

#' @rdname columnfunctions
setGeneric("isNaN", function(x) { standardGeneric("isNaN") })

#' @rdname columnfunctions
setGeneric("isNull", function(x) { standardGeneric("isNull") })

#' @rdname columnfunctions
setGeneric("isNotNull", function(x) { standardGeneric("isNotNull") })

#' @rdname columnfunctions
setGeneric("like", function(x, ...) { standardGeneric("like") })

#' @rdname columnfunctions
setGeneric("rlike", function(x, ...) { standardGeneric("rlike") })

#' @rdname startsWith
setGeneric("startsWith", function(x, prefix) { standardGeneric("startsWith") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("when", function(condition, value) { standardGeneric("when") })

#' @rdname otherwise
setGeneric("otherwise", function(x, value) { standardGeneric("otherwise") })

#' @rdname over
setGeneric("over", function(x, window) { standardGeneric("over") })

#' @rdname eq_null_safe
setGeneric("%<=>%", function(x, value) { standardGeneric("%<=>%") })

###################### WindowSpec Methods ##########################

#' @rdname partitionBy
setGeneric("partitionBy", function(x, ...) { standardGeneric("partitionBy") })

#' @rdname rowsBetween
setGeneric("rowsBetween", function(x, start, end) { standardGeneric("rowsBetween") })

#' @rdname rangeBetween
setGeneric("rangeBetween", function(x, start, end) { standardGeneric("rangeBetween") })

#' @rdname windowPartitionBy
setGeneric("windowPartitionBy", function(col, ...) { standardGeneric("windowPartitionBy") })

#' @rdname windowOrderBy
setGeneric("windowOrderBy", function(col, ...) { standardGeneric("windowOrderBy") })

###################### Expression Function Methods ##########################

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("add_months", function(y, x) { standardGeneric("add_months") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("approx_count_distinct", function(x, ...) { standardGeneric("approx_count_distinct") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("approxCountDistinct", function(x, ...) { standardGeneric("approxCountDistinct") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_contains", function(x, value) { standardGeneric("array_contains") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_distinct", function(x) { standardGeneric("array_distinct") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_except", function(x, y) { standardGeneric("array_except") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_intersect", function(x, y) { standardGeneric("array_intersect") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_join", function(x, delimiter, ...) { standardGeneric("array_join") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_max", function(x) { standardGeneric("array_max") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_min", function(x) { standardGeneric("array_min") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_position", function(x, value) { standardGeneric("array_position") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_remove", function(x, value) { standardGeneric("array_remove") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_repeat", function(x, count) { standardGeneric("array_repeat") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_sort", function(x) { standardGeneric("array_sort") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("arrays_overlap", function(x, y) { standardGeneric("arrays_overlap") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_union", function(x, y) { standardGeneric("array_union") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("arrays_zip", function(x, ...) { standardGeneric("arrays_zip") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("ascii", function(x) { standardGeneric("ascii") })

#' @param x Column to compute on or a GroupedData object.
#' @param ... additional argument(s) when \code{x} is a GroupedData object.
#' @rdname avg
setGeneric("avg", function(x, ...) { standardGeneric("avg") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("base64", function(x) { standardGeneric("base64") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("bin", function(x) { standardGeneric("bin") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("bitwiseNOT", function(x) { standardGeneric("bitwiseNOT") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("bround", function(x, ...) { standardGeneric("bround") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("cbrt", function(x) { standardGeneric("cbrt") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("ceil", function(x) { standardGeneric("ceil") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("collect_list", function(x) { standardGeneric("collect_list") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("collect_set", function(x) { standardGeneric("collect_set") })

#' @rdname column
setGeneric("column", function(x) { standardGeneric("column") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("concat", function(x, ...) { standardGeneric("concat") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("concat_ws", function(sep, x, ...) { standardGeneric("concat_ws") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("conv", function(x, fromBase, toBase) { standardGeneric("conv") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("countDistinct", function(x, ...) { standardGeneric("countDistinct") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("crc32", function(x) { standardGeneric("crc32") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("create_array", function(x, ...) { standardGeneric("create_array") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("create_map", function(x, ...) { standardGeneric("create_map") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("hash", function(x, ...) { standardGeneric("hash") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("cume_dist", function(x = "missing") { standardGeneric("cume_dist") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("current_date", function(x = "missing") { standardGeneric("current_date") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("current_timestamp", function(x = "missing") { standardGeneric("current_timestamp") })


#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("datediff", function(y, x) { standardGeneric("datediff") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_add", function(y, x) { standardGeneric("date_add") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_format", function(y, x) { standardGeneric("date_format") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_sub", function(y, x) { standardGeneric("date_sub") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("date_trunc", function(format, x) { standardGeneric("date_trunc") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofmonth", function(x) { standardGeneric("dayofmonth") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofweek", function(x) { standardGeneric("dayofweek") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofyear", function(x) { standardGeneric("dayofyear") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("decode", function(x, charset) { standardGeneric("decode") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("dense_rank", function(x = "missing") { standardGeneric("dense_rank") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("element_at", function(x, extraction) { standardGeneric("element_at") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("encode", function(x, charset) { standardGeneric("encode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("explode", function(x) { standardGeneric("explode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("explode_outer", function(x) { standardGeneric("explode_outer") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("expr", function(x) { standardGeneric("expr") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("flatten", function(x) { standardGeneric("flatten") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("from_utc_timestamp", function(y, x) { standardGeneric("from_utc_timestamp") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("format_number", function(y, x) { standardGeneric("format_number") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("format_string", function(format, x, ...) { standardGeneric("format_string") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("from_json", function(x, schema, ...) { standardGeneric("from_json") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("from_csv", function(x, schema, ...) { standardGeneric("from_csv") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("from_unixtime", function(x, ...) { standardGeneric("from_unixtime") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("greatest", function(x, ...) { standardGeneric("greatest") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("grouping_bit", function(x) { standardGeneric("grouping_bit") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("grouping_id", function(x, ...) { standardGeneric("grouping_id") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("hex", function(x) { standardGeneric("hex") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("hour", function(x) { standardGeneric("hour") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("hypot", function(y, x) { standardGeneric("hypot") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("initcap", function(x) { standardGeneric("initcap") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("input_file_name",
           function(x = "missing") { standardGeneric("input_file_name") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("instr", function(y, x) { standardGeneric("instr") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("isnan", function(x) { standardGeneric("isnan") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("kurtosis", function(x) { standardGeneric("kurtosis") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("lag", function(x, ...) { standardGeneric("lag") })

#' @rdname last
setGeneric("last", function(x, ...) { standardGeneric("last") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("last_day", function(x) { standardGeneric("last_day") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("lead", function(x, offset, defaultValue = NULL) { standardGeneric("lead") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("least", function(x, ...) { standardGeneric("least") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("levenshtein", function(y, x) { standardGeneric("levenshtein") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("lit", function(x) { standardGeneric("lit") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("locate", function(substr, str, ...) { standardGeneric("locate") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("lower", function(x) { standardGeneric("lower") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("lpad", function(x, len, pad) { standardGeneric("lpad") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("ltrim", function(x, trimString) { standardGeneric("ltrim") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_concat", function(x, ...) { standardGeneric("map_concat") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_entries", function(x) { standardGeneric("map_entries") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_from_arrays", function(x, y) { standardGeneric("map_from_arrays") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_from_entries", function(x) { standardGeneric("map_from_entries") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_keys", function(x) { standardGeneric("map_keys") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_values", function(x) { standardGeneric("map_values") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("md5", function(x) { standardGeneric("md5") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("minute", function(x) { standardGeneric("minute") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("monotonically_increasing_id",
           function(x = "missing") { standardGeneric("monotonically_increasing_id") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("month", function(x) { standardGeneric("month") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("months_between", function(y, x, ...) { standardGeneric("months_between") })

#' @rdname count
setGeneric("n", function(x) { standardGeneric("n") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("nanvl", function(y, x) { standardGeneric("nanvl") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("negate", function(x) { standardGeneric("negate") })

#' @rdname not
setGeneric("not", function(x) { standardGeneric("not") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("next_day", function(y, x) { standardGeneric("next_day") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("ntile", function(x) { standardGeneric("ntile") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("n_distinct", function(x, ...) { standardGeneric("n_distinct") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("percent_rank", function(x = "missing") { standardGeneric("percent_rank") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("pmod", function(y, x) { standardGeneric("pmod") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("posexplode", function(x) { standardGeneric("posexplode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("posexplode_outer", function(x) { standardGeneric("posexplode_outer") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("quarter", function(x) { standardGeneric("quarter") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("rand", function(seed) { standardGeneric("rand") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("randn", function(seed) { standardGeneric("randn") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("rank", function(x, ...) { standardGeneric("rank") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("regexp_extract", function(x, pattern, idx) { standardGeneric("regexp_extract") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("regexp_replace",
           function(x, pattern, replacement) { standardGeneric("regexp_replace") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("repeat_string", function(x, n) { standardGeneric("repeat_string") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("reverse", function(x) { standardGeneric("reverse") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("rint", function(x) { standardGeneric("rint") })

#' @rdname column_window_functions
#' @name NULL
setGeneric("row_number", function(x = "missing") { standardGeneric("row_number") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("rpad", function(x, len, pad) { standardGeneric("rpad") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("rtrim", function(x, trimString) { standardGeneric("rtrim") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("schema_of_csv", function(x, ...) { standardGeneric("schema_of_csv") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("schema_of_json", function(x, ...) { standardGeneric("schema_of_json") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("sd", function(x, na.rm = FALSE) { standardGeneric("sd") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("second", function(x) { standardGeneric("second") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("sha1", function(x) { standardGeneric("sha1") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("sha2", function(y, x) { standardGeneric("sha2") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftLeft", function(y, x) { standardGeneric("shiftLeft") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftRight", function(y, x) { standardGeneric("shiftRight") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftRightUnsigned", function(y, x) { standardGeneric("shiftRightUnsigned") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("shuffle", function(x) { standardGeneric("shuffle") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("signum", function(x) { standardGeneric("signum") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("size", function(x) { standardGeneric("size") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("skewness", function(x) { standardGeneric("skewness") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("slice", function(x, start, length) { standardGeneric("slice") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("sort_array", function(x, asc = TRUE) { standardGeneric("sort_array") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("split_string", function(x, pattern, ...) { standardGeneric("split_string") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("soundex", function(x) { standardGeneric("soundex") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("spark_partition_id", function(x = "missing") { standardGeneric("spark_partition_id") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev", function(x) { standardGeneric("stddev") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev_pop", function(x) { standardGeneric("stddev_pop") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev_samp", function(x) { standardGeneric("stddev_samp") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("struct", function(x, ...) { standardGeneric("struct") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("substring_index", function(x, delim, count) { standardGeneric("substring_index") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("sumDistinct", function(x) { standardGeneric("sumDistinct") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("degrees", function(x) { standardGeneric("degrees") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("toDegrees", function(x) { standardGeneric("toDegrees") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("radians", function(x) { standardGeneric("radians") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("toRadians", function(x) { standardGeneric("toRadians") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("to_date", function(x, format) { standardGeneric("to_date") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("to_json", function(x, ...) { standardGeneric("to_json") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("to_csv", function(x, ...) { standardGeneric("to_csv") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("to_timestamp", function(x, format) { standardGeneric("to_timestamp") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("to_utc_timestamp", function(y, x) { standardGeneric("to_utc_timestamp") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("translate", function(x, matchingString, replaceString) { standardGeneric("translate") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("trim", function(x, trimString) { standardGeneric("trim") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("unbase64", function(x) { standardGeneric("unbase64") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("unhex", function(x) { standardGeneric("unhex") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("unix_timestamp", function(x, format) { standardGeneric("unix_timestamp") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("upper", function(x) { standardGeneric("upper") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("var", function(x, y = NULL, na.rm = FALSE, use) { standardGeneric("var") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("variance", function(x) { standardGeneric("variance") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("var_pop", function(x) { standardGeneric("var_pop") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("var_samp", function(x) { standardGeneric("var_samp") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("weekofyear", function(x) { standardGeneric("weekofyear") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("window", function(x, ...) { standardGeneric("window") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("xxhash64", function(x, ...) { standardGeneric("xxhash64") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("year", function(x) { standardGeneric("year") })


###################### Spark.ML Methods ##########################

#' @rdname fitted
setGeneric("fitted")

# Do not carry stats::glm usage and param here, and do not document the generic
#' @noRd
setGeneric("glm")

#' @param object a fitted ML model object.
#' @param ... additional argument(s) passed to the method.
#' @rdname predict
setGeneric("predict", function(object, ...) { standardGeneric("predict") })

#' @rdname rbind
setGeneric("rbind", signature = "...")

#' @rdname spark.als
setGeneric("spark.als", function(data, ...) { standardGeneric("spark.als") })

#' @rdname spark.bisectingKmeans
setGeneric("spark.bisectingKmeans",
           function(data, formula, ...) { standardGeneric("spark.bisectingKmeans") })

#' @rdname spark.gaussianMixture
setGeneric("spark.gaussianMixture",
           function(data, formula, ...) { standardGeneric("spark.gaussianMixture") })

#' @rdname spark.gbt
setGeneric("spark.gbt", function(data, formula, ...) { standardGeneric("spark.gbt") })

#' @rdname spark.glm
setGeneric("spark.glm", function(data, formula, ...) { standardGeneric("spark.glm") })

#' @rdname spark.isoreg
setGeneric("spark.isoreg", function(data, formula, ...) { standardGeneric("spark.isoreg") })

#' @rdname spark.kmeans
setGeneric("spark.kmeans", function(data, formula, ...) { standardGeneric("spark.kmeans") })

#' @rdname spark.kstest
setGeneric("spark.kstest", function(data, ...) { standardGeneric("spark.kstest") })

#' @rdname spark.lda
setGeneric("spark.lda", function(data, ...) { standardGeneric("spark.lda") })

#' @rdname spark.logit
setGeneric("spark.logit", function(data, formula, ...) { standardGeneric("spark.logit") })

#' @rdname spark.mlp
setGeneric("spark.mlp", function(data, formula, ...) { standardGeneric("spark.mlp") })

#' @rdname spark.naiveBayes
setGeneric("spark.naiveBayes", function(data, formula, ...) { standardGeneric("spark.naiveBayes") })

#' @rdname spark.decisionTree
setGeneric("spark.decisionTree",
           function(data, formula, ...) { standardGeneric("spark.decisionTree") })

#' @rdname spark.randomForest
setGeneric("spark.randomForest",
           function(data, formula, ...) { standardGeneric("spark.randomForest") })

#' @rdname spark.survreg
setGeneric("spark.survreg", function(data, formula, ...) { standardGeneric("spark.survreg") })

#' @rdname spark.svmLinear
setGeneric("spark.svmLinear", function(data, formula, ...) { standardGeneric("spark.svmLinear") })

#' @rdname spark.lda
setGeneric("spark.posterior", function(object, newData) { standardGeneric("spark.posterior") })

#' @rdname spark.lda
setGeneric("spark.perplexity", function(object, data) { standardGeneric("spark.perplexity") })

#' @rdname spark.fpGrowth
setGeneric("spark.fpGrowth", function(data, ...) { standardGeneric("spark.fpGrowth") })

#' @rdname spark.fpGrowth
setGeneric("spark.freqItemsets", function(object) { standardGeneric("spark.freqItemsets") })

#' @rdname spark.fpGrowth
setGeneric("spark.associationRules", function(object) { standardGeneric("spark.associationRules") })

#' @rdname spark.prefixSpan
setGeneric("spark.findFrequentSequentialPatterns",
            function(data, ...) { standardGeneric("spark.findFrequentSequentialPatterns") })

#' @rdname spark.powerIterationClustering
setGeneric("spark.assignClusters",
            function(data, ...) { standardGeneric("spark.assignClusters") })

#' @param object a fitted ML model object.
#' @param path the directory where the model is saved.
#' @param ... additional argument(s) passed to the method.
#' @rdname write.ml
setGeneric("write.ml", function(object, path, ...) { standardGeneric("write.ml") })


###################### Streaming Methods ##########################

#' @rdname awaitTermination
setGeneric("awaitTermination", function(x, timeout = NULL) { standardGeneric("awaitTermination") })

#' @rdname isActive
setGeneric("isActive", function(x) { standardGeneric("isActive") })

#' @rdname lastProgress
setGeneric("lastProgress", function(x) { standardGeneric("lastProgress") })

#' @rdname queryName
setGeneric("queryName", function(x) { standardGeneric("queryName") })

#' @rdname status
setGeneric("status", function(x) { standardGeneric("status") })

#' @rdname stopQuery
setGeneric("stopQuery", function(x) { standardGeneric("stopQuery") })
