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
# @export
setGeneric("aggregateRDD",
           function(x, zeroValue, seqOp, combOp) { standardGeneric("aggregateRDD") })

# @rdname cache-methods
# @export
setGeneric("cache", function(x) { standardGeneric("cache") })

# @rdname coalesce
# @seealso repartition
# @export
setGeneric("coalesce", function(x, numPartitions, ...) { standardGeneric("coalesce") })

# @rdname checkpoint-methods
# @export
setGeneric("checkpoint", function(x) { standardGeneric("checkpoint") })

# @rdname collect-methods
# @export
setGeneric("collect", function(x, ...) { standardGeneric("collect") })

# @rdname collect-methods
# @export
setGeneric("collectAsMap", function(x) { standardGeneric("collectAsMap") })

# @rdname collect-methods
# @export
setGeneric("collectPartition",
           function(x, partitionId) {
             standardGeneric("collectPartition")
           })

# @rdname count
# @export
setGeneric("count", function(x) { standardGeneric("count") })

# @rdname countByValue
# @export
setGeneric("countByValue", function(x) { standardGeneric("countByValue") })

# @rdname statfunctions
# @export
setGeneric("crosstab", function(x, col1, col2) { standardGeneric("crosstab") })

# @rdname statfunctions
# @export
setGeneric("freqItems", function(x, cols, support = 0.01) { standardGeneric("freqItems") })

# @rdname statfunctions
# @export
setGeneric("approxQuantile",
           function(x, col, probabilities, relativeError) {
             standardGeneric("approxQuantile")
           })

# @rdname distinct
# @export
setGeneric("distinct", function(x, numPartitions = 1) { standardGeneric("distinct") })

# @rdname filterRDD
# @export
setGeneric("filterRDD", function(x, f) { standardGeneric("filterRDD") })

# @rdname first
# @export
setGeneric("first", function(x, ...) { standardGeneric("first") })

# @rdname flatMap
# @export
setGeneric("flatMap", function(X, FUN) { standardGeneric("flatMap") })

# @rdname fold
# @seealso reduce
# @export
setGeneric("fold", function(x, zeroValue, op) { standardGeneric("fold") })

setGeneric("foreach", function(x, func) { standardGeneric("foreach") })

setGeneric("foreachPartition", function(x, func) { standardGeneric("foreachPartition") })

# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })

# @rdname glom
# @export
setGeneric("glom", function(x) { standardGeneric("glom") })

# @rdname histogram
# @export
setGeneric("histogram", function(df, col, nbins=10) { standardGeneric("histogram") })

# @rdname keyBy
# @export
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
# @export
setGeneric("maximum", function(x) { standardGeneric("maximum") })

# @rdname minimum
# @export
setGeneric("minimum", function(x) { standardGeneric("minimum") })

# @rdname sumRDD
# @export
setGeneric("sumRDD", function(x) { standardGeneric("sumRDD") })

# @rdname name
# @export
setGeneric("name", function(x) { standardGeneric("name") })

# @rdname getNumPartitions
# @export
setGeneric("getNumPartitions", function(x) { standardGeneric("getNumPartitions") })

# @rdname getNumPartitions
# @export
setGeneric("numPartitions", function(x) { standardGeneric("numPartitions") })

# @rdname persist
# @export
setGeneric("persist", function(x, newLevel) { standardGeneric("persist") })

# @rdname pipeRDD
# @export
setGeneric("pipeRDD", function(x, command, env = list()) { standardGeneric("pipeRDD")})

# @rdname reduce
# @export
setGeneric("reduce", function(x, func) { standardGeneric("reduce") })

# @rdname repartition
# @seealso coalesce
# @export
setGeneric("repartition", function(x, ...) { standardGeneric("repartition") })

# @rdname sampleRDD
# @export
setGeneric("sampleRDD",
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

# @rdname saveAsObjectFile
# @seealso objectFile
# @export
setGeneric("saveAsObjectFile", function(x, path) { standardGeneric("saveAsObjectFile") })

# @rdname saveAsTextFile
# @export
setGeneric("saveAsTextFile", function(x, path) { standardGeneric("saveAsTextFile") })

# @rdname setName
# @export
setGeneric("setName", function(x, name) { standardGeneric("setName") })

# @rdname sortBy
# @export
setGeneric("sortBy",
           function(x, func, ascending = TRUE, numPartitions = 1) {
             standardGeneric("sortBy")
           })

# @rdname take
# @export
setGeneric("take", function(x, num) { standardGeneric("take") })

# @rdname takeOrdered
# @export
setGeneric("takeOrdered", function(x, num) { standardGeneric("takeOrdered") })

# @rdname takeSample
# @export
setGeneric("takeSample",
           function(x, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })

# @rdname top
# @export
setGeneric("top", function(x, num) { standardGeneric("top") })

# @rdname unionRDD
# @export
setGeneric("unionRDD", function(x, y) { standardGeneric("unionRDD") })

# @rdname unpersist-methods
# @export
setGeneric("unpersist", function(x, ...) { standardGeneric("unpersist") })

# @rdname zipRDD
# @export
setGeneric("zipRDD", function(x, other) { standardGeneric("zipRDD") })

# @rdname zipRDD
# @export
setGeneric("zipPartitions", function(..., func) { standardGeneric("zipPartitions") },
           signature = "...")

# @rdname zipWithIndex
# @seealso zipWithUniqueId
# @export
setGeneric("zipWithIndex", function(x) { standardGeneric("zipWithIndex") })

# @rdname zipWithUniqueId
# @seealso zipWithIndex
# @export
setGeneric("zipWithUniqueId", function(x) { standardGeneric("zipWithUniqueId") })


############ Binary Functions #############

# @rdname cartesian
# @export
setGeneric("cartesian", function(x, other) { standardGeneric("cartesian") })

# @rdname countByKey
# @export
setGeneric("countByKey", function(x) { standardGeneric("countByKey") })

# @rdname flatMapValues
# @export
setGeneric("flatMapValues", function(X, FUN) { standardGeneric("flatMapValues") })

# @rdname intersection
# @export
setGeneric("intersection",
           function(x, other, numPartitions = 1) {
             standardGeneric("intersection")
           })

# @rdname keys
# @export
setGeneric("keys", function(x) { standardGeneric("keys") })

# @rdname lookup
# @export
setGeneric("lookup", function(x, key) { standardGeneric("lookup") })

# @rdname mapValues
# @export
setGeneric("mapValues", function(X, FUN) { standardGeneric("mapValues") })

# @rdname sampleByKey
# @export
setGeneric("sampleByKey",
           function(x, withReplacement, fractions, seed) {
             standardGeneric("sampleByKey")
           })

# @rdname values
# @export
setGeneric("values", function(x) { standardGeneric("values") })


############ Shuffle Functions ############

# @rdname aggregateByKey
# @seealso foldByKey, combineByKey
# @export
setGeneric("aggregateByKey",
           function(x, zeroValue, seqOp, combOp, numPartitions) {
             standardGeneric("aggregateByKey")
           })

# @rdname cogroup
# @export
setGeneric("cogroup",
           function(..., numPartitions) {
             standardGeneric("cogroup")
           },
           signature = "...")

# @rdname combineByKey
# @seealso groupByKey, reduceByKey
# @export
setGeneric("combineByKey",
           function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

# @rdname foldByKey
# @seealso aggregateByKey, combineByKey
# @export
setGeneric("foldByKey",
           function(x, zeroValue, func, numPartitions) {
             standardGeneric("foldByKey")
           })

# @rdname join-methods
# @export
setGeneric("fullOuterJoin", function(x, y, numPartitions) { standardGeneric("fullOuterJoin") })

# @rdname groupByKey
# @seealso reduceByKey
# @export
setGeneric("groupByKey", function(x, numPartitions) { standardGeneric("groupByKey") })

# @rdname join-methods
# @export
setGeneric("join", function(x, y, ...) { standardGeneric("join") })

# @rdname join-methods
# @export
setGeneric("leftOuterJoin", function(x, y, numPartitions) { standardGeneric("leftOuterJoin") })

#' @rdname partitionBy
#' @export
setGeneric("partitionBy", function(x, ...) { standardGeneric("partitionBy") })

# @rdname reduceByKey
# @seealso groupByKey
# @export
setGeneric("reduceByKey", function(x, combineFunc, numPartitions) { standardGeneric("reduceByKey")})

# @rdname reduceByKeyLocally
# @seealso reduceByKey
# @export
setGeneric("reduceByKeyLocally",
           function(x, combineFunc) {
             standardGeneric("reduceByKeyLocally")
           })

# @rdname join-methods
# @export
setGeneric("rightOuterJoin", function(x, y, numPartitions) { standardGeneric("rightOuterJoin") })

# @rdname sortByKey
# @export
setGeneric("sortByKey",
           function(x, ascending = TRUE, numPartitions = 1) {
             standardGeneric("sortByKey")
           })

# @rdname subtract
# @export
setGeneric("subtract",
           function(x, other, numPartitions = 1) {
             standardGeneric("subtract")
           })

# @rdname subtractByKey
# @export
setGeneric("subtractByKey",
           function(x, other, numPartitions = 1) {
             standardGeneric("subtractByKey")
           })


################### Broadcast Variable Methods #################

# @rdname broadcast
# @export
setGeneric("value", function(bcast) { standardGeneric("value") })


####################  SparkDataFrame Methods ########################

#' @rdname agg
#' @export
setGeneric("agg", function (x, ...) { standardGeneric("agg") })

#' @rdname arrange
#' @export
setGeneric("arrange", function(x, col, ...) { standardGeneric("arrange") })

#' @rdname as.data.frame
#' @export
setGeneric("as.data.frame",
           function(x, row.names = NULL, optional = FALSE, ...) {
             standardGeneric("as.data.frame")
           })

#' @rdname attach
#' @export
setGeneric("attach")

#' @rdname columns
#' @export
setGeneric("colnames", function(x, do.NULL = TRUE, prefix = "col") { standardGeneric("colnames") })

#' @rdname columns
#' @export
setGeneric("colnames<-", function(x, value) { standardGeneric("colnames<-") })

#' @rdname coltypes
#' @export
setGeneric("coltypes", function(x) { standardGeneric("coltypes") })

#' @rdname coltypes
#' @export
setGeneric("coltypes<-", function(x, value) { standardGeneric("coltypes<-") })

#' @rdname schema
#' @export
setGeneric("columns", function(x) {standardGeneric("columns") })

#' @rdname statfunctions
#' @export
setGeneric("cov", function(x, ...) {standardGeneric("cov") })

#' @rdname statfunctions
#' @export
setGeneric("corr", function(x, ...) {standardGeneric("corr") })

#' @rdname statfunctions
#' @export
setGeneric("covar_samp", function(col1, col2) {standardGeneric("covar_samp") })

#' @rdname statfunctions
#' @export
setGeneric("covar_pop", function(col1, col2) {standardGeneric("covar_pop") })

#' @rdname dapply
#' @export
setGeneric("dapply", function(x, func, schema) { standardGeneric("dapply") })

#' @rdname dapply
#' @export
setGeneric("dapplyCollect", function(x, func) { standardGeneric("dapplyCollect") })

#' @rdname summary
#' @export
setGeneric("describe", function(x, col, ...) { standardGeneric("describe") })

#' @rdname drop
#' @export
setGeneric("drop", function(x, ...) { standardGeneric("drop") })

#' @rdname dropduplicates
#' @export
setGeneric("dropDuplicates",
           function(x, colNames = columns(x)) {
             standardGeneric("dropDuplicates")
           })

#' @rdname nafunctions
#' @export
setGeneric("dropna",
           function(x, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
             standardGeneric("dropna")
           })

#' @rdname nafunctions
#' @export
setGeneric("na.omit",
           function(object, ...) {
             standardGeneric("na.omit")
           })

#' @rdname schema
#' @export
setGeneric("dtypes", function(x) { standardGeneric("dtypes") })

#' @rdname explain
#' @export
setGeneric("explain", function(x, ...) { standardGeneric("explain") })

#' @rdname except
#' @export
setGeneric("except", function(x, y) { standardGeneric("except") })

#' @rdname nafunctions
#' @export
setGeneric("fillna", function(x, value, cols = NULL) { standardGeneric("fillna") })

#' @rdname filter
#' @export
setGeneric("filter", function(x, condition) { standardGeneric("filter") })

#' @rdname groupBy
#' @export
setGeneric("group_by", function(x, ...) { standardGeneric("group_by") })

#' @rdname groupBy
#' @export
setGeneric("groupBy", function(x, ...) { standardGeneric("groupBy") })

#' @rdname insertInto
#' @export
setGeneric("insertInto", function(x, tableName, ...) { standardGeneric("insertInto") })

#' @rdname intersect
#' @export
setGeneric("intersect", function(x, y) { standardGeneric("intersect") })

#' @rdname isLocal
#' @export
setGeneric("isLocal", function(x) { standardGeneric("isLocal") })

#' @rdname limit
#' @export
setGeneric("limit", function(x, num) {standardGeneric("limit") })

#' @rdname merge
#' @export
setGeneric("merge")

#' @rdname mutate
#' @export
setGeneric("mutate", function(.data, ...) {standardGeneric("mutate") })

#' @rdname arrange
#' @export
setGeneric("orderBy", function(x, col, ...) { standardGeneric("orderBy") })

#' @rdname schema
#' @export
setGeneric("printSchema", function(x) { standardGeneric("printSchema") })

#' @rdname rename
#' @export
setGeneric("rename", function(x, ...) { standardGeneric("rename") })

#' @rdname registerTempTable
#' @export
setGeneric("registerTempTable", function(x, tableName) { standardGeneric("registerTempTable") })

#' @rdname sample
#' @export
setGeneric("sample",
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sample")
           })

#' @rdname sample
#' @export
setGeneric("sample_frac",
           function(x, withReplacement, fraction, seed) { standardGeneric("sample_frac") })

#' @rdname statfunctions
#' @export
setGeneric("sampleBy", function(x, col, fractions, seed) { standardGeneric("sampleBy") })

#' @rdname saveAsTable
#' @export
setGeneric("saveAsTable", function(df, tableName, source = NULL, mode = "error", ...) {
  standardGeneric("saveAsTable")
})

#' @export
setGeneric("str")

#' @rdname mutate
#' @export
setGeneric("transform", function(`_data`, ...) {standardGeneric("transform") })

#' @rdname write.df
#' @export
setGeneric("write.df", function(df, path, source = NULL, mode = "error", ...) {
  standardGeneric("write.df")
})

#' @rdname write.df
#' @export
setGeneric("saveDF", function(df, path, source = NULL, mode = "error", ...) {
  standardGeneric("saveDF")
})

#' @rdname write.jdbc
#' @export
setGeneric("write.jdbc", function(x, url, tableName, mode = "error", ...) {
  standardGeneric("write.jdbc")
})

#' @rdname write.json
#' @export
setGeneric("write.json", function(x, path) { standardGeneric("write.json") })

#' @rdname write.parquet
#' @export
setGeneric("write.parquet", function(x, path) { standardGeneric("write.parquet") })

#' @rdname write.parquet
#' @export
setGeneric("saveAsParquetFile", function(x, path) { standardGeneric("saveAsParquetFile") })

#' @rdname write.text
#' @export
setGeneric("write.text", function(x, path) { standardGeneric("write.text") })

#' @rdname schema
#' @export
setGeneric("schema", function(x) { standardGeneric("schema") })

#' @rdname select
#' @export
setGeneric("select", function(x, col, ...) { standardGeneric("select") } )

#' @rdname select
#' @export
setGeneric("selectExpr", function(x, expr, ...) { standardGeneric("selectExpr") })

#' @rdname showDF
#' @export
setGeneric("showDF", function(x, ...) { standardGeneric("showDF") })

# @rdname subset
# @export
setGeneric("subset", function(x, ...) { standardGeneric("subset") })

#' @rdname agg
#' @export
setGeneric("summarize", function(x, ...) { standardGeneric("summarize") })

#' @rdname summary
#' @export
setGeneric("summary", function(object, ...) { standardGeneric("summary") })

setGeneric("toJSON", function(x) { standardGeneric("toJSON") })

setGeneric("toRDD", function(x) { standardGeneric("toRDD") })

#' @rdname rbind
#' @export
setGeneric("unionAll", function(x, y) { standardGeneric("unionAll") })

#' @rdname filter
#' @export
setGeneric("where", function(x, condition) { standardGeneric("where") })

#' @rdname with
#' @export
setGeneric("with")

#' @rdname withColumn
#' @export
setGeneric("withColumn", function(x, colName, col) { standardGeneric("withColumn") })

#' @rdname rename
#' @export
setGeneric("withColumnRenamed",
           function(x, existingCol, newCol) { standardGeneric("withColumnRenamed") })

#' @rdname write.df
#' @export
setGeneric("write.df", function(df, path, ...) { standardGeneric("write.df") })

###################### Column Methods ##########################

#' @rdname column
#' @export
setGeneric("asc", function(x) { standardGeneric("asc") })

#' @rdname column
#' @export
setGeneric("between", function(x, bounds) { standardGeneric("between") })

#' @rdname column
#' @export
setGeneric("cast", function(x, dataType) { standardGeneric("cast") })

#' @rdname column
#' @export
setGeneric("contains", function(x, ...) { standardGeneric("contains") })

#' @rdname column
#' @export
setGeneric("desc", function(x) { standardGeneric("desc") })

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
setGeneric("isNaN", function(x) { standardGeneric("isNaN") })

#' @rdname column
#' @export
setGeneric("isNull", function(x) { standardGeneric("isNull") })

#' @rdname column
#' @export
setGeneric("isNotNull", function(x) { standardGeneric("isNotNull") })

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
setGeneric("when", function(condition, value) { standardGeneric("when") })

#' @rdname column
#' @export
setGeneric("otherwise", function(x, value) { standardGeneric("otherwise") })

#' @rdname over
#' @export
setGeneric("over", function(x, window) { standardGeneric("over") })

###################### WindowSpec Methods ##########################

#' @rdname rowsBetween
#' @export
setGeneric("rowsBetween", function(x, start, end) { standardGeneric("rowsBetween") })

#' @rdname rangeBetween
#' @export
setGeneric("rangeBetween", function(x, start, end) { standardGeneric("rangeBetween") })

#' @rdname window.partitionBy
#' @export
setGeneric("window.partitionBy", function(col, ...) { standardGeneric("window.partitionBy") })

#' @rdname window.orderBy
#' @export
setGeneric("window.orderBy", function(col, ...) { standardGeneric("window.orderBy") })

###################### Expression Function Methods ##########################

#' @rdname add_months
#' @export
setGeneric("add_months", function(y, x) { standardGeneric("add_months") })

#' @rdname approxCountDistinct
#' @export
setGeneric("approxCountDistinct", function(x, ...) { standardGeneric("approxCountDistinct") })

#' @rdname array_contains
#' @export
setGeneric("array_contains", function(x, value) { standardGeneric("array_contains") })

#' @rdname ascii
#' @export
setGeneric("ascii", function(x) { standardGeneric("ascii") })

#' @rdname avg
#' @export
setGeneric("avg", function(x, ...) { standardGeneric("avg") })

#' @rdname base64
#' @export
setGeneric("base64", function(x) { standardGeneric("base64") })

#' @rdname bin
#' @export
setGeneric("bin", function(x) { standardGeneric("bin") })

#' @rdname bitwiseNOT
#' @export
setGeneric("bitwiseNOT", function(x) { standardGeneric("bitwiseNOT") })

#' @rdname bround
#' @export
setGeneric("bround", function(x, ...) { standardGeneric("bround") })

#' @rdname cbrt
#' @export
setGeneric("cbrt", function(x) { standardGeneric("cbrt") })

#' @rdname ceil
#' @export
setGeneric("ceil", function(x) { standardGeneric("ceil") })

#' @rdname col
#' @export
setGeneric("column", function(x) { standardGeneric("column") })

#' @rdname concat
#' @export
setGeneric("concat", function(x, ...) { standardGeneric("concat") })

#' @rdname concat_ws
#' @export
setGeneric("concat_ws", function(sep, x, ...) { standardGeneric("concat_ws") })

#' @rdname conv
#' @export
setGeneric("conv", function(x, fromBase, toBase) { standardGeneric("conv") })

#' @rdname countDistinct
#' @export
setGeneric("countDistinct", function(x, ...) { standardGeneric("countDistinct") })

#' @rdname crc32
#' @export
setGeneric("crc32", function(x) { standardGeneric("crc32") })

#' @rdname hash
#' @export
setGeneric("hash", function(x, ...) { standardGeneric("hash") })

#' @rdname cume_dist
#' @export
setGeneric("cume_dist", function(x) { standardGeneric("cume_dist") })

#' @rdname datediff
#' @export
setGeneric("datediff", function(y, x) { standardGeneric("datediff") })

#' @rdname date_add
#' @export
setGeneric("date_add", function(y, x) { standardGeneric("date_add") })

#' @rdname date_format
#' @export
setGeneric("date_format", function(y, x) { standardGeneric("date_format") })

#' @rdname date_sub
#' @export
setGeneric("date_sub", function(y, x) { standardGeneric("date_sub") })

#' @rdname dayofmonth
#' @export
setGeneric("dayofmonth", function(x) { standardGeneric("dayofmonth") })

#' @rdname dayofyear
#' @export
setGeneric("dayofyear", function(x) { standardGeneric("dayofyear") })

#' @rdname decode
#' @export
setGeneric("decode", function(x, charset) { standardGeneric("decode") })

#' @rdname dense_rank
#' @export
setGeneric("dense_rank", function(x) { standardGeneric("dense_rank") })

#' @rdname encode
#' @export
setGeneric("encode", function(x, charset) { standardGeneric("encode") })

#' @rdname explode
#' @export
setGeneric("explode", function(x) { standardGeneric("explode") })

#' @rdname expr
#' @export
setGeneric("expr", function(x) { standardGeneric("expr") })

#' @rdname from_utc_timestamp
#' @export
setGeneric("from_utc_timestamp", function(y, x) { standardGeneric("from_utc_timestamp") })

#' @rdname format_number
#' @export
setGeneric("format_number", function(y, x) { standardGeneric("format_number") })

#' @rdname format_string
#' @export
setGeneric("format_string", function(format, x, ...) { standardGeneric("format_string") })

#' @rdname from_unixtime
#' @export
setGeneric("from_unixtime", function(x, ...) { standardGeneric("from_unixtime") })

#' @rdname greatest
#' @export
setGeneric("greatest", function(x, ...) { standardGeneric("greatest") })

#' @rdname hex
#' @export
setGeneric("hex", function(x) { standardGeneric("hex") })

#' @rdname hour
#' @export
setGeneric("hour", function(x) { standardGeneric("hour") })

#' @rdname hypot
#' @export
setGeneric("hypot", function(y, x) { standardGeneric("hypot") })

#' @rdname initcap
#' @export
setGeneric("initcap", function(x) { standardGeneric("initcap") })

#' @rdname instr
#' @export
setGeneric("instr", function(y, x) { standardGeneric("instr") })

#' @rdname is.nan
#' @export
setGeneric("isnan", function(x) { standardGeneric("isnan") })

#' @rdname kurtosis
#' @export
setGeneric("kurtosis", function(x) { standardGeneric("kurtosis") })

#' @rdname lag
#' @export
setGeneric("lag", function(x, ...) { standardGeneric("lag") })

#' @rdname last
#' @export
setGeneric("last", function(x, ...) { standardGeneric("last") })

#' @rdname last_day
#' @export
setGeneric("last_day", function(x) { standardGeneric("last_day") })

#' @rdname lead
#' @export
setGeneric("lead", function(x, offset, defaultValue = NULL) { standardGeneric("lead") })

#' @rdname least
#' @export
setGeneric("least", function(x, ...) { standardGeneric("least") })

#' @rdname levenshtein
#' @export
setGeneric("levenshtein", function(y, x) { standardGeneric("levenshtein") })

#' @rdname lit
#' @export
setGeneric("lit", function(x) { standardGeneric("lit") })

#' @rdname locate
#' @export
setGeneric("locate", function(substr, str, ...) { standardGeneric("locate") })

#' @rdname lower
#' @export
setGeneric("lower", function(x) { standardGeneric("lower") })

#' @rdname lpad
#' @export
setGeneric("lpad", function(x, len, pad) { standardGeneric("lpad") })

#' @rdname ltrim
#' @export
setGeneric("ltrim", function(x) { standardGeneric("ltrim") })

#' @rdname md5
#' @export
setGeneric("md5", function(x) { standardGeneric("md5") })

#' @rdname minute
#' @export
setGeneric("minute", function(x) { standardGeneric("minute") })

#' @rdname month
#' @export
setGeneric("month", function(x) { standardGeneric("month") })

#' @rdname months_between
#' @export
setGeneric("months_between", function(y, x) { standardGeneric("months_between") })

#' @rdname count
#' @export
setGeneric("n", function(x) { standardGeneric("n") })

#' @rdname nanvl
#' @export
setGeneric("nanvl", function(y, x) { standardGeneric("nanvl") })

#' @rdname negate
#' @export
setGeneric("negate", function(x) { standardGeneric("negate") })

#' @rdname next_day
#' @export
setGeneric("next_day", function(y, x) { standardGeneric("next_day") })

#' @rdname ntile
#' @export
setGeneric("ntile", function(x) { standardGeneric("ntile") })

#' @rdname countDistinct
#' @export
setGeneric("n_distinct", function(x, ...) { standardGeneric("n_distinct") })

#' @rdname percent_rank
#' @export
setGeneric("percent_rank", function(x) { standardGeneric("percent_rank") })

#' @rdname pmod
#' @export
setGeneric("pmod", function(y, x) { standardGeneric("pmod") })

#' @rdname quarter
#' @export
setGeneric("quarter", function(x) { standardGeneric("quarter") })

#' @rdname rand
#' @export
setGeneric("rand", function(seed) { standardGeneric("rand") })

#' @rdname randn
#' @export
setGeneric("randn", function(seed) { standardGeneric("randn") })

#' @rdname rank
#' @export
setGeneric("rank", function(x, ...) { standardGeneric("rank") })

#' @rdname regexp_extract
#' @export
setGeneric("regexp_extract", function(x, pattern, idx) { standardGeneric("regexp_extract") })

#' @rdname regexp_replace
#' @export
setGeneric("regexp_replace",
           function(x, pattern, replacement) { standardGeneric("regexp_replace") })

#' @rdname reverse
#' @export
setGeneric("reverse", function(x) { standardGeneric("reverse") })

#' @rdname rint
#' @export
setGeneric("rint", function(x, ...) { standardGeneric("rint") })

#' @rdname row_number
#' @export
setGeneric("row_number", function(x) { standardGeneric("row_number") })

#' @rdname rpad
#' @export
setGeneric("rpad", function(x, len, pad) { standardGeneric("rpad") })

#' @rdname rtrim
#' @export
setGeneric("rtrim", function(x) { standardGeneric("rtrim") })

#' @rdname sd
#' @export
setGeneric("sd", function(x, na.rm = FALSE) { standardGeneric("sd") })

#' @rdname second
#' @export
setGeneric("second", function(x) { standardGeneric("second") })

#' @rdname sha1
#' @export
setGeneric("sha1", function(x) { standardGeneric("sha1") })

#' @rdname sha2
#' @export
setGeneric("sha2", function(y, x) { standardGeneric("sha2") })

#' @rdname shiftLeft
#' @export
setGeneric("shiftLeft", function(y, x) { standardGeneric("shiftLeft") })

#' @rdname shiftRight
#' @export
setGeneric("shiftRight", function(y, x) { standardGeneric("shiftRight") })

#' @rdname shiftRightUnsigned
#' @export
setGeneric("shiftRightUnsigned", function(y, x) { standardGeneric("shiftRightUnsigned") })

#' @rdname signum
#' @export
setGeneric("signum", function(x) { standardGeneric("signum") })

#' @rdname size
#' @export
setGeneric("size", function(x) { standardGeneric("size") })

#' @rdname skewness
#' @export
setGeneric("skewness", function(x) { standardGeneric("skewness") })

#' @rdname sort_array
#' @export
setGeneric("sort_array", function(x, asc = TRUE) { standardGeneric("sort_array") })

#' @rdname soundex
#' @export
setGeneric("soundex", function(x) { standardGeneric("soundex") })

#' @rdname sd
#' @export
setGeneric("stddev", function(x) { standardGeneric("stddev") })

#' @rdname stddev_pop
#' @export
setGeneric("stddev_pop", function(x) { standardGeneric("stddev_pop") })

#' @rdname stddev_samp
#' @export
setGeneric("stddev_samp", function(x) { standardGeneric("stddev_samp") })

#' @rdname struct
#' @export
setGeneric("struct", function(x, ...) { standardGeneric("struct") })

#' @rdname substring_index
#' @export
setGeneric("substring_index", function(x, delim, count) { standardGeneric("substring_index") })

#' @rdname sumDistinct
#' @export
setGeneric("sumDistinct", function(x) { standardGeneric("sumDistinct") })

#' @rdname toDegrees
#' @export
setGeneric("toDegrees", function(x) { standardGeneric("toDegrees") })

#' @rdname toRadians
#' @export
setGeneric("toRadians", function(x) { standardGeneric("toRadians") })

#' @rdname to_date
#' @export
setGeneric("to_date", function(x) { standardGeneric("to_date") })

#' @rdname to_utc_timestamp
#' @export
setGeneric("to_utc_timestamp", function(y, x) { standardGeneric("to_utc_timestamp") })

#' @rdname translate
#' @export
setGeneric("translate", function(x, matchingString, replaceString) { standardGeneric("translate") })

#' @rdname trim
#' @export
setGeneric("trim", function(x) { standardGeneric("trim") })

#' @rdname unbase64
#' @export
setGeneric("unbase64", function(x) { standardGeneric("unbase64") })

#' @rdname unhex
#' @export
setGeneric("unhex", function(x) { standardGeneric("unhex") })

#' @rdname unix_timestamp
#' @export
setGeneric("unix_timestamp", function(x, format) { standardGeneric("unix_timestamp") })

#' @rdname upper
#' @export
setGeneric("upper", function(x) { standardGeneric("upper") })

#' @rdname var
#' @export
setGeneric("var", function(x, y = NULL, na.rm = FALSE, use) { standardGeneric("var") })

#' @rdname var
#' @export
setGeneric("variance", function(x) { standardGeneric("variance") })

#' @rdname var_pop
#' @export
setGeneric("var_pop", function(x) { standardGeneric("var_pop") })

#' @rdname var_samp
#' @export
setGeneric("var_samp", function(x) { standardGeneric("var_samp") })

#' @rdname weekofyear
#' @export
setGeneric("weekofyear", function(x) { standardGeneric("weekofyear") })

#' @rdname window
#' @export
setGeneric("window", function(x, ...) { standardGeneric("window") })

#' @rdname year
#' @export
setGeneric("year", function(x) { standardGeneric("year") })

#' @rdname spark.glm
#' @export
setGeneric("spark.glm", function(data, formula, ...) { standardGeneric("spark.glm") })

#' @rdname glm
#' @export
setGeneric("glm")

#' @rdname predict
#' @export
setGeneric("predict", function(object, ...) { standardGeneric("predict") })

#' @rdname rbind
#' @export
setGeneric("rbind", signature = "...")

#' @rdname spark.kmeans
#' @export
setGeneric("spark.kmeans", function(data, formula, ...) { standardGeneric("spark.kmeans") })

#' @rdname fitted
#' @export
setGeneric("fitted")

#' @rdname spark.naiveBayes
#' @export
setGeneric("spark.naiveBayes", function(data, formula, ...) { standardGeneric("spark.naiveBayes") })

#' @rdname spark.survreg
#' @export
setGeneric("spark.survreg", function(data, formula, ...) { standardGeneric("spark.survreg") })

#' @rdname write.ml
#' @export
setGeneric("write.ml", function(object, path, ...) { standardGeneric("write.ml") })
