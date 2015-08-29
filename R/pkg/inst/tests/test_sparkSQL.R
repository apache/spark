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

library(testthat)

context("SparkSQL functions")

# Utility function for easily checking the values of a StructField
checkStructField <- function(actual, expectedName, expectedType, expectedNullable) {
  expect_equal(class(actual), "structField")
  expect_equal(actual$name(), expectedName)
  expect_equal(actual$dataType.toString(), expectedType)
  expect_equal(actual$nullable(), expectedNullable)
}

# Tests for SparkSQL functions in SparkR

sc <- sparkR.init()

sqlContext <- sparkRSQL.init(sc)

mockLines <- c("{\"name\":\"Michael\"}",
               "{\"name\":\"Andy\", \"age\":30}",
               "{\"name\":\"Justin\", \"age\":19}")
jsonPath <- tempfile(pattern="sparkr-test", fileext=".tmp")
parquetPath <- tempfile(pattern="sparkr-test", fileext=".parquet")
writeLines(mockLines, jsonPath)

# For test nafunctions, like dropna(), fillna(),...
mockLinesNa <- c("{\"name\":\"Bob\",\"age\":16,\"height\":176.5}",
                 "{\"name\":\"Alice\",\"age\":null,\"height\":164.3}",
                 "{\"name\":\"David\",\"age\":60,\"height\":null}",
                 "{\"name\":\"Amy\",\"age\":null,\"height\":null}",
                 "{\"name\":null,\"age\":null,\"height\":null}")
jsonPathNa <- tempfile(pattern="sparkr-test", fileext=".tmp")
writeLines(mockLinesNa, jsonPathNa)

test_that("infer types", {
  expect_equal(infer_type(1L), "integer")
  expect_equal(infer_type(1.0), "double")
  expect_equal(infer_type("abc"), "string")
  expect_equal(infer_type(TRUE), "boolean")
  expect_equal(infer_type(as.Date("2015-03-11")), "date")
  expect_equal(infer_type(as.POSIXlt("2015-03-11 12:13:04.043")), "timestamp")
  expect_equal(infer_type(c(1L, 2L)),
               list(type = "array", elementType = "integer", containsNull = TRUE))
  expect_equal(infer_type(list(1L, 2L)),
               list(type = "array", elementType = "integer", containsNull = TRUE))
  testStruct <- infer_type(list(a = 1L, b = "2"))
  expect_equal(class(testStruct), "structType")
  checkStructField(testStruct$fields()[[1]], "a", "IntegerType", TRUE)
  checkStructField(testStruct$fields()[[2]], "b", "StringType", TRUE)
  e <- new.env()
  assign("a", 1L, envir = e)
  expect_equal(infer_type(e),
               list(type = "map", keyType = "string", valueType = "integer",
                    valueContainsNull = TRUE))
})

test_that("structType and structField", {
  testField <- structField("a", "string")
  expect_is(testField, "structField")
  expect_equal(testField$name(), "a")
  expect_true(testField$nullable())

  testSchema <- structType(testField, structField("b", "integer"))
  expect_is(testSchema, "structType")
  expect_is(testSchema$fields()[[2]], "structField")
  expect_equal(testSchema$fields()[[1]]$dataType.toString(), "StringType")
})

test_that("create DataFrame from RDD", {
  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
  df <- createDataFrame(sqlContext, rdd, list("a", "b"))
  expect_is(df, "DataFrame")
  expect_equal(count(df), 10)
  expect_equal(nrow(df), 10)
  expect_equal(ncol(df), 2)
  expect_equal(dim(df), c(10, 2))
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  df <- createDataFrame(sqlContext, rdd)
  expect_is(df, "DataFrame")
  expect_equal(columns(df), c("_1", "_2"))

  schema <- structType(structField(x = "a", type = "integer", nullable = TRUE),
                        structField(x = "b", type = "string", nullable = TRUE))
  df <- createDataFrame(sqlContext, rdd, schema)
  expect_is(df, "DataFrame")
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(a = x, b = as.character(x)) })
  df <- createDataFrame(sqlContext, rdd)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  df <- jsonFile(sqlContext, jsonPathNa)
  hiveCtx <- tryCatch({
    newJObject("org.apache.spark.sql.hive.test.TestHiveContext", ssc)
  },
  error = function(err) {
    skip("Hive is not build with SparkSQL, skipped")
  })
  sql(hiveCtx, "CREATE TABLE people (name string, age double, height float)")
  insertInto(df, "people")
  expect_equal(sql(hiveCtx, "SELECT age from people WHERE name = 'Bob'"), c(16))
  expect_equal(sql(hiveCtx, "SELECT height from people WHERE name ='Bob'"), c(176.5))

  schema <- structType(structField("name", "string"), structField("age", "integer"),
                       structField("height", "float"))
  df2 <- createDataFrame(sqlContext, df.toRDD, schema)
  expect_equal(columns(df2), c("name", "age", "height"))
  expect_equal(dtypes(df2), list(c("name", "string"), c("age", "int"), c("height", "float")))
  expect_equal(collect(where(df2, df2$name == "Bob")), c("Bob", 16, 176.5))

  localDF <- data.frame(name=c("John", "Smith", "Sarah"),
                        age=c(19, 23, 18),
                        height=c(164.10, 181.4, 173.7))
  df <- createDataFrame(sqlContext, localDF, schema)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 3)
  expect_equal(columns(df), c("name", "age", "height"))
  expect_equal(dtypes(df), list(c("name", "string"), c("age", "int"), c("height", "float")))
  expect_equal(collect(where(df, df$name == "John")), c("John", 19, 164.10))
})

test_that("convert NAs to null type in DataFrames", {
  rdd <- parallelize(sc, list(list(1L, 2L), list(NA, 4L)))
  df <- createDataFrame(sqlContext, rdd, list("a", "b"))
  expect_true(is.na(collect(df)[2, "a"]))
  expect_equal(collect(df)[2, "b"], 4L)

  l <- data.frame(x = 1L, y = c(1L, NA_integer_, 3L))
  df <- createDataFrame(sqlContext, l)
  expect_equal(collect(df)[2, "x"], 1L)
  expect_true(is.na(collect(df)[2, "y"]))

  rdd <- parallelize(sc, list(list(1, 2), list(NA, 4)))
  df <- createDataFrame(sqlContext, rdd, list("a", "b"))
  expect_true(is.na(collect(df)[2, "a"]))
  expect_equal(collect(df)[2, "b"], 4)

  l <- data.frame(x = 1, y = c(1, NA_real_, 3))
  df <- createDataFrame(sqlContext, l)
  expect_equal(collect(df)[2, "x"], 1)
  expect_true(is.na(collect(df)[2, "y"]))

  l <- list("a", "b", NA, "d")
  df <- createDataFrame(sqlContext, l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], "d")

  l <- list("a", "b", NA_character_, "d")
  df <- createDataFrame(sqlContext, l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], "d")

  l <- list(TRUE, FALSE, NA, TRUE)
  df <- createDataFrame(sqlContext, l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], TRUE)
})

test_that("toDF", {
  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
  df <- toDF(rdd, list("a", "b"))
  expect_is(df, "DataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  df <- toDF(rdd)
  expect_is(df, "DataFrame")
  expect_equal(columns(df), c("_1", "_2"))

  schema <- structType(structField(x = "a", type = "integer", nullable = TRUE),
                        structField(x = "b", type = "string", nullable = TRUE))
  df <- toDF(rdd, schema)
  expect_is(df, "DataFrame")
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(a = x, b = as.character(x)) })
  df <- toDF(rdd)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
})

test_that("create DataFrame from list or data.frame", {
  l <- list(list(1, 2), list(3, 4))
  df <- createDataFrame(sqlContext, l, c("a", "b"))
  expect_equal(columns(df), c("a", "b"))

  l <- list(list(a=1, b=2), list(a=3, b=4))
  df <- createDataFrame(sqlContext, l)
  expect_equal(columns(df), c("a", "b"))

  a <- 1:3
  b <- c("a", "b", "c")
  ldf <- data.frame(a, b)
  df <- createDataFrame(sqlContext, ldf)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
  expect_equal(count(df), 3)
  ldf2 <- collect(df)
  expect_equal(ldf$a, ldf2$a)
})

test_that("create DataFrame with different data types", {
  l <- list(a = 1L, b = 2, c = TRUE, d = "ss", e = as.Date("2012-12-13"),
            f = as.POSIXct("2015-03-15 12:13:14.056"))
  df <- createDataFrame(sqlContext, list(l))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "double"), c("c", "boolean"),
                                c("d", "string"), c("e", "date"), c("f", "timestamp")))
  expect_equal(count(df), 1)
  expect_equal(collect(df), data.frame(l, stringsAsFactors = FALSE))
})

# TODO: enable this test after fix serialization for nested object
#test_that("create DataFrame with nested array and struct", {
#  e <- new.env()
#  assign("n", 3L, envir = e)
#  l <- list(1:10, list("a", "b"), e, list(a="aa", b=3L))
#  df <- createDataFrame(sqlContext, list(l), c("a", "b", "c", "d"))
#  expect_equal(dtypes(df), list(c("a", "array<int>"), c("b", "array<string>"),
#                                c("c", "map<string,int>"), c("d", "struct<a:string,b:int>")))
#  expect_equal(count(df), 1)
#  ldf <- collect(df)
#  expect_equal(ldf[1,], l[[1]])
#})

test_that("jsonFile() on a local file returns a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 3)
})

test_that("jsonRDD() on a RDD with json string", {
  rdd <- parallelize(sc, mockLines)
  expect_equal(count(rdd), 3)
  df <- jsonRDD(sqlContext, rdd)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 3)

  rdd2 <- flatMap(rdd, function(x) c(x, x))
  df <- jsonRDD(sqlContext, rdd2)
  expect_is(df, "DataFrame")
  expect_equal(count(df), 6)
})

test_that("test cache, uncache and clearCache", {
  df <- jsonFile(sqlContext, jsonPath)
  registerTempTable(df, "table1")
  cacheTable(sqlContext, "table1")
  uncacheTable(sqlContext, "table1")
  clearCache(sqlContext)
  dropTempTable(sqlContext, "table1")
})

test_that("test tableNames and tables", {
  df <- jsonFile(sqlContext, jsonPath)
  registerTempTable(df, "table1")
  expect_equal(length(tableNames(sqlContext)), 1)
  df <- tables(sqlContext)
  expect_equal(count(df), 1)
  dropTempTable(sqlContext, "table1")
})

test_that("registerTempTable() results in a queryable table and sql() results in a new DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  registerTempTable(df, "table1")
  newdf <- sql(sqlContext, "SELECT * FROM table1 where name = 'Michael'")
  expect_is(newdf, "DataFrame")
  expect_equal(count(newdf), 1)
  dropTempTable(sqlContext, "table1")
})

test_that("insertInto() on a registered table", {
  df <- read.df(sqlContext, jsonPath, "json")
  write.df(df, parquetPath, "parquet", "overwrite")
  dfParquet <- read.df(sqlContext, parquetPath, "parquet")

  lines <- c("{\"name\":\"Bob\", \"age\":24}",
             "{\"name\":\"James\", \"age\":35}")
  jsonPath2 <- tempfile(pattern="jsonPath2", fileext=".tmp")
  parquetPath2 <- tempfile(pattern = "parquetPath2", fileext = ".parquet")
  writeLines(lines, jsonPath2)
  df2 <- read.df(sqlContext, jsonPath2, "json")
  write.df(df2, parquetPath2, "parquet", "overwrite")
  dfParquet2 <- read.df(sqlContext, parquetPath2, "parquet")

  registerTempTable(dfParquet, "table1")
  insertInto(dfParquet2, "table1")
  expect_equal(count(sql(sqlContext, "select * from table1")), 5)
  expect_equal(first(sql(sqlContext, "select * from table1 order by age"))$name, "Michael")
  dropTempTable(sqlContext, "table1")

  registerTempTable(dfParquet, "table1")
  insertInto(dfParquet2, "table1", overwrite = TRUE)
  expect_equal(count(sql(sqlContext, "select * from table1")), 2)
  expect_equal(first(sql(sqlContext, "select * from table1 order by age"))$name, "Bob")
  dropTempTable(sqlContext, "table1")
})

test_that("table() returns a new DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  registerTempTable(df, "table1")
  tabledf <- table(sqlContext, "table1")
  expect_is(tabledf, "DataFrame")
  expect_equal(count(tabledf), 3)
  dropTempTable(sqlContext, "table1")
})

test_that("toRDD() returns an RRDD", {
  df <- jsonFile(sqlContext, jsonPath)
  testRDD <- toRDD(df)
  expect_is(testRDD, "RDD")
  expect_equal(count(testRDD), 3)
})

test_that("union on two RDDs created from DataFrames returns an RRDD", {
  df <- jsonFile(sqlContext, jsonPath)
  RDD1 <- toRDD(df)
  RDD2 <- toRDD(df)
  unioned <- unionRDD(RDD1, RDD2)
  expect_is(unioned, "RDD")
  expect_equal(SparkR:::getSerializedMode(unioned), "byte")
  expect_equal(collect(unioned)[[2]]$name, "Andy")
})

test_that("union on mixed serialization types correctly returns a byte RRDD", {
  # Byte RDD
  nums <- 1:10
  rdd <- parallelize(sc, nums, 2L)

  # String RDD
  textLines <- c("Michael",
                 "Andy, 30",
                 "Justin, 19")
  textPath <- tempfile(pattern="sparkr-textLines", fileext=".tmp")
  writeLines(textLines, textPath)
  textRDD <- textFile(sc, textPath)

  df <- jsonFile(sqlContext, jsonPath)
  dfRDD <- toRDD(df)

  unionByte <- unionRDD(rdd, dfRDD)
  expect_is(unionByte, "RDD")
  expect_equal(SparkR:::getSerializedMode(unionByte), "byte")
  expect_equal(collect(unionByte)[[1]], 1)
  expect_equal(collect(unionByte)[[12]]$name, "Andy")

  unionString <- unionRDD(textRDD, dfRDD)
  expect_is(unionString, "RDD")
  expect_equal(SparkR:::getSerializedMode(unionString), "byte")
  expect_equal(collect(unionString)[[1]], "Michael")
  expect_equal(collect(unionString)[[5]]$name, "Andy")
})

test_that("objectFile() works with row serialization", {
  objectPath <- tempfile(pattern="spark-test", fileext=".tmp")
  df <- jsonFile(sqlContext, jsonPath)
  dfRDD <- toRDD(df)
  saveAsObjectFile(coalesce(dfRDD, 1L), objectPath)
  objectIn <- objectFile(sc, objectPath)

  expect_is(objectIn, "RDD")
  expect_equal(SparkR:::getSerializedMode(objectIn), "byte")
  expect_equal(collect(objectIn)[[2]]$age, 30)
})

test_that("lapply() on a DataFrame returns an RDD with the correct columns", {
  df <- jsonFile(sqlContext, jsonPath)
  testRDD <- lapply(df, function(row) {
    row$newCol <- row$age + 5
    row
    })
  expect_is(testRDD, "RDD")
  collected <- collect(testRDD)
  expect_equal(collected[[1]]$name, "Michael")
  expect_equal(collected[[2]]$newCol, 35)
})

test_that("collect() returns a data.frame", {
  df <- jsonFile(sqlContext, jsonPath)
  rdf <- collect(df)
  expect_true(is.data.frame(rdf))
  expect_equal(names(rdf)[1], "age")
  expect_equal(nrow(rdf), 3)
  expect_equal(ncol(rdf), 2)

  # collect() returns data correctly from a DataFrame with 0 row
  df0 <- limit(df, 0)
  rdf <- collect(df0)
  expect_true(is.data.frame(rdf))
  expect_equal(names(rdf)[1], "age")
  expect_equal(nrow(rdf), 0)
  expect_equal(ncol(rdf), 2)
})

test_that("limit() returns DataFrame with the correct number of rows", {
  df <- jsonFile(sqlContext, jsonPath)
  dfLimited <- limit(df, 2)
  expect_is(dfLimited, "DataFrame")
  expect_equal(count(dfLimited), 2)
})

test_that("collect() and take() on a DataFrame return the same number of rows and columns", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_equal(nrow(collect(df)), nrow(take(df, 10)))
  expect_equal(ncol(collect(df)), ncol(take(df, 10)))
})

test_that("multiple pipeline transformations result in an RDD with the correct values", {
  df <- jsonFile(sqlContext, jsonPath)
  first <- lapply(df, function(row) {
    row$age <- row$age + 5
    row
  })
  second <- lapply(first, function(row) {
    row$testCol <- if (row$age == 35 && !is.na(row$age)) TRUE else FALSE
    row
  })
  expect_is(second, "RDD")
  expect_equal(count(second), 3)
  expect_equal(collect(second)[[2]]$age, 35)
  expect_true(collect(second)[[2]]$testCol)
  expect_false(collect(second)[[3]]$testCol)
})

test_that("cache(), persist(), and unpersist() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_false(df@env$isCached)
  cache(df)
  expect_true(df@env$isCached)

  unpersist(df)
  expect_false(df@env$isCached)

  persist(df, "MEMORY_AND_DISK")
  expect_true(df@env$isCached)

  unpersist(df)
  expect_false(df@env$isCached)

  # make sure the data is collectable
  expect_true(is.data.frame(collect(df)))
})

test_that("schema(), dtypes(), columns(), names() return the correct values/format", {
  df <- jsonFile(sqlContext, jsonPath)
  testSchema <- schema(df)
  expect_equal(length(testSchema$fields()), 2)
  expect_equal(testSchema$fields()[[1]]$dataType.toString(), "LongType")
  expect_equal(testSchema$fields()[[2]]$dataType.simpleString(), "string")
  expect_equal(testSchema$fields()[[1]]$name(), "age")

  testTypes <- dtypes(df)
  expect_equal(length(testTypes[[1]]), 2)
  expect_equal(testTypes[[1]][1], "age")

  testCols <- columns(df)
  expect_equal(length(testCols), 2)
  expect_equal(testCols[2], "name")

  testNames <- names(df)
  expect_equal(length(testNames), 2)
  expect_equal(testNames[2], "name")
})

test_that("head() and first() return the correct data", {
  df <- jsonFile(sqlContext, jsonPath)
  testHead <- head(df)
  expect_equal(nrow(testHead), 3)
  expect_equal(ncol(testHead), 2)

  testHead2 <- head(df, 2)
  expect_equal(nrow(testHead2), 2)
  expect_equal(ncol(testHead2), 2)

  testFirst <- first(df)
  expect_equal(nrow(testFirst), 1)

  # head() and first() return the correct data on
  # a DataFrame with 0 row
  df0 <- limit(df, 0)

  testHead <- head(df0)
  expect_equal(nrow(testHead), 0)
  expect_equal(ncol(testHead), 2)

  testFirst <- first(df0)
  expect_equal(nrow(testFirst), 0)
  expect_equal(ncol(testFirst), 2)
})

test_that("distinct() and unique on DataFrames", {
  lines <- c("{\"name\":\"Michael\"}",
             "{\"name\":\"Andy\", \"age\":30}",
             "{\"name\":\"Justin\", \"age\":19}",
             "{\"name\":\"Justin\", \"age\":19}")
  jsonPathWithDup <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(lines, jsonPathWithDup)

  df <- jsonFile(sqlContext, jsonPathWithDup)
  uniques <- distinct(df)
  expect_is(uniques, "DataFrame")
  expect_equal(count(uniques), 3)

  uniques2 <- unique(df)
  expect_is(uniques2, "DataFrame")
  expect_equal(count(uniques2), 3)
})

test_that("sample on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  sampled <- sample(df, FALSE, 1.0)
  expect_equal(nrow(collect(sampled)), count(df))
  expect_is(sampled, "DataFrame")
  sampled2 <- sample(df, FALSE, 0.1)
  expect_true(count(sampled2) < 3)

  # Also test sample_frac
  sampled3 <- sample_frac(df, FALSE, 0.1)
  expect_true(count(sampled3) < 3)
})

test_that("select operators", {
  df <- select(jsonFile(sqlContext, jsonPath), "name", "age")
  expect_is(df$name, "Column")
  expect_is(df[[2]], "Column")
  expect_is(df[["age"]], "Column")

  expect_is(df[,1], "DataFrame")
  expect_equal(columns(df[,1]), c("name"))
  expect_equal(columns(df[,"age"]), c("age"))
  df2 <- df[,c("age", "name")]
  expect_is(df2, "DataFrame")
  expect_equal(columns(df2), c("age", "name"))

  df$age2 <- df$age
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == df$age)), 2)
  df$age2 <- df$age * 2
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == df$age * 2)), 2)

  df$age2 <- NULL
  expect_equal(columns(df), c("name", "age"))
  df$age3 <- NULL
  expect_equal(columns(df), c("name", "age"))
})

test_that("select with column", {
  df <- jsonFile(sqlContext, jsonPath)
  df1 <- select(df, "name")
  expect_equal(columns(df1), c("name"))
  expect_equal(count(df1), 3)

  df2 <- select(df, df$age)
  expect_equal(columns(df2), c("age"))
  expect_equal(count(df2), 3)

  df3 <- select(df, lit("x"))
  expect_equal(columns(df3), c("x"))
  expect_equal(count(df3), 3)
  expect_equal(collect(select(df3, "x"))[[1, 1]], "x")
})

test_that("subsetting", {
  # jsonFile returns columns in random order
  df <- select(jsonFile(sqlContext, jsonPath), "name", "age")
  filtered <- df[df$age > 20,]
  expect_equal(count(filtered), 1)
  expect_equal(columns(filtered), c("name", "age"))
  expect_equal(collect(filtered)$name, "Andy")

  df2 <- df[df$age == 19, 1]
  expect_is(df2, "DataFrame")
  expect_equal(count(df2), 1)
  expect_equal(columns(df2), c("name"))
  expect_equal(collect(df2)$name, "Justin")

  df3 <- df[df$age > 20, 2]
  expect_equal(count(df3), 1)
  expect_equal(columns(df3), c("age"))

  df4 <- df[df$age %in% c(19, 30), 1:2]
  expect_equal(count(df4), 2)
  expect_equal(columns(df4), c("name", "age"))
  
  df5 <- df[df$age %in% c(19), c(1,2)]
  expect_equal(count(df5), 1)
  expect_equal(columns(df5), c("name", "age"))

  df6 <- subset(df, df$age %in% c(30), c(1,2))
  expect_equal(count(df6), 1)
  expect_equal(columns(df6), c("name", "age"))
})

test_that("selectExpr() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  selected <- selectExpr(df, "age * 2")
  expect_equal(names(selected), "(age * 2)")
  expect_equal(collect(selected), collect(select(df, df$age * 2L)))

  selected2 <- selectExpr(df, "name as newName", "abs(age) as age")
  expect_equal(names(selected2), c("newName", "age"))
  expect_equal(count(selected2), 3)
})

test_that("expr() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_equal(collect(select(df, expr("abs(-123)")))[1, 1], 123)
})

test_that("column calculation", {
  df <- jsonFile(sqlContext, jsonPath)
  d <- collect(select(df, alias(df$age + 1, "age2")))
  expect_equal(names(d), c("age2"))
  df2 <- select(df, lower(df$name), abs(df$age))
  expect_is(df2, "DataFrame")
  expect_equal(count(df2), 3)
})

test_that("read.df() from json file", {
  df <- read.df(sqlContext, jsonPath, "json")
  expect_is(df, "DataFrame")
  expect_equal(count(df), 3)

  # Check if we can apply a user defined schema
  schema <- structType(structField("name", type = "string"),
                       structField("age", type = "double"))

  df1 <- read.df(sqlContext, jsonPath, "json", schema)
  expect_is(df1, "DataFrame")
  expect_equal(dtypes(df1), list(c("name", "string"), c("age", "double")))

  # Run the same with loadDF
  df2 <- loadDF(sqlContext, jsonPath, "json", schema)
  expect_is(df2, "DataFrame")
  expect_equal(dtypes(df2), list(c("name", "string"), c("age", "double")))
})

test_that("write.df() as parquet file", {
  df <- read.df(sqlContext, jsonPath, "json")
  write.df(df, parquetPath, "parquet", mode="overwrite")
  df2 <- read.df(sqlContext, parquetPath, "parquet")
  expect_is(df2, "DataFrame")
  expect_equal(count(df2), 3)
})

test_that("test HiveContext", {
  hiveCtx <- tryCatch({
    newJObject("org.apache.spark.sql.hive.test.TestHiveContext", ssc)
  },
  error = function(err) {
    skip("Hive is not build with SparkSQL, skipped")
  })
  df <- createExternalTable(hiveCtx, "json", jsonPath, "json")
  expect_is(df, "DataFrame")
  expect_equal(count(df), 3)
  df2 <- sql(hiveCtx, "select * from json")
  expect_is(df2, "DataFrame")
  expect_equal(count(df2), 3)

  jsonPath2 <- tempfile(pattern="sparkr-test", fileext=".tmp")
  saveAsTable(df, "json", "json", "append", path = jsonPath2)
  df3 <- sql(hiveCtx, "select * from json")
  expect_is(df3, "DataFrame")
  expect_equal(count(df3), 6)
})

test_that("column operators", {
  c <- SparkR:::col("a")
  c2 <- (- c + 1 - 2) * 3 / 4.0
  c3 <- (c + c2 - c2) * c2 %% c2
  c4 <- (c > c2) & (c2 <= c3) | (c == c2) & (c2 != c3)
  c5 <- c2 ^ c3 ^ c4
})

test_that("column functions", {
  c <- SparkR:::col("a")
  c1 <- abs(c) + acos(c) + approxCountDistinct(c) + ascii(c) + asin(c) + atan(c)
  c2 <- avg(c) + base64(c) + bin(c) + bitwiseNOT(c) + cbrt(c) + ceil(c) + cos(c)
  c3 <- cosh(c) + count(c) + crc32(c) + exp(c)
  c4 <- explode(c) + expm1(c) + factorial(c) + first(c) + floor(c) + hex(c)
  c5 <- hour(c) + initcap(c) + isNaN(c) + last(c) + last_day(c) + length(c)
  c6 <- log(c) + (c) + log1p(c) + log2(c) + lower(c) + ltrim(c) + max(c) + md5(c)
  c7 <- mean(c) + min(c) + month(c) + negate(c) + quarter(c)
  c8 <- reverse(c) + rint(c) + round(c) + rtrim(c) + sha1(c)
  c9 <- signum(c) + sin(c) + sinh(c) + size(c) + soundex(c) + sqrt(c) + sum(c)
  c10 <- sumDistinct(c) + tan(c) + tanh(c) + toDegrees(c) + toRadians(c)
  c11 <- to_date(c) + trim(c) + unbase64(c) + unhex(c) + upper(c)

  df <- jsonFile(sqlContext, jsonPath)
  df2 <- select(df, between(df$age, c(20, 30)), between(df$age, c(10, 20)))
  expect_equal(collect(df2)[[2, 1]], TRUE)
  expect_equal(collect(df2)[[2, 2]], FALSE)
  expect_equal(collect(df2)[[3, 1]], FALSE)
  expect_equal(collect(df2)[[3, 2]], TRUE)

  df3 <- select(df, between(df$name, c("Apache", "Spark")))
  expect_equal(collect(df3)[[1, 1]], TRUE)
  expect_equal(collect(df3)[[2, 1]], FALSE)
  expect_equal(collect(df3)[[3, 1]], TRUE)

  df4 <- createDataFrame(sqlContext, list(list(a = "010101")))
  expect_equal(collect(select(df4, conv(df4$a, 2, 16)))[1, 1], "15")
})
#
test_that("column binary mathfunctions", {
  lines <- c("{\"a\":1, \"b\":5}",
             "{\"a\":2, \"b\":6}",
             "{\"a\":3, \"b\":7}",
             "{\"a\":4, \"b\":8}")
  jsonPathWithDup <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(lines, jsonPathWithDup)
  df <- jsonFile(sqlContext, jsonPathWithDup)
  expect_equal(collect(select(df, atan2(df$a, df$b)))[1, "ATAN2(a, b)"], atan2(1, 5))
  expect_equal(collect(select(df, atan2(df$a, df$b)))[2, "ATAN2(a, b)"], atan2(2, 6))
  expect_equal(collect(select(df, atan2(df$a, df$b)))[3, "ATAN2(a, b)"], atan2(3, 7))
  expect_equal(collect(select(df, atan2(df$a, df$b)))[4, "ATAN2(a, b)"], atan2(4, 8))
  ## nolint start
  expect_equal(collect(select(df, hypot(df$a, df$b)))[1, "HYPOT(a, b)"], sqrt(1^2 + 5^2))
  expect_equal(collect(select(df, hypot(df$a, df$b)))[2, "HYPOT(a, b)"], sqrt(2^2 + 6^2))
  expect_equal(collect(select(df, hypot(df$a, df$b)))[3, "HYPOT(a, b)"], sqrt(3^2 + 7^2))
  expect_equal(collect(select(df, hypot(df$a, df$b)))[4, "HYPOT(a, b)"], sqrt(4^2 + 8^2))
  ## nolint end
  expect_equal(collect(select(df, shiftLeft(df$b, 1)))[4, 1], 16)
  expect_equal(collect(select(df, shiftRight(df$b, 1)))[4, 1], 4)
  expect_equal(collect(select(df, shiftRightUnsigned(df$b, 1)))[4, 1], 4)
  expect_equal(class(collect(select(df, rand()))[2, 1]), "numeric")
  expect_equal(collect(select(df, rand(1)))[1, 1], 0.45, tolerance = 0.01)
  expect_equal(class(collect(select(df, randn()))[2, 1]), "numeric")
  expect_equal(collect(select(df, randn(1)))[1, 1], -0.0111, tolerance = 0.01)
})

test_that("string operators", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_equal(count(where(df, like(df$name, "A%"))), 1)
  expect_equal(count(where(df, startsWith(df$name, "A"))), 1)
  expect_equal(first(select(df, substr(df$name, 1, 2)))[[1]], "Mi")
  expect_equal(collect(select(df, cast(df$age, "string")))[[2, 1]], "30")
  expect_equal(collect(select(df, concat(df$name, lit(":"), df$age)))[[2, 1]], "Andy:30")
  expect_equal(collect(select(df, concat_ws(":", df$name)))[[2, 1]], "Andy")
  expect_equal(collect(select(df, concat_ws(":", df$name, df$age)))[[2, 1]], "Andy:30")
  expect_equal(collect(select(df, instr(df$name, "i")))[, 1], c(2, 0, 5))
  expect_equal(collect(select(df, format_number(df$age, 2)))[2, 1], "30.00")
  expect_equal(collect(select(df, sha1(df$name)))[2, 1],
               "ab5a000e88b5d9d0fa2575f5c6263eb93452405d")
  expect_equal(collect(select(df, sha2(df$name, 256)))[2, 1],
               "80f2aed3c618c423ddf05a2891229fba44942d907173152442cf6591441ed6dc")
  expect_equal(collect(select(df, format_string("Name:%s", df$name)))[2, 1], "Name:Andy")
  expect_equal(collect(select(df, format_string("%s, %d", df$name, df$age)))[2, 1], "Andy, 30")
  expect_equal(collect(select(df, regexp_extract(df$name, "(n.y)", 1)))[2, 1], "ndy")
  expect_equal(collect(select(df, regexp_replace(df$name, "(n.y)", "ydn")))[2, 1], "Aydn")

  l2 <- list(list(a = "aaads"))
  df2 <- createDataFrame(sqlContext, l2)
  expect_equal(collect(select(df2, locate("aa", df2$a)))[1, 1], 1)
  expect_equal(collect(select(df2, locate("aa", df2$a, 1)))[1, 1], 2)
  expect_equal(collect(select(df2, lpad(df2$a, 8, "#")))[1, 1], "###aaads")
  expect_equal(collect(select(df2, rpad(df2$a, 8, "#")))[1, 1], "aaads###")

  l3 <- list(list(a = "a.b.c.d"))
  df3 <- createDataFrame(sqlContext, l3)
  expect_equal(collect(select(df3, substring_index(df3$a, ".", 2)))[1, 1], "a.b")
  expect_equal(collect(select(df3, substring_index(df3$a, ".", -3)))[1, 1], "b.c.d")
  expect_equal(collect(select(df3, translate(df3$a, "bc", "12")))[1, 1], "a.1.2.d")
})

test_that("date functions on a DataFrame", {
  .originalTimeZone <- Sys.getenv("TZ")
  Sys.setenv(TZ = "UTC")
  l <- list(list(a = 1L, b = as.Date("2012-12-13")),
            list(a = 2L, b = as.Date("2013-12-14")),
            list(a = 3L, b = as.Date("2014-12-15")))
  df <- createDataFrame(sqlContext, l)
  expect_equal(collect(select(df, dayofmonth(df$b)))[, 1], c(13, 14, 15))
  expect_equal(collect(select(df, dayofyear(df$b)))[, 1], c(348, 348, 349))
  expect_equal(collect(select(df, weekofyear(df$b)))[, 1], c(50, 50, 51))
  expect_equal(collect(select(df, year(df$b)))[, 1], c(2012, 2013, 2014))
  expect_equal(collect(select(df, month(df$b)))[, 1], c(12, 12, 12))
  expect_equal(collect(select(df, last_day(df$b)))[, 1],
               c(as.Date("2012-12-31"), as.Date("2013-12-31"), as.Date("2014-12-31")))
  expect_equal(collect(select(df, next_day(df$b, "MONDAY")))[, 1],
               c(as.Date("2012-12-17"), as.Date("2013-12-16"), as.Date("2014-12-22")))
  expect_equal(collect(select(df, date_format(df$b, "y")))[, 1], c("2012", "2013", "2014"))
  expect_equal(collect(select(df, add_months(df$b, 3)))[, 1],
               c(as.Date("2013-03-13"), as.Date("2014-03-14"), as.Date("2015-03-15")))
  expect_equal(collect(select(df, date_add(df$b, 1)))[, 1],
               c(as.Date("2012-12-14"), as.Date("2013-12-15"), as.Date("2014-12-16")))
  expect_equal(collect(select(df, date_sub(df$b, 1)))[, 1],
               c(as.Date("2012-12-12"), as.Date("2013-12-13"), as.Date("2014-12-14")))

  l2 <- list(list(a = 1L, b = as.POSIXlt("2012-12-13 12:34:00", tz = "UTC")),
            list(a = 2L, b = as.POSIXlt("2014-12-15 01:24:34", tz = "UTC")))
  df2 <- createDataFrame(sqlContext, l2)
  expect_equal(collect(select(df2, minute(df2$b)))[, 1], c(34, 24))
  expect_equal(collect(select(df2, second(df2$b)))[, 1], c(0, 34))
  expect_equal(collect(select(df2, from_utc_timestamp(df2$b, "JST")))[, 1],
               c(as.POSIXlt("2012-12-13 21:34:00 UTC"), as.POSIXlt("2014-12-15 10:24:34 UTC")))
  expect_equal(collect(select(df2, to_utc_timestamp(df2$b, "JST")))[, 1],
               c(as.POSIXlt("2012-12-13 03:34:00 UTC"), as.POSIXlt("2014-12-14 16:24:34 UTC")))
  expect_more_than(collect(select(df2, unix_timestamp()))[1, 1], 0)
  expect_more_than(collect(select(df2, unix_timestamp(df2$b)))[1, 1], 0)
  expect_more_than(collect(select(df2, unix_timestamp(lit("2015-01-01"), "yyyy-MM-dd")))[1, 1], 0)

  l3 <- list(list(a = 1000), list(a = -1000))
  df3 <- createDataFrame(sqlContext, l3)
  result31 <- collect(select(df3, from_unixtime(df3$a)))
  expect_equal(grep("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", result31[, 1], perl = TRUE),
               c(1, 2))
  result32 <- collect(select(df3, from_unixtime(df3$a, "yyyy")))
  expect_equal(grep("\\d{4}", result32[, 1]), c(1, 2))
  Sys.setenv(TZ = .originalTimeZone)
})

test_that("greatest() and least() on a DataFrame", {
  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(sqlContext, l)
  expect_equal(collect(select(df, greatest(df$a, df$b)))[, 1], c(2, 4))
  expect_equal(collect(select(df, least(df$a, df$b)))[, 1], c(1, 3))
})

test_that("when(), otherwise() and ifelse() on a DataFrame", {
  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(sqlContext, l)
  expect_equal(collect(select(df, when(df$a > 1 & df$b > 2, 1)))[, 1], c(NA, 1))
  expect_equal(collect(select(df, otherwise(when(df$a > 1, 1), 0)))[, 1], c(0, 1))
  expect_equal(collect(select(df, ifelse(df$a > 1 & df$b > 2, 0, 1)))[, 1], c(1, 0))
})

test_that("group by", {
  df <- jsonFile(sqlContext, jsonPath)
  df1 <- agg(df, name = "max", age = "sum")
  expect_equal(1, count(df1))
  df1 <- agg(df, age2 = max(df$age))
  expect_equal(1, count(df1))
  expect_equal(columns(df1), c("age2"))

  gd <- groupBy(df, "name")
  expect_is(gd, "GroupedData")
  df2 <- count(gd)
  expect_is(df2, "DataFrame")
  expect_equal(3, count(df2))

  # Also test group_by, summarize, mean
  gd1 <- group_by(df, "name")
  expect_is(gd1, "GroupedData")
  df_summarized <- summarize(gd, mean_age = mean(df$age))
  expect_is(df_summarized, "DataFrame")
  expect_equal(3, count(df_summarized))

  df3 <- agg(gd, age = "sum")
  expect_is(df3, "DataFrame")
  expect_equal(3, count(df3))

  df3 <- agg(gd, age = sum(df$age))
  expect_is(df3, "DataFrame")
  expect_equal(3, count(df3))
  expect_equal(columns(df3), c("name", "age"))

  df4 <- sum(gd, "age")
  expect_is(df4, "DataFrame")
  expect_equal(3, count(df4))
  expect_equal(3, count(mean(gd, "age")))
  expect_equal(3, count(max(gd, "age")))
})

test_that("arrange() and orderBy() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  sorted <- arrange(df, df$age)
  expect_equal(collect(sorted)[1,2], "Michael")

  sorted2 <- arrange(df, "name")
  expect_equal(collect(sorted2)[2,"age"], 19)

  sorted3 <- orderBy(df, asc(df$age))
  expect_true(is.na(first(sorted3)$age))
  expect_equal(collect(sorted3)[2, "age"], 19)

  sorted4 <- orderBy(df, desc(df$name))
  expect_equal(first(sorted4)$name, "Michael")
  expect_equal(collect(sorted4)[3,"name"], "Andy")
})

test_that("filter() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  filtered <- filter(df, "age > 20")
  expect_equal(count(filtered), 1)
  expect_equal(collect(filtered)$name, "Andy")
  filtered2 <- where(df, df$name != "Michael")
  expect_equal(count(filtered2), 2)
  expect_equal(collect(filtered2)$age[2], 19)

  # test suites for %in%
  filtered3 <- filter(df, "age in (19)")
  expect_equal(count(filtered3), 1)
  filtered4 <- filter(df, "age in (19, 30)")
  expect_equal(count(filtered4), 2)
  filtered5 <- where(df, df$age %in% c(19))
  expect_equal(count(filtered5), 1)
  filtered6 <- where(df, df$age %in% c(19, 30))
  expect_equal(count(filtered6), 2)
})

test_that("join() and merge() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)

  mockLines2 <- c("{\"name\":\"Michael\", \"test\": \"yes\"}",
                  "{\"name\":\"Andy\",  \"test\": \"no\"}",
                  "{\"name\":\"Justin\", \"test\": \"yes\"}",
                  "{\"name\":\"Bob\", \"test\": \"yes\"}")
  jsonPath2 <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(mockLines2, jsonPath2)
  df2 <- jsonFile(sqlContext, jsonPath2)

  joined <- join(df, df2)
  expect_equal(names(joined), c("age", "name", "name", "test"))
  expect_equal(count(joined), 12)

  joined2 <- join(df, df2, df$name == df2$name)
  expect_equal(names(joined2), c("age", "name", "name", "test"))
  expect_equal(count(joined2), 3)

  joined3 <- join(df, df2, df$name == df2$name, "right_outer")
  expect_equal(names(joined3), c("age", "name", "name", "test"))
  expect_equal(count(joined3), 4)
  expect_true(is.na(collect(orderBy(joined3, joined3$age))$age[2]))

  joined4 <- select(join(df, df2, df$name == df2$name, "outer"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined4), c("newAge", "name", "test"))
  expect_equal(count(joined4), 4)
  expect_equal(collect(orderBy(joined4, joined4$name))$newAge[3], 24)

  merged <- select(merge(df, df2, df$name == df2$name, "outer"),
                   alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(merged), c("newAge", "name", "test"))
  expect_equal(count(merged), 4)
  expect_equal(collect(orderBy(merged, joined4$name))$newAge[3], 24)
})

test_that("toJSON() returns an RDD of the correct values", {
  df <- jsonFile(sqlContext, jsonPath)
  testRDD <- toJSON(df)
  expect_is(testRDD, "RDD")
  expect_equal(SparkR:::getSerializedMode(testRDD), "string")
  expect_equal(collect(testRDD)[[1]], mockLines[1])
})

test_that("showDF()", {
  df <- jsonFile(sqlContext, jsonPath)
  s <- capture.output(showDF(df))
  expected <- paste("+----+-------+\n",
                    "| age|   name|\n",
                    "+----+-------+\n",
                    "|null|Michael|\n",
                    "|  30|   Andy|\n",
                    "|  19| Justin|\n",
                    "+----+-------+\n", sep="")
  expect_output(s , expected)
})

test_that("isLocal()", {
  df <- jsonFile(sqlContext, jsonPath)
  expect_false(isLocal(df))
})

test_that("unionAll(), rbind(), except(), and intersect() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)

  lines <- c("{\"name\":\"Bob\", \"age\":24}",
             "{\"name\":\"Andy\", \"age\":30}",
             "{\"name\":\"James\", \"age\":35}")
  jsonPath2 <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(lines, jsonPath2)
  df2 <- read.df(sqlContext, jsonPath2, "json")

  unioned <- arrange(unionAll(df, df2), df$age)
  expect_is(unioned, "DataFrame")
  expect_equal(count(unioned), 6)
  expect_equal(first(unioned)$name, "Michael")

  unioned2 <- arrange(rbind(unioned, df, df2), df$age)
  expect_is(unioned2, "DataFrame")
  expect_equal(count(unioned2), 12)
  expect_equal(first(unioned2)$name, "Michael")

  excepted <- arrange(except(df, df2), desc(df$age))
  expect_is(unioned, "DataFrame")
  expect_equal(count(excepted), 2)
  expect_equal(first(excepted)$name, "Justin")

  intersected <- arrange(intersect(df, df2), df$age)
  expect_is(unioned, "DataFrame")
  expect_equal(count(intersected), 1)
  expect_equal(first(intersected)$name, "Andy")
})

test_that("withColumn() and withColumnRenamed()", {
  df <- jsonFile(sqlContext, jsonPath)
  newDF <- withColumn(df, "newAge", df$age + 2)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 32)

  newDF2 <- withColumnRenamed(df, "age", "newerAge")
  expect_equal(length(columns(newDF2)), 2)
  expect_equal(columns(newDF2)[1], "newerAge")
})

test_that("mutate(), transform(), rename() and names()", {
  df <- jsonFile(sqlContext, jsonPath)
  newDF <- mutate(df, newAge = df$age + 2)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 32)

  newDF2 <- rename(df, newerAge = df$age)
  expect_equal(length(columns(newDF2)), 2)
  expect_equal(columns(newDF2)[1], "newerAge")

  names(newDF2) <- c("newerName", "evenNewerAge")
  expect_equal(length(names(newDF2)), 2)
  expect_equal(names(newDF2)[1], "newerName")

  transformedDF <- transform(df, newAge = -df$age, newAge2 = df$age / 2)
  expect_equal(length(columns(transformedDF)), 4)
  expect_equal(columns(transformedDF)[3], "newAge")
  expect_equal(columns(transformedDF)[4], "newAge2")
  expect_equal(first(filter(transformedDF, transformedDF$name == "Andy"))$newAge, -30)

  # test if transform on local data frames works
  # ensure the proper signature is used - otherwise this will fail to run
  attach(airquality)
  result <- transform(Ozone, logOzone = log(Ozone))
  expect_equal(nrow(result), 153)
  expect_equal(ncol(result), 2)
  detach(airquality)
})

test_that("write.df() on DataFrame and works with parquetFile", {
  df <- jsonFile(sqlContext, jsonPath)
  write.df(df, parquetPath, "parquet", mode="overwrite")
  parquetDF <- parquetFile(sqlContext, parquetPath)
  expect_is(parquetDF, "DataFrame")
  expect_equal(count(df), count(parquetDF))
})

test_that("parquetFile works with multiple input paths", {
  df <- jsonFile(sqlContext, jsonPath)
  write.df(df, parquetPath, "parquet", mode="overwrite")
  parquetPath2 <- tempfile(pattern = "parquetPath2", fileext = ".parquet")
  write.df(df, parquetPath2, "parquet", mode="overwrite")
  parquetDF <- parquetFile(sqlContext, parquetPath, parquetPath2)
  expect_is(parquetDF, "DataFrame")
  expect_equal(count(parquetDF), count(df) * 2)

  # Test if varargs works with variables
  saveMode <- "overwrite"
  mergeSchema <- "true"
  parquetPath3 <- tempfile(pattern = "parquetPath3", fileext = ".parquet")
  write.df(df, parquetPath2, "parquet", mode = saveMode, mergeSchema = mergeSchema)
})

test_that("describe() and summarize() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPath)
  stats <- describe(df, "age")
  expect_equal(collect(stats)[1, "summary"], "count")
  expect_equal(collect(stats)[2, "age"], "24.5")
  expect_equal(collect(stats)[3, "age"], "5.5")
  stats <- describe(df)
  expect_equal(collect(stats)[4, "name"], "Andy")
  expect_equal(collect(stats)[5, "age"], "30")

  stats2 <- summary(df)
  expect_equal(collect(stats2)[4, "name"], "Andy")
  expect_equal(collect(stats2)[5, "age"], "30")
})

test_that("dropna() and na.omit() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPathNa)
  rows <- collect(df)

  # drop with columns

  expected <- rows[!is.na(rows$name),]
  actual <- collect(dropna(df, cols = "name"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, cols = "name"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age),]
  actual <- collect(dropna(df, cols = "age"))
  row.names(expected) <- row.names(actual)
  # identical on two dataframes does not work here. Don't know why.
  # use identical on all columns as a workaround.
  expect_identical(expected$age, actual$age)
  expect_identical(expected$height, actual$height)
  expect_identical(expected$name, actual$name)
  actual <- collect(na.omit(df, cols = "age"))

  expected <- rows[!is.na(rows$age) & !is.na(rows$height),]
  actual <- collect(dropna(df, cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name),]
  actual <- collect(dropna(df))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df))
  expect_identical(expected, actual)

  # drop with how

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name),]
  actual <- collect(dropna(df))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) | !is.na(rows$height) | !is.na(rows$name),]
  actual <- collect(dropna(df, "all"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "all"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name),]
  actual <- collect(dropna(df, "any"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "any"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height),]
  actual <- collect(dropna(df, "any", cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "any", cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) | !is.na(rows$height),]
  actual <- collect(dropna(df, "all", cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "all", cols = c("age", "height")))
  expect_identical(expected, actual)

  # drop with threshold

  expected <- rows[as.integer(!is.na(rows$age)) + as.integer(!is.na(rows$height)) >= 2,]
  actual <- collect(dropna(df, minNonNulls = 2, cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, minNonNulls = 2, cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[as.integer(!is.na(rows$age)) +
                   as.integer(!is.na(rows$height)) +
                   as.integer(!is.na(rows$name)) >= 3,]
  actual <- collect(dropna(df, minNonNulls = 3, cols = c("name", "age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, minNonNulls = 3, cols = c("name", "age", "height")))
  expect_identical(expected, actual)
})

test_that("fillna() on a DataFrame", {
  df <- jsonFile(sqlContext, jsonPathNa)
  rows <- collect(df)

  # fill with value

  expected <- rows
  expected$age[is.na(expected$age)] <- 50
  expected$height[is.na(expected$height)] <- 50.6
  actual <- collect(fillna(df, 50.6))
  expect_identical(expected, actual)

  expected <- rows
  expected$name[is.na(expected$name)] <- "unknown"
  actual <- collect(fillna(df, "unknown"))
  expect_identical(expected, actual)

  expected <- rows
  expected$age[is.na(expected$age)] <- 50
  actual <- collect(fillna(df, 50.6, "age"))
  expect_identical(expected, actual)

  expected <- rows
  expected$name[is.na(expected$name)] <- "unknown"
  actual <- collect(fillna(df, "unknown", c("age", "name")))
  expect_identical(expected, actual)

  # fill with named list

  expected <- rows
  expected$age[is.na(expected$age)] <- 50
  expected$height[is.na(expected$height)] <- 50.6
  expected$name[is.na(expected$name)] <- "unknown"
  actual <- collect(fillna(df, list("age" = 50, "height" = 50.6, "name" = "unknown")))
  expect_identical(expected, actual)
})

test_that("crosstab() on a DataFrame", {
  rdd <- lapply(parallelize(sc, 0:3), function(x) {
    list(paste0("a", x %% 3), paste0("b", x %% 2))
  })
  df <- toDF(rdd, list("a", "b"))
  ct <- crosstab(df, "a", "b")
  ordered <- ct[order(ct$a_b),]
  row.names(ordered) <- NULL
  expected <- data.frame("a_b" = c("a0", "a1", "a2"), "b0" = c(1, 0, 1), "b1" = c(1, 1, 0),
                         stringsAsFactors = FALSE, row.names = NULL)
  expect_identical(expected, ordered)
})

test_that("SQL error message is returned from JVM", {
  retError <- tryCatch(sql(sqlContext, "select * from blah"), error = function(e) e)
  expect_equal(grepl("Table Not Found: blah", retError), TRUE)
})

unlink(parquetPath)
unlink(jsonPath)
unlink(jsonPathNa)
