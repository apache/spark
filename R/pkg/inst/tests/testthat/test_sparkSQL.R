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

markUtf8 <- function(s) {
  Encoding(s) <- "UTF-8"
  s
}

setHiveContext <- function(sc) {
  if (exists(".testHiveSession", envir = .sparkREnv)) {
    hiveSession <- get(".testHiveSession", envir = .sparkREnv)
  } else {
    # initialize once and reuse
    ssc <- callJMethod(sc, "sc")
    hiveCtx <- tryCatch({
      newJObject("org.apache.spark.sql.hive.test.TestHiveContext", ssc, FALSE)
    },
    error = function(err) {
      skip("Hive is not build with SparkSQL, skipped")
    })
    hiveSession <- callJMethod(hiveCtx, "sparkSession")
  }
  previousSession <- get(".sparkRsession", envir = .sparkREnv)
  assign(".sparkRsession", hiveSession, envir = .sparkREnv)
  assign(".prevSparkRsession", previousSession, envir = .sparkREnv)
  hiveSession
}

unsetHiveContext <- function() {
  previousSession <- get(".prevSparkRsession", envir = .sparkREnv)
  assign(".sparkRsession", previousSession, envir = .sparkREnv)
  remove(".prevSparkRsession", envir = .sparkREnv)
}

# Tests for SparkSQL functions in SparkR

sparkSession <- sparkR.session()
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

mockLines <- c("{\"name\":\"Michael\"}",
               "{\"name\":\"Andy\", \"age\":30}",
               "{\"name\":\"Justin\", \"age\":19}")
jsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
parquetPath <- tempfile(pattern = "sparkr-test", fileext = ".parquet")
orcPath <- tempfile(pattern = "sparkr-test", fileext = ".orc")
writeLines(mockLines, jsonPath)

# For test nafunctions, like dropna(), fillna(),...
mockLinesNa <- c("{\"name\":\"Bob\",\"age\":16,\"height\":176.5}",
                 "{\"name\":\"Alice\",\"age\":null,\"height\":164.3}",
                 "{\"name\":\"David\",\"age\":60,\"height\":null}",
                 "{\"name\":\"Amy\",\"age\":null,\"height\":null}",
                 "{\"name\":null,\"age\":null,\"height\":null}")
jsonPathNa <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
writeLines(mockLinesNa, jsonPathNa)

# For test complex types in DataFrame
mockLinesComplexType <-
  c("{\"c1\":[1, 2, 3], \"c2\":[\"a\", \"b\", \"c\"], \"c3\":[1.0, 2.0, 3.0]}",
    "{\"c1\":[4, 5, 6], \"c2\":[\"d\", \"e\", \"f\"], \"c3\":[4.0, 5.0, 6.0]}",
    "{\"c1\":[7, 8, 9], \"c2\":[\"g\", \"h\", \"i\"], \"c3\":[7.0, 8.0, 9.0]}")
complexTypeJsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
writeLines(mockLinesComplexType, complexTypeJsonPath)

test_that("calling sparkRSQL.init returns existing SQL context", {
  sqlContext <- suppressWarnings(sparkRSQL.init(sc))
  expect_equal(suppressWarnings(sparkRSQL.init(sc)), sqlContext)
})

test_that("calling sparkRSQL.init returns existing SparkSession", {
  expect_equal(suppressWarnings(sparkRSQL.init(sc)), sparkSession)
})

test_that("calling sparkR.session returns existing SparkSession", {
  expect_equal(sparkR.session(), sparkSession)
})

test_that("infer types and check types", {
  expect_equal(infer_type(1L), "integer")
  expect_equal(infer_type(1.0), "double")
  expect_equal(infer_type("abc"), "string")
  expect_equal(infer_type(TRUE), "boolean")
  expect_equal(infer_type(as.Date("2015-03-11")), "date")
  expect_equal(infer_type(as.POSIXlt("2015-03-11 12:13:04.043")), "timestamp")
  expect_equal(infer_type(c(1L, 2L)), "array<integer>")
  expect_equal(infer_type(list(1L, 2L)), "array<integer>")
  expect_equal(infer_type(listToStruct(list(a = 1L, b = "2"))), "struct<a:integer,b:string>")
  e <- new.env()
  assign("a", 1L, envir = e)
  expect_equal(infer_type(e), "map<string,integer>")

  expect_error(checkType("map<integer,integer>"), "Key type in a map must be string or character")

  expect_equal(infer_type(as.raw(c(1, 2, 3))), "binary")
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
  df <- createDataFrame(rdd, list("a", "b"))
  dfAsDF <- as.DataFrame(rdd, list("a", "b"))
  expect_is(df, "SparkDataFrame")
  expect_is(dfAsDF, "SparkDataFrame")
  expect_equal(count(df), 10)
  expect_equal(count(dfAsDF), 10)
  expect_equal(nrow(df), 10)
  expect_equal(nrow(dfAsDF), 10)
  expect_equal(ncol(df), 2)
  expect_equal(ncol(dfAsDF), 2)
  expect_equal(dim(df), c(10, 2))
  expect_equal(dim(dfAsDF), c(10, 2))
  expect_equal(columns(df), c("a", "b"))
  expect_equal(columns(dfAsDF), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
  expect_equal(dtypes(dfAsDF), list(c("a", "int"), c("b", "string")))

  df <- createDataFrame(rdd)
  dfAsDF <- as.DataFrame(rdd)
  expect_is(df, "SparkDataFrame")
  expect_is(dfAsDF, "SparkDataFrame")
  expect_equal(columns(df), c("_1", "_2"))
  expect_equal(columns(dfAsDF), c("_1", "_2"))

  schema <- structType(structField(x = "a", type = "integer", nullable = TRUE),
                        structField(x = "b", type = "string", nullable = TRUE))
  df <- createDataFrame(rdd, schema)
  expect_is(df, "SparkDataFrame")
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(a = x, b = as.character(x)) })
  df <- createDataFrame(rdd)
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  schema <- structType(structField("name", "string"), structField("age", "integer"),
                       structField("height", "float"))
  df <- read.df(jsonPathNa, "json", schema)
  df2 <- createDataFrame(toRDD(df), schema)
  df2AsDF <- as.DataFrame(toRDD(df), schema)
  expect_equal(columns(df2), c("name", "age", "height"))
  expect_equal(columns(df2AsDF), c("name", "age", "height"))
  expect_equal(dtypes(df2), list(c("name", "string"), c("age", "int"), c("height", "float")))
  expect_equal(dtypes(df2AsDF), list(c("name", "string"), c("age", "int"), c("height", "float")))
  expect_equal(as.list(collect(where(df2, df2$name == "Bob"))),
               list(name = "Bob", age = 16, height = 176.5))
  expect_equal(as.list(collect(where(df2AsDF, df2AsDF$name == "Bob"))),
               list(name = "Bob", age = 16, height = 176.5))

  localDF <- data.frame(name = c("John", "Smith", "Sarah"),
                        age = c(19L, 23L, 18L),
                        height = c(176.5, 181.4, 173.7))
  df <- createDataFrame(localDF, schema)
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 3)
  expect_equal(columns(df), c("name", "age", "height"))
  expect_equal(dtypes(df), list(c("name", "string"), c("age", "int"), c("height", "float")))
  expect_equal(as.list(collect(where(df, df$name == "John"))),
               list(name = "John", age = 19L, height = 176.5))

  setHiveContext(sc)
  sql("CREATE TABLE people (name string, age double, height float)")
  df <- read.df(jsonPathNa, "json", schema)
  invisible(insertInto(df, "people"))
  expect_equal(collect(sql("SELECT age from people WHERE name = 'Bob'"))$age,
               c(16))
  expect_equal(collect(sql("SELECT height from people WHERE name ='Bob'"))$height,
               c(176.5))
  unsetHiveContext()
})

test_that("read/write csv as DataFrame", {
  csvPath <- tempfile(pattern = "sparkr-test", fileext = ".csv")
  mockLinesCsv <- c("year,make,model,comment,blank",
                   "\"2012\",\"Tesla\",\"S\",\"No comment\",",
                   "1997,Ford,E350,\"Go get one now they are going fast\",",
                   "2015,Chevy,Volt",
                   "NA,Dummy,Placeholder")
  writeLines(mockLinesCsv, csvPath)

  # default "header" is false, inferSchema to handle "year" as "int"
  df <- read.df(csvPath, "csv", header = "true", inferSchema = "true")
  expect_equal(count(df), 4)
  expect_equal(columns(df), c("year", "make", "model", "comment", "blank"))
  expect_equal(sort(unlist(collect(where(df, df$year == 2015)))),
               sort(unlist(list(year = 2015, make = "Chevy", model = "Volt"))))

  # since "year" is "int", let's skip the NA values
  withoutna <- na.omit(df, how = "any", cols = "year")
  expect_equal(count(withoutna), 3)

  unlink(csvPath)
  csvPath <- tempfile(pattern = "sparkr-test", fileext = ".csv")
  mockLinesCsv <- c("year,make,model,comment,blank",
                   "\"2012\",\"Tesla\",\"S\",\"No comment\",",
                   "1997,Ford,E350,\"Go get one now they are going fast\",",
                   "2015,Chevy,Volt",
                   "Empty,Dummy,Placeholder")
  writeLines(mockLinesCsv, csvPath)

  df2 <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "Empty")
  expect_equal(count(df2), 4)
  withoutna2 <- na.omit(df2, how = "any", cols = "year")
  expect_equal(count(withoutna2), 3)
  expect_equal(count(where(withoutna2, withoutna2$make == "Dummy")), 0)

  # writing csv file
  csvPath2 <- tempfile(pattern = "csvtest2", fileext = ".csv")
  write.df(df2, path = csvPath2, "csv", header = "true")
  df3 <- read.df(csvPath2, "csv", header = "true")
  expect_equal(nrow(df3), nrow(df2))
  expect_equal(colnames(df3), colnames(df2))
  csv <- read.csv(file = list.files(csvPath2, pattern = "^part", full.names = T)[[1]])
  expect_equal(colnames(df3), colnames(csv))

  unlink(csvPath)
  unlink(csvPath2)
})

test_that("Support other types for options", {
  csvPath <- tempfile(pattern = "sparkr-test", fileext = ".csv")
  mockLinesCsv <- c("year,make,model,comment,blank",
  "\"2012\",\"Tesla\",\"S\",\"No comment\",",
  "1997,Ford,E350,\"Go get one now they are going fast\",",
  "2015,Chevy,Volt",
  "NA,Dummy,Placeholder")
  writeLines(mockLinesCsv, csvPath)

  csvDf <- read.df(csvPath, "csv", header = "true", inferSchema = "true")
  expected <- read.df(csvPath, "csv", header = TRUE, inferSchema = TRUE)
  expect_equal(collect(csvDf), collect(expected))

  expect_error(read.df(csvPath, "csv", header = TRUE, maxColumns = 3))
  unlink(csvPath)
})

test_that("convert NAs to null type in DataFrames", {
  rdd <- parallelize(sc, list(list(1L, 2L), list(NA, 4L)))
  df <- createDataFrame(rdd, list("a", "b"))
  expect_true(is.na(collect(df)[2, "a"]))
  expect_equal(collect(df)[2, "b"], 4L)

  l <- data.frame(x = 1L, y = c(1L, NA_integer_, 3L))
  df <- createDataFrame(l)
  expect_equal(collect(df)[2, "x"], 1L)
  expect_true(is.na(collect(df)[2, "y"]))

  rdd <- parallelize(sc, list(list(1, 2), list(NA, 4)))
  df <- createDataFrame(rdd, list("a", "b"))
  expect_true(is.na(collect(df)[2, "a"]))
  expect_equal(collect(df)[2, "b"], 4)

  l <- data.frame(x = 1, y = c(1, NA_real_, 3))
  df <- createDataFrame(l)
  expect_equal(collect(df)[2, "x"], 1)
  expect_true(is.na(collect(df)[2, "y"]))

  l <- list("a", "b", NA, "d")
  df <- createDataFrame(l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], "d")

  l <- list("a", "b", NA_character_, "d")
  df <- createDataFrame(l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], "d")

  l <- list(TRUE, FALSE, NA, TRUE)
  df <- createDataFrame(l)
  expect_true(is.na(collect(df)[3, "_1"]))
  expect_equal(collect(df)[4, "_1"], TRUE)
})

test_that("toDF", {
  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
  df <- toDF(rdd, list("a", "b"))
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  df <- toDF(rdd)
  expect_is(df, "SparkDataFrame")
  expect_equal(columns(df), c("_1", "_2"))

  schema <- structType(structField(x = "a", type = "integer", nullable = TRUE),
                        structField(x = "b", type = "string", nullable = TRUE))
  df <- toDF(rdd, schema)
  expect_is(df, "SparkDataFrame")
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))

  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(a = x, b = as.character(x)) })
  df <- toDF(rdd)
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 10)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
})

test_that("create DataFrame from list or data.frame", {
  l <- list(list(1, 2), list(3, 4))
  df <- createDataFrame(l, c("a", "b"))
  expect_equal(columns(df), c("a", "b"))

  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(l)
  expect_equal(columns(df), c("a", "b"))

  a <- 1:3
  b <- c("a", "b", "c")
  ldf <- data.frame(a, b)
  df <- createDataFrame(ldf)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
  expect_equal(count(df), 3)
  ldf2 <- collect(df)
  expect_equal(ldf$a, ldf2$a)

  irisdf <- suppressWarnings(createDataFrame(iris))
  iris_collected <- collect(irisdf)
  expect_equivalent(iris_collected[, -5], iris[, -5])
  expect_equal(iris_collected$Species, as.character(iris$Species))

  mtcarsdf <- createDataFrame(mtcars)
  expect_equivalent(collect(mtcarsdf), mtcars)

  bytes <- as.raw(c(1, 2, 3))
  df <- createDataFrame(list(list(bytes)))
  expect_equal(collect(df)[[1]][[1]], bytes)
})

test_that("create DataFrame with different data types", {
  l <- list(a = 1L, b = 2, c = TRUE, d = "ss", e = as.Date("2012-12-13"),
            f = as.POSIXct("2015-03-15 12:13:14.056"))
  df <- createDataFrame(list(l))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "double"), c("c", "boolean"),
                                c("d", "string"), c("e", "date"), c("f", "timestamp")))
  expect_equal(count(df), 1)
  expect_equal(collect(df), data.frame(l, stringsAsFactors = FALSE))
})

test_that("create DataFrame with complex types", {
  e <- new.env()
  assign("n", 3L, envir = e)

  s <- listToStruct(list(a = "aa", b = 3L))

  l <- list(as.list(1:10), list("a", "b"), e, s)
  df <- createDataFrame(list(l), c("a", "b", "c", "d"))
  expect_equal(dtypes(df), list(c("a", "array<int>"),
                                c("b", "array<string>"),
                                c("c", "map<string,int>"),
                                c("d", "struct<a:string,b:int>")))
  expect_equal(count(df), 1)
  ldf <- collect(df)
  expect_equal(names(ldf), c("a", "b", "c", "d"))
  expect_equal(ldf[1, 1][[1]], l[[1]])
  expect_equal(ldf[1, 2][[1]], l[[2]])

  e <- ldf$c[[1]]
  expect_equal(class(e), "environment")
  expect_equal(ls(e), "n")
  expect_equal(e$n, 3L)

  s <- ldf$d[[1]]
  expect_equal(class(s), "struct")
  expect_equal(s$a, "aa")
  expect_equal(s$b, 3L)
})

test_that("create DataFrame from a data.frame with complex types", {
  ldf <- data.frame(row.names = 1:2)
  ldf$a_list <- list(list(1, 2), list(3, 4))
  ldf$an_envir <- c(as.environment(list(a = 1, b = 2)), as.environment(list(c = 3)))

  sdf <- createDataFrame(ldf)
  collected <- collect(sdf)

  expect_identical(ldf[, 1, FALSE], collected[, 1, FALSE])
  expect_equal(ldf$an_envir, collected$an_envir)
})

# For test map type and struct type in DataFrame
mockLinesMapType <- c("{\"name\":\"Bob\",\"info\":{\"age\":16,\"height\":176.5}}",
                      "{\"name\":\"Alice\",\"info\":{\"age\":20,\"height\":164.3}}",
                      "{\"name\":\"David\",\"info\":{\"age\":60,\"height\":180}}")
mapTypeJsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
writeLines(mockLinesMapType, mapTypeJsonPath)

test_that("Collect DataFrame with complex types", {
  # ArrayType
  df <- read.json(complexTypeJsonPath)
  ldf <- collect(df)
  expect_equal(nrow(ldf), 3)
  expect_equal(ncol(ldf), 3)
  expect_equal(names(ldf), c("c1", "c2", "c3"))
  expect_equal(ldf$c1, list(list(1, 2, 3), list(4, 5, 6), list (7, 8, 9)))
  expect_equal(ldf$c2, list(list("a", "b", "c"), list("d", "e", "f"), list ("g", "h", "i")))
  expect_equal(ldf$c3, list(list(1.0, 2.0, 3.0), list(4.0, 5.0, 6.0), list (7.0, 8.0, 9.0)))

  # MapType
  schema <- structType(structField("name", "string"),
                       structField("info", "map<string,double>"))
  df <- read.df(mapTypeJsonPath, "json", schema)
  expect_equal(dtypes(df), list(c("name", "string"),
                                c("info", "map<string,double>")))
  ldf <- collect(df)
  expect_equal(nrow(ldf), 3)
  expect_equal(ncol(ldf), 2)
  expect_equal(names(ldf), c("name", "info"))
  expect_equal(ldf$name, c("Bob", "Alice", "David"))
  bob <- ldf$info[[1]]
  expect_equal(class(bob), "environment")
  expect_equal(bob$age, 16)
  expect_equal(bob$height, 176.5)

  # StructType
  df <- read.json(mapTypeJsonPath)
  expect_equal(dtypes(df), list(c("info", "struct<age:bigint,height:double>"),
                                c("name", "string")))
  ldf <- collect(df)
  expect_equal(nrow(ldf), 3)
  expect_equal(ncol(ldf), 2)
  expect_equal(names(ldf), c("info", "name"))
  expect_equal(ldf$name, c("Bob", "Alice", "David"))
  bob <- ldf$info[[1]]
  expect_equal(class(bob), "struct")
  expect_equal(bob$age, 16)
  expect_equal(bob$height, 176.5)
})

test_that("read/write json files", {
  # Test read.df
  df <- read.df(jsonPath, "json")
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 3)

  # Test read.df with a user defined schema
  schema <- structType(structField("name", type = "string"),
                       structField("age", type = "double"))

  df1 <- read.df(jsonPath, "json", schema)
  expect_is(df1, "SparkDataFrame")
  expect_equal(dtypes(df1), list(c("name", "string"), c("age", "double")))

  # Test loadDF
  df2 <- loadDF(jsonPath, "json", schema)
  expect_is(df2, "SparkDataFrame")
  expect_equal(dtypes(df2), list(c("name", "string"), c("age", "double")))

  # Test read.json
  df <- read.json(jsonPath)
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 3)

  # Test write.df
  jsonPath2 <- tempfile(pattern = "jsonPath2", fileext = ".json")
  write.df(df, jsonPath2, "json", mode = "overwrite")

  # Test write.json
  jsonPath3 <- tempfile(pattern = "jsonPath3", fileext = ".json")
  write.json(df, jsonPath3)

  # Test read.json()/jsonFile() works with multiple input paths
  jsonDF1 <- read.json(c(jsonPath2, jsonPath3))
  expect_is(jsonDF1, "SparkDataFrame")
  expect_equal(count(jsonDF1), 6)
  # Suppress warnings because jsonFile is deprecated
  jsonDF2 <- suppressWarnings(jsonFile(c(jsonPath2, jsonPath3)))
  expect_is(jsonDF2, "SparkDataFrame")
  expect_equal(count(jsonDF2), 6)

  unlink(jsonPath2)
  unlink(jsonPath3)
})

test_that("read/write json files - compression option", {
  df <- read.df(jsonPath, "json")

  jsonPath <- tempfile(pattern = "jsonPath", fileext = ".json")
  write.json(df, jsonPath, compression = "gzip")
  jsonDF <- read.json(jsonPath)
  expect_is(jsonDF, "SparkDataFrame")
  expect_equal(count(jsonDF), count(df))
  expect_true(length(list.files(jsonPath, pattern = ".gz")) > 0)

  unlink(jsonPath)
})

test_that("jsonRDD() on a RDD with json string", {
  sqlContext <- suppressWarnings(sparkRSQL.init(sc))
  rdd <- parallelize(sc, mockLines)
  expect_equal(countRDD(rdd), 3)
  df <- suppressWarnings(jsonRDD(sqlContext, rdd))
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 3)

  rdd2 <- flatMap(rdd, function(x) c(x, x))
  df <- suppressWarnings(jsonRDD(sqlContext, rdd2))
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 6)
})

test_that("test tableNames and tables", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  expect_equal(length(tableNames()), 1)
  tables <- tables()
  expect_equal(count(tables), 1)

  suppressWarnings(registerTempTable(df, "table2"))
  tables <- tables()
  expect_equal(count(tables), 2)
  suppressWarnings(dropTempTable("table1"))
  dropTempView("table2")

  tables <- tables()
  expect_equal(count(tables), 0)
})

test_that(
  "createOrReplaceTempView() results in a queryable table and sql() results in a new DataFrame", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  newdf <- sql("SELECT * FROM table1 where name = 'Michael'")
  expect_is(newdf, "SparkDataFrame")
  expect_equal(count(newdf), 1)
  dropTempView("table1")

  createOrReplaceTempView(df, "dfView")
  sqlCast <- collect(sql("select cast('2' as decimal) as x from dfView limit 1"))
  out <- capture.output(sqlCast)
  expect_true(is.data.frame(sqlCast))
  expect_equal(names(sqlCast)[1], "x")
  expect_equal(nrow(sqlCast), 1)
  expect_equal(ncol(sqlCast), 1)
  expect_equal(out[1], "  x")
  expect_equal(out[2], "1 2")
  dropTempView("dfView")
})

test_that("test cache, uncache and clearCache", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  cacheTable("table1")
  uncacheTable("table1")
  clearCache()
  dropTempView("table1")
})

test_that("insertInto() on a registered table", {
  df <- read.df(jsonPath, "json")
  write.df(df, parquetPath, "parquet", "overwrite")
  dfParquet <- read.df(parquetPath, "parquet")

  lines <- c("{\"name\":\"Bob\", \"age\":24}",
             "{\"name\":\"James\", \"age\":35}")
  jsonPath2 <- tempfile(pattern = "jsonPath2", fileext = ".tmp")
  parquetPath2 <- tempfile(pattern = "parquetPath2", fileext = ".parquet")
  writeLines(lines, jsonPath2)
  df2 <- read.df(jsonPath2, "json")
  write.df(df2, parquetPath2, "parquet", "overwrite")
  dfParquet2 <- read.df(parquetPath2, "parquet")

  createOrReplaceTempView(dfParquet, "table1")
  insertInto(dfParquet2, "table1")
  expect_equal(count(sql("select * from table1")), 5)
  expect_equal(first(sql("select * from table1 order by age"))$name, "Michael")
  dropTempView("table1")

  createOrReplaceTempView(dfParquet, "table1")
  insertInto(dfParquet2, "table1", overwrite = TRUE)
  expect_equal(count(sql("select * from table1")), 2)
  expect_equal(first(sql("select * from table1 order by age"))$name, "Bob")
  dropTempView("table1")

  unlink(jsonPath2)
  unlink(parquetPath2)
})

test_that("tableToDF() returns a new DataFrame", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  tabledf <- tableToDF("table1")
  expect_is(tabledf, "SparkDataFrame")
  expect_equal(count(tabledf), 3)
  tabledf2 <- tableToDF("table1")
  expect_equal(count(tabledf2), 3)
  dropTempView("table1")
})

test_that("toRDD() returns an RRDD", {
  df <- read.json(jsonPath)
  testRDD <- toRDD(df)
  expect_is(testRDD, "RDD")
  expect_equal(countRDD(testRDD), 3)
})

test_that("union on two RDDs created from DataFrames returns an RRDD", {
  df <- read.json(jsonPath)
  RDD1 <- toRDD(df)
  RDD2 <- toRDD(df)
  unioned <- unionRDD(RDD1, RDD2)
  expect_is(unioned, "RDD")
  expect_equal(getSerializedMode(unioned), "byte")
  expect_equal(collectRDD(unioned)[[2]]$name, "Andy")
})

test_that("union on mixed serialization types correctly returns a byte RRDD", {
  # Byte RDD
  nums <- 1:10
  rdd <- parallelize(sc, nums, 2L)

  # String RDD
  textLines <- c("Michael",
                 "Andy, 30",
                 "Justin, 19")
  textPath <- tempfile(pattern = "sparkr-textLines", fileext = ".tmp")
  writeLines(textLines, textPath)
  textRDD <- textFile(sc, textPath)

  df <- read.json(jsonPath)
  dfRDD <- toRDD(df)

  unionByte <- unionRDD(rdd, dfRDD)
  expect_is(unionByte, "RDD")
  expect_equal(getSerializedMode(unionByte), "byte")
  expect_equal(collectRDD(unionByte)[[1]], 1)
  expect_equal(collectRDD(unionByte)[[12]]$name, "Andy")

  unionString <- unionRDD(textRDD, dfRDD)
  expect_is(unionString, "RDD")
  expect_equal(getSerializedMode(unionString), "byte")
  expect_equal(collectRDD(unionString)[[1]], "Michael")
  expect_equal(collectRDD(unionString)[[5]]$name, "Andy")
})

test_that("objectFile() works with row serialization", {
  objectPath <- tempfile(pattern = "spark-test", fileext = ".tmp")
  df <- read.json(jsonPath)
  dfRDD <- toRDD(df)
  saveAsObjectFile(coalesce(dfRDD, 1L), objectPath)
  objectIn <- objectFile(sc, objectPath)

  expect_is(objectIn, "RDD")
  expect_equal(getSerializedMode(objectIn), "byte")
  expect_equal(collectRDD(objectIn)[[2]]$age, 30)
})

test_that("lapply() on a DataFrame returns an RDD with the correct columns", {
  df <- read.json(jsonPath)
  testRDD <- lapply(df, function(row) {
    row$newCol <- row$age + 5
    row
    })
  expect_is(testRDD, "RDD")
  collected <- collectRDD(testRDD)
  expect_equal(collected[[1]]$name, "Michael")
  expect_equal(collected[[2]]$newCol, 35)
})

test_that("collect() returns a data.frame", {
  df <- read.json(jsonPath)
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

  # collect() correctly handles multiple columns with same name
  df <- createDataFrame(list(list(1, 2)), schema = c("name", "name"))
  ldf <- collect(df)
  expect_equal(names(ldf), c("name", "name"))
})

test_that("limit() returns DataFrame with the correct number of rows", {
  df <- read.json(jsonPath)
  dfLimited <- limit(df, 2)
  expect_is(dfLimited, "SparkDataFrame")
  expect_equal(count(dfLimited), 2)
})

test_that("collect() and take() on a DataFrame return the same number of rows and columns", {
  df <- read.json(jsonPath)
  expect_equal(nrow(collect(df)), nrow(take(df, 10)))
  expect_equal(ncol(collect(df)), ncol(take(df, 10)))
})

test_that("collect() support Unicode characters", {
  lines <- c("{\"name\":\"안녕하세요\"}",
             "{\"name\":\"您好\", \"age\":30}",
             "{\"name\":\"こんにちは\", \"age\":19}",
             "{\"name\":\"Xin chào\"}")

  jsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(lines, jsonPath)

  df <- read.df(jsonPath, "json")
  rdf <- collect(df)
  expect_true(is.data.frame(rdf))
  expect_equal(rdf$name[1], markUtf8("안녕하세요"))
  expect_equal(rdf$name[2], markUtf8("您好"))
  expect_equal(rdf$name[3], markUtf8("こんにちは"))
  expect_equal(rdf$name[4], markUtf8("Xin chào"))

  df1 <- createDataFrame(rdf)
  expect_equal(collect(where(df1, df1$name == markUtf8("您好")))$name, markUtf8("您好"))
})

test_that("multiple pipeline transformations result in an RDD with the correct values", {
  df <- read.json(jsonPath)
  first <- lapply(df, function(row) {
    row$age <- row$age + 5
    row
  })
  second <- lapply(first, function(row) {
    row$testCol <- if (row$age == 35 && !is.na(row$age)) TRUE else FALSE
    row
  })
  expect_is(second, "RDD")
  expect_equal(countRDD(second), 3)
  expect_equal(collectRDD(second)[[2]]$age, 35)
  expect_true(collectRDD(second)[[2]]$testCol)
  expect_false(collectRDD(second)[[3]]$testCol)
})

test_that("cache(), persist(), and unpersist() on a DataFrame", {
  df <- read.json(jsonPath)
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
  df <- read.json(jsonPath)
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

test_that("names() colnames() set the column names", {
  df <- read.json(jsonPath)
  names(df) <- c("col1", "col2")
  expect_equal(colnames(df)[2], "col2")

  colnames(df) <- c("col3", "col4")
  expect_equal(names(df)[1], "col3")

  expect_error(colnames(df) <- c("sepal.length", "sepal_width"),
               "Colum names cannot contain the '.' symbol.")
  expect_error(colnames(df) <- c(1, 2), "Invalid column names.")
  expect_error(colnames(df) <- c("a"),
               "Column names must have the same length as the number of columns in the dataset.")
  expect_error(colnames(df) <- c("1", NA), "Column names cannot be NA.")

  # Note: if this test is broken, remove check for "." character on colnames<- method
  irisDF <- suppressWarnings(createDataFrame(iris))
  expect_equal(names(irisDF)[1], "Sepal_Length")

  # Test base::colnames base::names
  m2 <- cbind(1, 1:4)
  expect_equal(colnames(m2, do.NULL = FALSE), c("col1", "col2"))
  colnames(m2) <- c("x", "Y")
  expect_equal(colnames(m2), c("x", "Y"))

  z <- list(a = 1, b = "c", c = 1:3)
  expect_equal(names(z)[3], "c")
  names(z)[3] <- "c2"
  expect_equal(names(z)[3], "c2")
})

test_that("head() and first() return the correct data", {
  df <- read.json(jsonPath)
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

test_that("distinct(), unique() and dropDuplicates() on DataFrames", {
  lines <- c("{\"name\":\"Michael\"}",
             "{\"name\":\"Andy\", \"age\":30}",
             "{\"name\":\"Justin\", \"age\":19}",
             "{\"name\":\"Justin\", \"age\":19}")
  jsonPathWithDup <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(lines, jsonPathWithDup)

  df <- read.json(jsonPathWithDup)
  uniques <- distinct(df)
  expect_is(uniques, "SparkDataFrame")
  expect_equal(count(uniques), 3)

  uniques2 <- unique(df)
  expect_is(uniques2, "SparkDataFrame")
  expect_equal(count(uniques2), 3)

  # Test dropDuplicates()
  df <- createDataFrame(
    list(
      list(2, 1, 2), list(1, 1, 1),
      list(1, 2, 1), list(2, 1, 2),
      list(2, 2, 2), list(2, 2, 1),
      list(2, 1, 1), list(1, 1, 2),
      list(1, 2, 2), list(1, 2, 1)),
    schema = c("key", "value1", "value2"))
  result <- collect(dropDuplicates(df))
  expected <- rbind.data.frame(
    c(1, 1, 1), c(1, 1, 2), c(1, 2, 1),
    c(1, 2, 2), c(2, 1, 1), c(2, 1, 2),
    c(2, 2, 1), c(2, 2, 2))
  names(expected) <- c("key", "value1", "value2")
  expect_equivalent(
    result[order(result$key, result$value1, result$value2), ],
    expected)

  result <- collect(dropDuplicates(df, c("key", "value1")))
  expected <- rbind.data.frame(
    c(1, 1, 1), c(1, 2, 1), c(2, 1, 2), c(2, 2, 2))
  names(expected) <- c("key", "value1", "value2")
  expect_equivalent(
    result[order(result$key, result$value1, result$value2), ],
    expected)

  result <- collect(dropDuplicates(df, "key", "value1"))
  expected <- rbind.data.frame(
    c(1, 1, 1), c(1, 2, 1), c(2, 1, 2), c(2, 2, 2))
  names(expected) <- c("key", "value1", "value2")
  expect_equivalent(
    result[order(result$key, result$value1, result$value2), ],
    expected)

  result <- collect(dropDuplicates(df, "key"))
  expected <- rbind.data.frame(
    c(1, 1, 1), c(2, 1, 2))
  names(expected) <- c("key", "value1", "value2")
  expect_equivalent(
    result[order(result$key, result$value1, result$value2), ],
    expected)
})

test_that("sample on a DataFrame", {
  df <- read.json(jsonPath)
  sampled <- sample(df, FALSE, 1.0)
  expect_equal(nrow(collect(sampled)), count(df))
  expect_is(sampled, "SparkDataFrame")
  sampled2 <- sample(df, FALSE, 0.1, 0) # set seed for predictable result
  expect_true(count(sampled2) < 3)

  count1 <- count(sample(df, FALSE, 0.1, 0))
  count2 <- count(sample(df, FALSE, 0.1, 0))
  expect_equal(count1, count2)

  # Also test sample_frac
  sampled3 <- sample_frac(df, FALSE, 0.1, 0) # set seed for predictable result
  expect_true(count(sampled3) < 3)

  # nolint start
  # Test base::sample is working
  #expect_equal(length(sample(1:12)), 12)
  # nolint end
})

test_that("select operators", {
  df <- select(read.json(jsonPath), "name", "age")
  expect_is(df$name, "Column")
  expect_is(df[[2]], "Column")
  expect_is(df[["age"]], "Column")

  expect_is(df[, 1, drop = F], "SparkDataFrame")
  expect_equal(columns(df[, 1, drop = F]), c("name"))
  expect_equal(columns(df[, "age", drop = F]), c("age"))

  df2 <- df[, c("age", "name")]
  expect_is(df2, "SparkDataFrame")
  expect_equal(columns(df2), c("age", "name"))

  df$age2 <- df$age
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == df$age)), 2)
  df$age2 <- df$age * 2
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == df$age * 2)), 2)

  # Test parameter drop
  expect_equal(class(df[, 1]) == "SparkDataFrame", T)
  expect_equal(class(df[, 1, drop = T]) == "Column", T)
  expect_equal(class(df[, 1, drop = F]) == "SparkDataFrame", T)
  expect_equal(class(df[df$age > 4, 2, drop = T]) == "Column", T)
  expect_equal(class(df[df$age > 4, 2, drop = F]) == "SparkDataFrame", T)
})

test_that("select with column", {
  df <- read.json(jsonPath)
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

  df4 <- select(df, c("name", "age"))
  expect_equal(columns(df4), c("name", "age"))
  expect_equal(count(df4), 3)

  expect_error(select(df, c("name", "age"), "name"),
                "To select multiple columns, use a character vector or list for col")
})

test_that("drop column", {
  df <- select(read.json(jsonPath), "name", "age")
  df1 <- drop(df, "name")
  expect_equal(columns(df1), c("age"))

  df$age2 <- df$age
  df1 <- drop(df, c("name", "age"))
  expect_equal(columns(df1), c("age2"))

  df1 <- drop(df, df$age)
  expect_equal(columns(df1), c("name", "age2"))

  df$age2 <- NULL
  expect_equal(columns(df), c("name", "age"))
  df$age3 <- NULL
  expect_equal(columns(df), c("name", "age"))

  # Test to make sure base::drop is not masked
  expect_equal(drop(1:3 %*% 2:4), 20)
})

test_that("subsetting", {
  # read.json returns columns in random order
  df <- select(read.json(jsonPath), "name", "age")
  filtered <- df[df$age > 20, ]
  expect_equal(count(filtered), 1)
  expect_equal(columns(filtered), c("name", "age"))
  expect_equal(collect(filtered)$name, "Andy")

  df2 <- df[df$age == 19, 1, drop = F]
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df2), 1)
  expect_equal(columns(df2), c("name"))
  expect_equal(collect(df2)$name, "Justin")

  df3 <- df[df$age > 20, 2, drop = F]
  expect_equal(count(df3), 1)
  expect_equal(columns(df3), c("age"))

  df4 <- df[df$age %in% c(19, 30), 1:2]
  expect_equal(count(df4), 2)
  expect_equal(columns(df4), c("name", "age"))

  df5 <- df[df$age %in% c(19), c(1, 2)]
  expect_equal(count(df5), 1)
  expect_equal(columns(df5), c("name", "age"))

  df6 <- subset(df, df$age %in% c(30), c(1, 2))
  expect_equal(count(df6), 1)
  expect_equal(columns(df6), c("name", "age"))

  df7 <- subset(df, select = "name", drop = F)
  expect_equal(count(df7), 3)
  expect_equal(columns(df7), c("name"))

  # Test base::subset is working
  expect_equal(nrow(subset(airquality, Temp > 80, select = c(Ozone, Temp))), 68)
})

test_that("selectExpr() on a DataFrame", {
  df <- read.json(jsonPath)
  selected <- selectExpr(df, "age * 2")
  expect_equal(names(selected), "(age * 2)")
  expect_equal(collect(selected), collect(select(df, df$age * 2L)))

  selected2 <- selectExpr(df, "name as newName", "abs(age) as age")
  expect_equal(names(selected2), c("newName", "age"))
  expect_equal(count(selected2), 3)
})

test_that("expr() on a DataFrame", {
  df <- read.json(jsonPath)
  expect_equal(collect(select(df, expr("abs(-123)")))[1, 1], 123)
})

test_that("column calculation", {
  df <- read.json(jsonPath)
  d <- collect(select(df, alias(df$age + 1, "age2")))
  expect_equal(names(d), c("age2"))
  df2 <- select(df, lower(df$name), abs(df$age))
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df2), 3)
})

test_that("test HiveContext", {
  setHiveContext(sc)
  df <- createExternalTable("json", jsonPath, "json")
  expect_is(df, "SparkDataFrame")
  expect_equal(count(df), 3)
  df2 <- sql("select * from json")
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df2), 3)

  jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  invisible(saveAsTable(df, "json2", "json", "append", path = jsonPath2))
  df3 <- sql("select * from json2")
  expect_is(df3, "SparkDataFrame")
  expect_equal(count(df3), 3)
  unlink(jsonPath2)

  hivetestDataPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  invisible(saveAsTable(df, "hivetestbl", path = hivetestDataPath))
  df4 <- sql("select * from hivetestbl")
  expect_is(df4, "SparkDataFrame")
  expect_equal(count(df4), 3)
  unlink(hivetestDataPath)

  parquetDataPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  invisible(saveAsTable(df, "parquetest", "parquet", mode = "overwrite", path = parquetDataPath))
  df5 <- sql("select * from parquetest")
  expect_is(df5, "SparkDataFrame")
  expect_equal(count(df5), 3)
  unlink(parquetDataPath)
  unsetHiveContext()
})

test_that("column operators", {
  c <- column("a")
  c2 <- (- c + 1 - 2) * 3 / 4.0
  c3 <- (c + c2 - c2) * c2 %% c2
  c4 <- (c > c2) & (c2 <= c3) | (c == c2) & (c2 != c3)
  c5 <- c2 ^ c3 ^ c4
})

test_that("column functions", {
  c <- column("a")
  c1 <- abs(c) + acos(c) + approxCountDistinct(c) + ascii(c) + asin(c) + atan(c)
  c2 <- avg(c) + base64(c) + bin(c) + bitwiseNOT(c) + cbrt(c) + ceil(c) + cos(c)
  c3 <- cosh(c) + count(c) + crc32(c) + hash(c) + exp(c)
  c4 <- explode(c) + expm1(c) + factorial(c) + first(c) + floor(c) + hex(c)
  c5 <- hour(c) + initcap(c) + last(c) + last_day(c) + length(c)
  c6 <- log(c) + (c) + log1p(c) + log2(c) + lower(c) + ltrim(c) + max(c) + md5(c)
  c7 <- mean(c) + min(c) + month(c) + negate(c) + posexplode(c) + quarter(c)
  c8 <- reverse(c) + rint(c) + round(c) + rtrim(c) + sha1(c) + monotonically_increasing_id()
  c9 <- signum(c) + sin(c) + sinh(c) + size(c) + stddev(c) + soundex(c) + sqrt(c) + sum(c)
  c10 <- sumDistinct(c) + tan(c) + tanh(c) + toDegrees(c) + toRadians(c)
  c11 <- to_date(c) + trim(c) + unbase64(c) + unhex(c) + upper(c)
  c12 <- variance(c)
  c13 <- lead("col", 1) + lead(c, 1) + lag("col", 1) + lag(c, 1)
  c14 <- cume_dist() + ntile(1) + corr(c, c1)
  c15 <- dense_rank() + percent_rank() + rank() + row_number()
  c16 <- is.nan(c) + isnan(c) + isNaN(c)
  c17 <- cov(c, c1) + cov("c", "c1") + covar_samp(c, c1) + covar_samp("c", "c1")
  c18 <- covar_pop(c, c1) + covar_pop("c", "c1")
  c19 <- spark_partition_id()

  # Test if base::is.nan() is exposed
  expect_equal(is.nan(c("a", "b")), c(FALSE, FALSE))

  # Test if base::rank() is exposed
  expect_equal(class(rank())[[1]], "Column")
  expect_equal(rank(1:3), as.numeric(c(1:3)))

  df <- read.json(jsonPath)
  df2 <- select(df, between(df$age, c(20, 30)), between(df$age, c(10, 20)))
  expect_equal(collect(df2)[[2, 1]], TRUE)
  expect_equal(collect(df2)[[2, 2]], FALSE)
  expect_equal(collect(df2)[[3, 1]], FALSE)
  expect_equal(collect(df2)[[3, 2]], TRUE)

  df3 <- select(df, between(df$name, c("Apache", "Spark")))
  expect_equal(collect(df3)[[1, 1]], TRUE)
  expect_equal(collect(df3)[[2, 1]], FALSE)
  expect_equal(collect(df3)[[3, 1]], TRUE)

  df4 <- select(df, countDistinct(df$age, df$name))
  expect_equal(collect(df4)[[1, 1]], 2)

  expect_equal(collect(select(df, sum(df$age)))[1, 1], 49)
  expect_true(abs(collect(select(df, stddev(df$age)))[1, 1] - 7.778175) < 1e-6)
  expect_equal(collect(select(df, var_pop(df$age)))[1, 1], 30.25)

  df5 <- createDataFrame(list(list(a = "010101")))
  expect_equal(collect(select(df5, conv(df5$a, 2, 16)))[1, 1], "15")

  # Test array_contains() and sort_array()
  df <- createDataFrame(list(list(list(1L, 2L, 3L)), list(list(6L, 5L, 4L))))
  result <- collect(select(df, array_contains(df[[1]], 1L)))[[1]]
  expect_equal(result, c(TRUE, FALSE))

  result <- collect(select(df, sort_array(df[[1]], FALSE)))[[1]]
  expect_equal(result, list(list(3L, 2L, 1L), list(6L, 5L, 4L)))
  result <- collect(select(df, sort_array(df[[1]])))[[1]]
  expect_equal(result, list(list(1L, 2L, 3L), list(4L, 5L, 6L)))

  # Test that stats::lag is working
  expect_equal(length(lag(ldeaths, 12)), 72)

  # Test struct()
  df <- createDataFrame(list(list(1L, 2L, 3L), list(4L, 5L, 6L)),
                        schema = c("a", "b", "c"))
  result <- collect(select(df, struct("a", "c")))
  expected <- data.frame(row.names = 1:2)
  expected$"struct(a, c)" <- list(listToStruct(list(a = 1L, c = 3L)),
                                 listToStruct(list(a = 4L, c = 6L)))
  expect_equal(result, expected)

  result <- collect(select(df, struct(df$a, df$b)))
  expected <- data.frame(row.names = 1:2)
  expected$"struct(a, b)" <- list(listToStruct(list(a = 1L, b = 2L)),
                                 listToStruct(list(a = 4L, b = 5L)))
  expect_equal(result, expected)

  # Test encode(), decode()
  bytes <- as.raw(c(0xe5, 0xa4, 0xa7, 0xe5, 0x8d, 0x83, 0xe4, 0xb8, 0x96, 0xe7, 0x95, 0x8c))
  df <- createDataFrame(list(list(markUtf8("大千世界"), "utf-8", bytes)),
                        schema = c("a", "b", "c"))
  result <- collect(select(df, encode(df$a, "utf-8"), decode(df$c, "utf-8")))
  expect_equal(result[[1]][[1]], bytes)
  expect_equal(result[[2]], markUtf8("大千世界"))

  # Test first(), last()
  df <- read.json(jsonPath)
  expect_equal(collect(select(df, first(df$age)))[[1]], NA)
  expect_equal(collect(select(df, first(df$age, TRUE)))[[1]], 30)
  expect_equal(collect(select(df, first("age")))[[1]], NA)
  expect_equal(collect(select(df, first("age", TRUE)))[[1]], 30)
  expect_equal(collect(select(df, last(df$age)))[[1]], 19)
  expect_equal(collect(select(df, last(df$age, TRUE)))[[1]], 19)
  expect_equal(collect(select(df, last("age")))[[1]], 19)
  expect_equal(collect(select(df, last("age", TRUE)))[[1]], 19)

  # Test bround()
  df <- createDataFrame(data.frame(x = c(2.5, 3.5)))
  expect_equal(collect(select(df, bround(df$x, 0)))[[1]][1], 2)
  expect_equal(collect(select(df, bround(df$x, 0)))[[1]][2], 4)
})

test_that("column binary mathfunctions", {
  lines <- c("{\"a\":1, \"b\":5}",
             "{\"a\":2, \"b\":6}",
             "{\"a\":3, \"b\":7}",
             "{\"a\":4, \"b\":8}")
  jsonPathWithDup <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(lines, jsonPathWithDup)
  df <- read.json(jsonPathWithDup)
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
  expect_equal(collect(select(df, rand(1)))[1, 1], 0.134, tolerance = 0.01)
  expect_equal(class(collect(select(df, randn()))[2, 1]), "numeric")
  expect_equal(collect(select(df, randn(1)))[1, 1], -1.03, tolerance = 0.01)
})

test_that("string operators", {
  df <- read.json(jsonPath)
  expect_equal(count(where(df, like(df$name, "A%"))), 1)
  expect_equal(count(where(df, startsWith(df$name, "A"))), 1)
  expect_true(first(select(df, startsWith(df$name, "M")))[[1]])
  expect_false(first(select(df, startsWith(df$name, "m")))[[1]])
  expect_true(first(select(df, endsWith(df$name, "el")))[[1]])
  expect_equal(first(select(df, substr(df$name, 1, 2)))[[1]], "Mi")
  if (as.numeric(R.version$major) >= 3 && as.numeric(R.version$minor) >= 3) {
    expect_true(startsWith("Hello World", "Hello"))
    expect_false(endsWith("Hello World", "a"))
  }
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
  df2 <- createDataFrame(l2)
  expect_equal(collect(select(df2, locate("aa", df2$a)))[1, 1], 1)
  expect_equal(collect(select(df2, locate("aa", df2$a, 2)))[1, 1], 2)
  expect_equal(collect(select(df2, lpad(df2$a, 8, "#")))[1, 1], "###aaads") # nolint
  expect_equal(collect(select(df2, rpad(df2$a, 8, "#")))[1, 1], "aaads###") # nolint

  l3 <- list(list(a = "a.b.c.d"))
  df3 <- createDataFrame(l3)
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
  df <- createDataFrame(l)
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
  df2 <- createDataFrame(l2)
  expect_equal(collect(select(df2, minute(df2$b)))[, 1], c(34, 24))
  expect_equal(collect(select(df2, second(df2$b)))[, 1], c(0, 34))
  expect_equal(collect(select(df2, from_utc_timestamp(df2$b, "JST")))[, 1],
               c(as.POSIXlt("2012-12-13 21:34:00 UTC"), as.POSIXlt("2014-12-15 10:24:34 UTC")))
  expect_equal(collect(select(df2, to_utc_timestamp(df2$b, "JST")))[, 1],
               c(as.POSIXlt("2012-12-13 03:34:00 UTC"), as.POSIXlt("2014-12-14 16:24:34 UTC")))
  expect_gt(collect(select(df2, unix_timestamp()))[1, 1], 0)
  expect_gt(collect(select(df2, unix_timestamp(df2$b)))[1, 1], 0)
  expect_gt(collect(select(df2, unix_timestamp(lit("2015-01-01"), "yyyy-MM-dd")))[1, 1], 0)

  l3 <- list(list(a = 1000), list(a = -1000))
  df3 <- createDataFrame(l3)
  result31 <- collect(select(df3, from_unixtime(df3$a)))
  expect_equal(grep("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", result31[, 1], perl = TRUE),
               c(1, 2))
  result32 <- collect(select(df3, from_unixtime(df3$a, "yyyy")))
  expect_equal(grep("\\d{4}", result32[, 1]), c(1, 2))
  Sys.setenv(TZ = .originalTimeZone)
})

test_that("greatest() and least() on a DataFrame", {
  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(l)
  expect_equal(collect(select(df, greatest(df$a, df$b)))[, 1], c(2, 4))
  expect_equal(collect(select(df, least(df$a, df$b)))[, 1], c(1, 3))
})

test_that("time windowing (window()) with all inputs", {
  df <- createDataFrame(data.frame(t = c("2016-03-11 09:00:07"), v = c(1)))
  df$window <- window(df$t, "5 seconds", "5 seconds", "0 seconds")
  local <- collect(df)$v
  # Not checking time windows because of possible time zone issues. Just checking that the function
  # works
  expect_equal(local, c(1))
})

test_that("time windowing (window()) with slide duration", {
  df <- createDataFrame(data.frame(t = c("2016-03-11 09:00:07"), v = c(1)))
  df$window <- window(df$t, "5 seconds", "2 seconds")
  local <- collect(df)$v
  # Not checking time windows because of possible time zone issues. Just checking that the function
  # works
  expect_equal(local, c(1, 1))
})

test_that("time windowing (window()) with start time", {
  df <- createDataFrame(data.frame(t = c("2016-03-11 09:00:07"), v = c(1)))
  df$window <- window(df$t, "5 seconds", startTime = "2 seconds")
  local <- collect(df)$v
  # Not checking time windows because of possible time zone issues. Just checking that the function
  # works
  expect_equal(local, c(1))
})

test_that("time windowing (window()) with just window duration", {
  df <- createDataFrame(data.frame(t = c("2016-03-11 09:00:07"), v = c(1)))
  df$window <- window(df$t, "5 seconds")
  local <- collect(df)$v
  # Not checking time windows because of possible time zone issues. Just checking that the function
  # works
  expect_equal(local, c(1))
})

test_that("when(), otherwise() and ifelse() on a DataFrame", {
  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(l)
  expect_equal(collect(select(df, when(df$a > 1 & df$b > 2, 1)))[, 1], c(NA, 1))
  expect_equal(collect(select(df, otherwise(when(df$a > 1, 1), 0)))[, 1], c(0, 1))
  expect_equal(collect(select(df, ifelse(df$a > 1 & df$b > 2, 0, 1)))[, 1], c(1, 0))
})

test_that("when(), otherwise() and ifelse() with column on a DataFrame", {
  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(l)
  expect_equal(collect(select(df, when(df$a > 1 & df$b > 2, lit(1))))[, 1], c(NA, 1))
  expect_equal(collect(select(df, otherwise(when(df$a > 1, lit(1)), lit(0))))[, 1], c(0, 1))
  expect_equal(collect(select(df, ifelse(df$a > 1 & df$b > 2, lit(0), lit(1))))[, 1], c(1, 0))
})

test_that("group by, agg functions", {
  df <- read.json(jsonPath)
  df1 <- agg(df, name = "max", age = "sum")
  expect_equal(1, count(df1))
  df1 <- agg(df, age2 = max(df$age))
  expect_equal(1, count(df1))
  expect_equal(columns(df1), c("age2"))

  gd <- groupBy(df, "name")
  expect_is(gd, "GroupedData")
  df2 <- count(gd)
  expect_is(df2, "SparkDataFrame")
  expect_equal(3, count(df2))

  # Also test group_by, summarize, mean
  gd1 <- group_by(df, "name")
  expect_is(gd1, "GroupedData")
  df_summarized <- summarize(gd, mean_age = mean(df$age))
  expect_is(df_summarized, "SparkDataFrame")
  expect_equal(3, count(df_summarized))

  df3 <- agg(gd, age = "stddev")
  expect_is(df3, "SparkDataFrame")
  df3_local <- collect(df3)
  expect_true(is.nan(df3_local[df3_local$name == "Andy", ][1, 2]))

  df4 <- agg(gd, sumAge = sum(df$age))
  expect_is(df4, "SparkDataFrame")
  expect_equal(3, count(df4))
  expect_equal(columns(df4), c("name", "sumAge"))

  df5 <- sum(gd, "age")
  expect_is(df5, "SparkDataFrame")
  expect_equal(3, count(df5))

  expect_equal(3, count(mean(gd)))
  expect_equal(3, count(max(gd)))
  expect_equal(30, collect(max(gd))[2, 2])
  expect_equal(1, collect(count(gd))[1, 2])

  mockLines2 <- c("{\"name\":\"ID1\", \"value\": \"10\"}",
                  "{\"name\":\"ID1\", \"value\": \"10\"}",
                  "{\"name\":\"ID1\", \"value\": \"22\"}",
                  "{\"name\":\"ID2\", \"value\": \"-3\"}")
  jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(mockLines2, jsonPath2)
  gd2 <- groupBy(read.json(jsonPath2), "name")
  df6 <- agg(gd2, value = "sum")
  df6_local <- collect(df6)
  expect_equal(42, df6_local[df6_local$name == "ID1", ][1, 2])
  expect_equal(-3, df6_local[df6_local$name == "ID2", ][1, 2])

  df7 <- agg(gd2, value = "stddev")
  df7_local <- collect(df7)
  expect_true(abs(df7_local[df7_local$name == "ID1", ][1, 2] - 6.928203) < 1e-6)
  expect_true(is.nan(df7_local[df7_local$name == "ID2", ][1, 2]))

  mockLines3 <- c("{\"name\":\"Andy\", \"age\":30}",
                  "{\"name\":\"Andy\", \"age\":30}",
                  "{\"name\":\"Justin\", \"age\":19}",
                  "{\"name\":\"Justin\", \"age\":1}")
  jsonPath3 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(mockLines3, jsonPath3)
  df8 <- read.json(jsonPath3)
  gd3 <- groupBy(df8, "name")
  gd3_local <- collect(sum(gd3))
  expect_equal(60, gd3_local[gd3_local$name == "Andy", ][1, 2])
  expect_equal(20, gd3_local[gd3_local$name == "Justin", ][1, 2])

  expect_true(abs(collect(agg(df, sd(df$age)))[1, 1] - 7.778175) < 1e-6)
  gd3_local <- collect(agg(gd3, var(df8$age)))
  expect_equal(162, gd3_local[gd3_local$name == "Justin", ][1, 2])

  # Test stats::sd, stats::var are working
  expect_true(abs(sd(1:2) - 0.7071068) < 1e-6)
  expect_true(abs(var(1:5, 1:5) - 2.5) < 1e-6)

  unlink(jsonPath2)
  unlink(jsonPath3)
})

test_that("pivot GroupedData column", {
  df <- createDataFrame(data.frame(
    earnings = c(10000, 10000, 11000, 15000, 12000, 20000, 21000, 22000),
    course = c("R", "Python", "R", "Python", "R", "Python", "R", "Python"),
    year = c(2013, 2013, 2014, 2014, 2015, 2015, 2016, 2016)
  ))
  sum1 <- collect(sum(pivot(groupBy(df, "year"), "course"), "earnings"))
  sum2 <- collect(sum(pivot(groupBy(df, "year"), "course", c("Python", "R")), "earnings"))
  sum3 <- collect(sum(pivot(groupBy(df, "year"), "course", list("Python", "R")), "earnings"))
  sum4 <- collect(sum(pivot(groupBy(df, "year"), "course", "R"), "earnings"))

  correct_answer <- data.frame(
    year = c(2013, 2014, 2015, 2016),
    Python = c(10000, 15000, 20000, 22000),
    R = c(10000, 11000, 12000, 21000)
  )
  expect_equal(sum1, correct_answer)
  expect_equal(sum2, correct_answer)
  expect_equal(sum3, correct_answer)
  expect_equal(sum4, correct_answer[, c("year", "R")])

  expect_error(collect(sum(pivot(groupBy(df, "year"), "course", c("R", "R")), "earnings")))
  expect_error(collect(sum(pivot(groupBy(df, "year"), "course", list("R", "R")), "earnings")))
})

test_that("arrange() and orderBy() on a DataFrame", {
  df <- read.json(jsonPath)
  sorted <- arrange(df, df$age)
  expect_equal(collect(sorted)[1, 2], "Michael")

  sorted2 <- arrange(df, "name", decreasing = FALSE)
  expect_equal(collect(sorted2)[2, "age"], 19)

  sorted3 <- orderBy(df, asc(df$age))
  expect_true(is.na(first(sorted3)$age))
  expect_equal(collect(sorted3)[2, "age"], 19)

  sorted4 <- orderBy(df, desc(df$name))
  expect_equal(first(sorted4)$name, "Michael")
  expect_equal(collect(sorted4)[3, "name"], "Andy")

  sorted5 <- arrange(df, "age", "name", decreasing = TRUE)
  expect_equal(collect(sorted5)[1, 2], "Andy")

  sorted6 <- arrange(df, "age", "name", decreasing = c(T, F))
  expect_equal(collect(sorted6)[1, 2], "Andy")

  sorted7 <- arrange(df, "name", decreasing = FALSE)
  expect_equal(collect(sorted7)[2, "age"], 19)
})

test_that("filter() on a DataFrame", {
  df <- read.json(jsonPath)
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

  # Test stats::filter is working
  #expect_true(is.ts(filter(1:100, rep(1, 3)))) # nolint
})

test_that("join() and merge() on a DataFrame", {
  df <- read.json(jsonPath)

  mockLines2 <- c("{\"name\":\"Michael\", \"test\": \"yes\"}",
                  "{\"name\":\"Andy\",  \"test\": \"no\"}",
                  "{\"name\":\"Justin\", \"test\": \"yes\"}",
                  "{\"name\":\"Bob\", \"test\": \"yes\"}")
  jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(mockLines2, jsonPath2)
  df2 <- read.json(jsonPath2)

  joined <- join(df, df2)
  expect_equal(names(joined), c("age", "name", "name", "test"))
  expect_equal(count(joined), 12)
  expect_equal(names(collect(joined)), c("age", "name", "name", "test"))

  joined2 <- join(df, df2, df$name == df2$name)
  expect_equal(names(joined2), c("age", "name", "name", "test"))
  expect_equal(count(joined2), 3)

  joined3 <- join(df, df2, df$name == df2$name, "rightouter")
  expect_equal(names(joined3), c("age", "name", "name", "test"))
  expect_equal(count(joined3), 4)
  expect_true(is.na(collect(orderBy(joined3, joined3$age))$age[2]))

  joined4 <- select(join(df, df2, df$name == df2$name, "outer"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined4), c("newAge", "name", "test"))
  expect_equal(count(joined4), 4)
  expect_equal(collect(orderBy(joined4, joined4$name))$newAge[3], 24)

  joined5 <- join(df, df2, df$name == df2$name, "leftouter")
  expect_equal(names(joined5), c("age", "name", "name", "test"))
  expect_equal(count(joined5), 3)
  expect_true(is.na(collect(orderBy(joined5, joined5$age))$age[1]))

  joined6 <- join(df, df2, df$name == df2$name, "inner")
  expect_equal(names(joined6), c("age", "name", "name", "test"))
  expect_equal(count(joined6), 3)

  joined7 <- join(df, df2, df$name == df2$name, "leftsemi")
  expect_equal(names(joined7), c("age", "name"))
  expect_equal(count(joined7), 3)

  joined8 <- join(df, df2, df$name == df2$name, "left_outer")
  expect_equal(names(joined8), c("age", "name", "name", "test"))
  expect_equal(count(joined8), 3)
  expect_true(is.na(collect(orderBy(joined8, joined8$age))$age[1]))

  joined9 <- join(df, df2, df$name == df2$name, "right_outer")
  expect_equal(names(joined9), c("age", "name", "name", "test"))
  expect_equal(count(joined9), 4)
  expect_true(is.na(collect(orderBy(joined9, joined9$age))$age[2]))

  merged <- merge(df, df2, by.x = "name", by.y = "name", all.x = TRUE, all.y = TRUE)
  expect_equal(count(merged), 4)
  expect_equal(names(merged), c("age", "name_x", "name_y", "test"))
  expect_equal(collect(orderBy(merged, merged$name_x))$age[3], 19)

  merged <- merge(df, df2, suffixes = c("-X", "-Y"))
  expect_equal(count(merged), 3)
  expect_equal(names(merged), c("age", "name-X", "name-Y", "test"))
  expect_equal(collect(orderBy(merged, merged$"name-X"))$age[1], 30)

  merged <- merge(df, df2, by = "name", suffixes = c("-X", "-Y"), sort = FALSE)
  expect_equal(count(merged), 3)
  expect_equal(names(merged), c("age", "name-X", "name-Y", "test"))
  expect_equal(collect(orderBy(merged, merged$"name-Y"))$"name-X"[3], "Michael")

  merged <- merge(df, df2, by = "name", all = T, sort = T)
  expect_equal(count(merged), 4)
  expect_equal(names(merged), c("age", "name_x", "name_y", "test"))
  expect_equal(collect(orderBy(merged, merged$"name_y"))$"name_x"[1], "Andy")

  merged <- merge(df, df2, by = NULL)
  expect_equal(count(merged), 12)
  expect_equal(names(merged), c("age", "name", "name", "test"))

  mockLines3 <- c("{\"name\":\"Michael\", \"name_y\":\"Michael\", \"test\": \"yes\"}",
                  "{\"name\":\"Andy\", \"name_y\":\"Andy\", \"test\": \"no\"}",
                  "{\"name\":\"Justin\", \"name_y\":\"Justin\", \"test\": \"yes\"}",
                  "{\"name\":\"Bob\", \"name_y\":\"Bob\", \"test\": \"yes\"}")
  jsonPath3 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(mockLines3, jsonPath3)
  df3 <- read.json(jsonPath3)
  expect_error(merge(df, df3),
               paste("The following column name: name_y occurs more than once in the 'DataFrame'.",
                     "Please use different suffixes for the intersected columns.", sep = ""))

  unlink(jsonPath2)
  unlink(jsonPath3)
})

test_that("toJSON() returns an RDD of the correct values", {
  df <- read.json(jsonPath)
  testRDD <- toJSON(df)
  expect_is(testRDD, "RDD")
  expect_equal(getSerializedMode(testRDD), "string")
  expect_equal(collectRDD(testRDD)[[1]], mockLines[1])
})

test_that("showDF()", {
  df <- read.json(jsonPath)
  expected <- paste("+----+-------+\n",
                    "| age|   name|\n",
                    "+----+-------+\n",
                    "|null|Michael|\n",
                    "|  30|   Andy|\n",
                    "|  19| Justin|\n",
                    "+----+-------+\n", sep = "")
  expected2 <- paste("+---+----+\n",
                     "|age|name|\n",
                     "+---+----+\n",
                     "|nul| Mic|\n",
                     "| 30| And|\n",
                     "| 19| Jus|\n",
                     "+---+----+\n", sep = "")
  expect_output(showDF(df), expected)
  expect_output(showDF(df, truncate = 3), expected2)
})

test_that("isLocal()", {
  df <- read.json(jsonPath)
  expect_false(isLocal(df))
})

test_that("union(), rbind(), except(), and intersect() on a DataFrame", {
  df <- read.json(jsonPath)

  lines <- c("{\"name\":\"Bob\", \"age\":24}",
             "{\"name\":\"Andy\", \"age\":30}",
             "{\"name\":\"James\", \"age\":35}")
  jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(lines, jsonPath2)
  df2 <- read.df(jsonPath2, "json")

  unioned <- arrange(union(df, df2), df$age)
  expect_is(unioned, "SparkDataFrame")
  expect_equal(count(unioned), 6)
  expect_equal(first(unioned)$name, "Michael")
  expect_equal(count(arrange(suppressWarnings(unionAll(df, df2)), df$age)), 6)

  unioned2 <- arrange(rbind(unioned, df, df2), df$age)
  expect_is(unioned2, "SparkDataFrame")
  expect_equal(count(unioned2), 12)
  expect_equal(first(unioned2)$name, "Michael")

  excepted <- arrange(except(df, df2), desc(df$age))
  expect_is(unioned, "SparkDataFrame")
  expect_equal(count(excepted), 2)
  expect_equal(first(excepted)$name, "Justin")

  intersected <- arrange(intersect(df, df2), df$age)
  expect_is(unioned, "SparkDataFrame")
  expect_equal(count(intersected), 1)
  expect_equal(first(intersected)$name, "Andy")

  # Test base::union is working
  expect_equal(union(c(1:3), c(3:5)), c(1:5))

  # Test base::rbind is working
  expect_equal(length(rbind(1:4, c = 2, a = 10, 10, deparse.level = 0)), 16)

  # Test base::intersect is working
  expect_equal(length(intersect(1:20, 3:23)), 18)

  unlink(jsonPath2)
})

test_that("withColumn() and withColumnRenamed()", {
  df <- read.json(jsonPath)
  newDF <- withColumn(df, "newAge", df$age + 2)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 32)

  # Replace existing column
  newDF <- withColumn(df, "age", df$age + 2)
  expect_equal(length(columns(newDF)), 2)
  expect_equal(first(filter(newDF, df$name != "Michael"))$age, 32)

  newDF2 <- withColumnRenamed(df, "age", "newerAge")
  expect_equal(length(columns(newDF2)), 2)
  expect_equal(columns(newDF2)[1], "newerAge")
})

test_that("mutate(), transform(), rename() and names()", {
  df <- read.json(jsonPath)
  newDF <- mutate(df, newAge = df$age + 2)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 32)

  newDF <- mutate(df, age = df$age + 2, newAge = df$age + 3)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 33)
  expect_equal(first(filter(newDF, df$name != "Michael"))$age, 32)

  newDF <- mutate(df, age = df$age + 2, newAge = df$age + 3,
                  age = df$age + 4, newAge = df$age + 5)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[3], "newAge")
  expect_equal(first(filter(newDF, df$name != "Michael"))$newAge, 35)
  expect_equal(first(filter(newDF, df$name != "Michael"))$age, 34)

  newDF <- mutate(df, df$age + 3)
  expect_equal(length(columns(newDF)), 3)
  expect_equal(columns(newDF)[[3]], "df$age + 3")
  expect_equal(first(filter(newDF, df$name != "Michael"))[[3]], 33)

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

  # test if base::transform on local data frames works
  # ensure the proper signature is used - otherwise this will fail to run
  attach(airquality)
  result <- transform(Ozone, logOzone = log(Ozone))
  expect_equal(nrow(result), 153)
  expect_equal(ncol(result), 2)
  detach(airquality)
})

test_that("read/write ORC files", {
  setHiveContext(sc)
  df <- read.df(jsonPath, "json")

  # Test write.df and read.df
  write.df(df, orcPath, "orc", mode = "overwrite")
  df2 <- read.df(orcPath, "orc")
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df), count(df2))

  # Test write.orc and read.orc
  orcPath2 <- tempfile(pattern = "orcPath2", fileext = ".orc")
  write.orc(df, orcPath2)
  orcDF <- read.orc(orcPath2)
  expect_is(orcDF, "SparkDataFrame")
  expect_equal(count(orcDF), count(df))

  unlink(orcPath2)
  unsetHiveContext()
})

test_that("read/write ORC files - compression option", {
  setHiveContext(sc)
  df <- read.df(jsonPath, "json")

  orcPath2 <- tempfile(pattern = "orcPath2", fileext = ".orc")
  write.orc(df, orcPath2, compression = "ZLIB")
  orcDF <- read.orc(orcPath2)
  expect_is(orcDF, "SparkDataFrame")
  expect_equal(count(orcDF), count(df))
  expect_true(length(list.files(orcPath2, pattern = ".zlib.orc")) > 0)

  unlink(orcPath2)
  unsetHiveContext()
})

test_that("read/write Parquet files", {
  df <- read.df(jsonPath, "json")
  # Test write.df and read.df
  write.df(df, parquetPath, "parquet", mode = "overwrite")
  df2 <- read.df(parquetPath, "parquet")
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df2), 3)

  # Test write.parquet/saveAsParquetFile and read.parquet/parquetFile
  parquetPath2 <- tempfile(pattern = "parquetPath2", fileext = ".parquet")
  write.parquet(df, parquetPath2)
  parquetPath3 <- tempfile(pattern = "parquetPath3", fileext = ".parquet")
  suppressWarnings(saveAsParquetFile(df, parquetPath3))
  parquetDF <- read.parquet(c(parquetPath2, parquetPath3))
  expect_is(parquetDF, "SparkDataFrame")
  expect_equal(count(parquetDF), count(df) * 2)
  parquetDF2 <- suppressWarnings(parquetFile(parquetPath2, parquetPath3))
  expect_is(parquetDF2, "SparkDataFrame")
  expect_equal(count(parquetDF2), count(df) * 2)

  # Test if varargs works with variables
  saveMode <- "overwrite"
  mergeSchema <- "true"
  parquetPath4 <- tempfile(pattern = "parquetPath3", fileext = ".parquet")
  write.df(df, parquetPath3, "parquet", mode = saveMode, mergeSchema = mergeSchema)

  unlink(parquetPath2)
  unlink(parquetPath3)
  unlink(parquetPath4)
})

test_that("read/write Parquet files - compression option/mode", {
  df <- read.df(jsonPath, "json")
  tempPath <- tempfile(pattern = "tempPath", fileext = ".parquet")

  # Test write.df and read.df
  write.parquet(df, tempPath, compression = "GZIP")
  df2 <- read.parquet(tempPath)
  expect_is(df2, "SparkDataFrame")
  expect_equal(count(df2), 3)
  expect_true(length(list.files(tempPath, pattern = ".gz.parquet")) > 0)

  write.parquet(df, tempPath, mode = "overwrite")
  df3 <- read.parquet(tempPath)
  expect_is(df3, "SparkDataFrame")
  expect_equal(count(df3), 3)
})

test_that("read/write text files", {
  # Test write.df and read.df
  df <- read.df(jsonPath, "text")
  expect_is(df, "SparkDataFrame")
  expect_equal(colnames(df), c("value"))
  expect_equal(count(df), 3)
  textPath <- tempfile(pattern = "textPath", fileext = ".txt")
  write.df(df, textPath, "text", mode = "overwrite")

  # Test write.text and read.text
  textPath2 <- tempfile(pattern = "textPath2", fileext = ".txt")
  write.text(df, textPath2)
  df2 <- read.text(c(textPath, textPath2))
  expect_is(df2, "SparkDataFrame")
  expect_equal(colnames(df2), c("value"))
  expect_equal(count(df2), count(df) * 2)

  unlink(textPath)
  unlink(textPath2)
})

test_that("read/write text files - compression option", {
  df <- read.df(jsonPath, "text")

  textPath <- tempfile(pattern = "textPath", fileext = ".txt")
  write.text(df, textPath, compression = "GZIP")
  textDF <- read.text(textPath)
  expect_is(textDF, "SparkDataFrame")
  expect_equal(count(textDF), count(df))
  expect_true(length(list.files(textPath, pattern = ".gz")) > 0)

  unlink(textPath)
})

test_that("describe() and summarize() on a DataFrame", {
  df <- read.json(jsonPath)
  stats <- describe(df, "age")
  expect_equal(collect(stats)[1, "summary"], "count")
  expect_equal(collect(stats)[2, "age"], "24.5")
  expect_equal(collect(stats)[3, "age"], "7.7781745930520225")
  stats <- describe(df)
  expect_equal(collect(stats)[4, "summary"], "min")
  expect_equal(collect(stats)[5, "age"], "30")

  stats2 <- summary(df)
  expect_equal(collect(stats2)[4, "summary"], "min")
  expect_equal(collect(stats2)[5, "age"], "30")

  # SPARK-16425: SparkR summary() fails on column of type logical
  df <- withColumn(df, "boolean", df$age == 30)
  summary(df)

  # Test base::summary is working
  expect_equal(length(summary(attenu, digits = 4)), 35)
})

test_that("dropna() and na.omit() on a DataFrame", {
  df <- read.json(jsonPathNa)
  rows <- collect(df)

  # drop with columns

  expected <- rows[!is.na(rows$name), ]
  actual <- collect(dropna(df, cols = "name"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, cols = "name"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age), ]
  actual <- collect(dropna(df, cols = "age"))
  row.names(expected) <- row.names(actual)
  # identical on two dataframes does not work here. Don't know why.
  # use identical on all columns as a workaround.
  expect_identical(expected$age, actual$age)
  expect_identical(expected$height, actual$height)
  expect_identical(expected$name, actual$name)
  actual <- collect(na.omit(df, cols = "age"))

  expected <- rows[!is.na(rows$age) & !is.na(rows$height), ]
  actual <- collect(dropna(df, cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name), ]
  actual <- collect(dropna(df))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df))
  expect_identical(expected, actual)

  # drop with how

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name), ]
  actual <- collect(dropna(df))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) | !is.na(rows$height) | !is.na(rows$name), ]
  actual <- collect(dropna(df, "all"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "all"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height) & !is.na(rows$name), ]
  actual <- collect(dropna(df, "any"))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "any"))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) & !is.na(rows$height), ]
  actual <- collect(dropna(df, "any", cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "any", cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[!is.na(rows$age) | !is.na(rows$height), ]
  actual <- collect(dropna(df, "all", cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, "all", cols = c("age", "height")))
  expect_identical(expected, actual)

  # drop with threshold

  expected <- rows[as.integer(!is.na(rows$age)) + as.integer(!is.na(rows$height)) >= 2, ]
  actual <- collect(dropna(df, minNonNulls = 2, cols = c("age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, minNonNulls = 2, cols = c("age", "height")))
  expect_identical(expected, actual)

  expected <- rows[as.integer(!is.na(rows$age)) +
                   as.integer(!is.na(rows$height)) +
                   as.integer(!is.na(rows$name)) >= 3, ]
  actual <- collect(dropna(df, minNonNulls = 3, cols = c("name", "age", "height")))
  expect_identical(expected, actual)
  actual <- collect(na.omit(df, minNonNulls = 3, cols = c("name", "age", "height")))
  expect_identical(expected, actual)

  # Test stats::na.omit is working
  expect_equal(nrow(na.omit(data.frame(x = c(0, 10, NA)))), 2)
})

test_that("fillna() on a DataFrame", {
  df <- read.json(jsonPathNa)
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
  ordered <- ct[order(ct$a_b), ]
  row.names(ordered) <- NULL
  expected <- data.frame("a_b" = c("a0", "a1", "a2"), "b0" = c(1, 0, 1), "b1" = c(1, 1, 0),
                         stringsAsFactors = FALSE, row.names = NULL)
  expect_identical(expected, ordered)
})

test_that("cov() and corr() on a DataFrame", {
  l <- lapply(c(0:9), function(x) { list(x, x * 2.0) })
  df <- createDataFrame(l, c("singles", "doubles"))
  result <- cov(df, "singles", "doubles")
  expect_true(abs(result - 55.0 / 3) < 1e-12)

  result <- corr(df, "singles", "doubles")
  expect_true(abs(result - 1.0) < 1e-12)
  result <- corr(df, "singles", "doubles", "pearson")
  expect_true(abs(result - 1.0) < 1e-12)

  # Test stats::cov is working
  #expect_true(abs(max(cov(swiss)) - 1739.295) < 1e-3) # nolint
})

test_that("freqItems() on a DataFrame", {
  input <- 1:1000
  rdf <- data.frame(numbers = input, letters = as.character(input),
                    negDoubles = input * -1.0, stringsAsFactors = F)
  rdf[ input %% 3 == 0, ] <- c(1, "1", -1)
  df <- createDataFrame(rdf)
  multiColResults <- freqItems(df, c("numbers", "letters"), support = 0.1)
  expect_true(1 %in% multiColResults$numbers[[1]])
  expect_true("1" %in% multiColResults$letters[[1]])
  singleColResult <- freqItems(df, "negDoubles", support = 0.1)
  expect_true(-1 %in% head(singleColResult$negDoubles)[[1]])

  l <- lapply(c(0:99), function(i) {
    if (i %% 2 == 0) { list(1L, -1.0) }
    else { list(i, i * -1.0) }})
  df <- createDataFrame(l, c("a", "b"))
  result <- freqItems(df, c("a", "b"), 0.4)
  expect_identical(result[[1]], list(list(1L, 99L)))
  expect_identical(result[[2]], list(list(-1, -99)))
})

test_that("sampleBy() on a DataFrame", {
  l <- lapply(c(0:99), function(i) { as.character(i %% 3) })
  df <- createDataFrame(l, "key")
  fractions <- list("0" = 0.1, "1" = 0.2)
  sample <- sampleBy(df, "key", fractions, 0)
  result <- collect(orderBy(count(groupBy(sample, "key")), "key"))
  expect_identical(as.list(result[1, ]), list(key = "0", count = 3))
  expect_identical(as.list(result[2, ]), list(key = "1", count = 7))
})

test_that("approxQuantile() on a DataFrame", {
  l <- lapply(c(0:99), function(i) { i })
  df <- createDataFrame(l, "key")
  quantiles <- approxQuantile(df, "key", c(0.5, 0.8), 0.0)
  expect_equal(quantiles[[1]], 50)
  expect_equal(quantiles[[2]], 80)
})

test_that("SQL error message is returned from JVM", {
  retError <- tryCatch(sql("select * from blah"), error = function(e) e)
  expect_equal(grepl("Table or view not found", retError), TRUE)
  expect_equal(grepl("blah", retError), TRUE)
})

irisDF <- suppressWarnings(createDataFrame(iris))

test_that("Method as.data.frame as a synonym for collect()", {
  expect_equal(as.data.frame(irisDF), collect(irisDF))
  irisDF2 <- irisDF[irisDF$Species == "setosa", ]
  expect_equal(as.data.frame(irisDF2), collect(irisDF2))

  # Make sure as.data.frame in the R base package is not covered
  expect_error(as.data.frame(c(1, 2)), NA)
})

test_that("attach() on a DataFrame", {
  df <- read.json(jsonPath)
  expect_error(age)
  attach(df)
  expect_is(age, "SparkDataFrame")
  expected_age <- data.frame(age = c(NA, 30, 19))
  expect_equal(head(age), expected_age)
  stat <- summary(age)
  expect_equal(collect(stat)[5, "age"], "30")
  age <- age$age + 1
  expect_is(age, "Column")
  rm(age)
  stat2 <- summary(age)
  expect_equal(collect(stat2)[5, "age"], "30")
  detach("df")
  stat3 <- summary(df[, "age", drop = F])
  expect_equal(collect(stat3)[5, "age"], "30")
  expect_error(age)
})

test_that("with() on a DataFrame", {
  df <- suppressWarnings(createDataFrame(iris))
  expect_error(Sepal_Length)
  sum1 <- with(df, list(summary(Sepal_Length), summary(Sepal_Width)))
  expect_equal(collect(sum1[[1]])[1, "Sepal_Length"], "150")
  sum2 <- with(df, distinct(Sepal_Length))
  expect_equal(nrow(sum2), 35)
})

test_that("Method coltypes() to get and set R's data types of a DataFrame", {
  expect_equal(coltypes(irisDF), c(rep("numeric", 4), "character"))

  data <- data.frame(c1 = c(1, 2, 3),
                     c2 = c(T, F, T),
                     c3 = c("2015/01/01 10:00:00", "2015/01/02 10:00:00", "2015/01/03 10:00:00"))

  schema <- structType(structField("c1", "byte"),
                       structField("c3", "boolean"),
                       structField("c4", "timestamp"))

  # Test primitive types
  DF <- createDataFrame(data, schema)
  expect_equal(coltypes(DF), c("integer", "logical", "POSIXct"))
  createOrReplaceTempView(DF, "DFView")
  sqlCast <- sql("select cast('2' as decimal) as x from DFView limit 1")
  expect_equal(coltypes(sqlCast), "numeric")

  # Test complex types
  x <- createDataFrame(list(list(as.environment(
    list("a" = "b", "c" = "d", "e" = "f")))))
  expect_equal(coltypes(x), "map<string,string>")

  df <- selectExpr(read.json(jsonPath), "name", "(age * 1.21) as age")
  expect_equal(dtypes(df), list(c("name", "string"), c("age", "decimal(24,2)")))

  df1 <- select(df, cast(df$age, "integer"))
  coltypes(df) <- c("character", "integer")
  expect_equal(dtypes(df), list(c("name", "string"), c("age", "int")))
  value <- collect(df[, 2, drop = F])[[3, 1]]
  expect_equal(value, collect(df1)[[3, 1]])
  expect_equal(value, 22)

  coltypes(df) <- c(NA, "numeric")
  expect_equal(dtypes(df), list(c("name", "string"), c("age", "double")))

  expect_error(coltypes(df) <- c("character"),
               "Length of type vector should match the number of columns for SparkDataFrame")
  expect_error(coltypes(df) <- c("environment", "list"),
               "Only atomic type is supported for column types")
})

test_that("Method str()", {
  # Structure of Iris
  iris2 <- iris
  colnames(iris2) <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species")
  iris2$col <- TRUE
  irisDF2 <- createDataFrame(iris2)

  out <- capture.output(str(irisDF2))
  expect_equal(length(out), 7)
  expect_equal(out[1], "'SparkDataFrame': 6 variables:")
  expect_equal(out[2], " $ Sepal_Length: num 5.1 4.9 4.7 4.6 5 5.4")
  expect_equal(out[3], " $ Sepal_Width : num 3.5 3 3.2 3.1 3.6 3.9")
  expect_equal(out[4], " $ Petal_Length: num 1.4 1.4 1.3 1.5 1.4 1.7")
  expect_equal(out[5], " $ Petal_Width : num 0.2 0.2 0.2 0.2 0.2 0.4")
  expect_equal(out[6], paste0(" $ Species     : chr \"setosa\" \"setosa\" \"",
                              "setosa\" \"setosa\" \"setosa\" \"setosa\""))
  expect_equal(out[7], " $ col         : logi TRUE TRUE TRUE TRUE TRUE TRUE")

  createOrReplaceTempView(irisDF2, "irisView")

  sqlCast <- sql("select cast('2' as decimal) as x from irisView limit 1")
  castStr <- capture.output(str(sqlCast))
  expect_equal(length(castStr), 2)
  expect_equal(castStr[1], "'SparkDataFrame': 1 variables:")
  expect_equal(castStr[2], " $ x: num 2")

  # A random dataset with many columns. This test is to check str limits
  # the number of columns. Therefore, it will suffice to check for the
  # number of returned rows
  x <- runif(200, 1, 10)
  df <- data.frame(t(as.matrix(data.frame(x, x, x, x, x, x, x, x, x))))
  DF <- createDataFrame(df)
  out <- capture.output(str(DF))
  expect_equal(length(out), 103)

  # Test utils:::str
  expect_equal(capture.output(utils:::str(iris)), capture.output(str(iris)))
})

test_that("show/head on Columns", {

  # collect
  x <- irisDF$Sepal_Length + 100
  y <- cos(x + irisDF$Sepal_Width) ^ 2
  z <- sin(x + irisDF$Sepal_Width) ^ 2
  expect_equal(any(head(y + z) == 1), TRUE)

  # show and print
  expect_equal(capture.output(show(z + y))[1], " [1] 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1")
  expect_equal(capture.output(print(z + y))[1], " [1] 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1")

  # head
  expect_equal(all(round(head(z + y)) == 1), TRUE)
  expect_equal(length(head(z + y, 100)), 100)

  # Columns without parent DataFrame
  expect_equal(is.numeric(head(rand())), TRUE)
})

test_that("Minimal column test.", {
  x <- column(irisDF$Sepal_Length@jc)
  expect_equal(head(select(irisDF, x))[1, 1], 5.1)
})

test_that("Histogram", {

  # Basic histogram test with colname
  expect_equal(
    all(histogram(irisDF, "Petal_Width", 8) ==
        data.frame(bins = seq(0, 7),
                   counts = c(48, 2, 7, 21, 24, 19, 15, 14),
                   centroids = seq(0, 7) * 0.3 + 0.25)),
        TRUE)

  # Basic histogram test with Column
  expect_equal(
    all(histogram(irisDF, irisDF$Petal_Width, 8) ==
          data.frame(bins = seq(0, 7),
                     counts = c(48, 2, 7, 21, 24, 19, 15, 14),
                     centroids = seq(0, 7) * 0.3 + 0.25)),
    TRUE)

  # Basic histogram test with derived column
  expect_equal(
    all(round(histogram(irisDF, irisDF$Petal_Width + 1, 8), 2) ==
          data.frame(bins = seq(0, 7),
                     counts = c(48, 2, 7, 21, 24, 19, 15, 14),
                     centroids = seq(0, 7) * 0.3 + 1.25)),
    TRUE)

  # Missing nbins
  expect_equal(length(histogram(irisDF, "Petal_Width")$counts), 10)

  # Wrong colname
  expect_error(histogram(irisDF, "xxx"),
               "Specified colname does not belong to the given SparkDataFrame.")

  # Invalid nbins
  expect_error(histogram(irisDF, "Petal_Width", nbins = 0),
               "The number of bins must be a positive integer number greater than 1.")

  # Test against R's hist
  expect_equal(all(hist(iris$Sepal.Width)$counts ==
                   histogram(irisDF, "Sepal_Width", 12)$counts), T)

  # Test when there are zero counts
  df <- as.DataFrame(data.frame(x = c(1, 2, 3, 4, 100)))
  expect_equal(histogram(df, "x")$counts, c(4, 0, 0, 0, 0, 0, 0, 0, 0, 1))
})

test_that("dapply() and dapplyCollect() on a DataFrame", {
  df <- createDataFrame(
          list(list(1L, 1, "1"), list(2L, 2, "2"), list(3L, 3, "3")),
          c("a", "b", "c"))
  ldf <- collect(df)
  df1 <- dapply(df, function(x) { x }, schema(df))
  result <- collect(df1)
  expect_identical(ldf, result)

  result <- dapplyCollect(df, function(x) { x })
  expect_identical(ldf, result)

  # Filter and add a column
  schema <- structType(structField("a", "integer"), structField("b", "double"),
                       structField("c", "string"), structField("d", "integer"))
  df1 <- dapply(
           df,
           function(x) {
             y <- x[x$a > 1, ]
             y <- cbind(y, y$a + 1L)
           },
           schema)
  result <- collect(df1)
  expected <- ldf[ldf$a > 1, ]
  expected$d <- expected$a + 1L
  rownames(expected) <- NULL
  expect_identical(expected, result)

  result <- dapplyCollect(
              df,
              function(x) {
                y <- x[x$a > 1, ]
                y <- cbind(y, y$a + 1L)
              })
  expected1 <- expected
  names(expected1) <- names(result)
  expect_identical(expected1, result)

  # Remove the added column
  df2 <- dapply(
           df1,
           function(x) {
             x[, c("a", "b", "c")]
           },
           schema(df))
  result <- collect(df2)
  expected <- expected[, c("a", "b", "c")]
  expect_identical(expected, result)

  result <- dapplyCollect(
              df1,
              function(x) {
               x[, c("a", "b", "c")]
              })
  expect_identical(expected, result)
})

test_that("dapplyCollect() on DataFrame with a binary column", {

  df <- data.frame(key = 1:3)
  df$bytes <- lapply(df$key, serialize, connection = NULL)

  df_spark <- createDataFrame(df)

  result1 <- collect(df_spark)
  expect_identical(df, result1)

  result2 <- dapplyCollect(df_spark, function(x) x)
  expect_identical(df, result2)

  # A data.frame with a single column of bytes
  scb <- subset(df, select = "bytes")
  scb_spark <- createDataFrame(scb)
  result <- dapplyCollect(scb_spark, function(x) x)
  expect_identical(scb, result)

})

test_that("repartition by columns on DataFrame", {
  df <- createDataFrame(
    list(list(1L, 1, "1", 0.1), list(1L, 2, "2", 0.2), list(3L, 3, "3", 0.3)),
    c("a", "b", "c", "d"))

  # no column and number of partitions specified
  retError <- tryCatch(repartition(df), error = function(e) e)
  expect_equal(grepl
    ("Please, specify the number of partitions and/or a column\\(s\\)", retError), TRUE)

  # repartition by column and number of partitions
  actual <- repartition(df, 3L, col = df$"a")

  # since we cannot access the number of partitions from dataframe, checking
  # that at least the dimensions are identical
  expect_identical(dim(df), dim(actual))

  # repartition by number of partitions
  actual <- repartition(df, 13L)
  expect_identical(dim(df), dim(actual))

  # a test case with a column and dapply
  schema <-  structType(structField("a", "integer"), structField("avg", "double"))
  df <- repartition(df, col = df$"a")
  df1 <- dapply(
    df,
    function(x) {
      y <- (data.frame(x$a[1], mean(x$b)))
    },
    schema)

  # Number of partitions is equal to 2
  expect_equal(nrow(df1), 2)
})

test_that("gapply() and gapplyCollect() on a DataFrame", {
  df <- createDataFrame (
    list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
    c("a", "b", "c", "d"))
  expected <- collect(df)
  df1 <- gapply(df, "a", function(key, x) { x }, schema(df))
  actual <- collect(df1)
  expect_identical(actual, expected)

  df1Collect <- gapplyCollect(df, list("a"), function(key, x) { x })
  expect_identical(df1Collect, expected)

  # Computes the sum of second column by grouping on the first and third columns
  # and checks if the sum is larger than 2
  schema <- structType(structField("a", "integer"), structField("e", "boolean"))
  df2 <- gapply(
    df,
    c(df$"a", df$"c"),
    function(key, x) {
      y <- data.frame(key[1], sum(x$b) > 2)
    },
    schema)
  actual <- collect(df2)$e
  expected <- c(TRUE, TRUE)
  expect_identical(actual, expected)

  df2Collect <- gapplyCollect(
    df,
    c(df$"a", df$"c"),
    function(key, x) {
      y <- data.frame(key[1], sum(x$b) > 2)
      colnames(y) <- c("a", "e")
      y
    })
    actual <- df2Collect$e
    expect_identical(actual, expected)

  # Computes the arithmetic mean of the second column by grouping
  # on the first and third columns. Output the groupping value and the average.
  schema <-  structType(structField("a", "integer"), structField("c", "string"),
               structField("avg", "double"))
  df3 <- gapply(
    df,
    c("a", "c"),
    function(key, x) {
      y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
    },
    schema)
  actual <- collect(df3)
  actual <-  actual[order(actual$a), ]
  rownames(actual) <- NULL
  expected <- collect(select(df, "a", "b", "c"))
  expected <- data.frame(aggregate(expected$b, by = list(expected$a, expected$c), FUN = mean))
  colnames(expected) <- c("a", "c", "avg")
  expected <-  expected[order(expected$a), ]
  rownames(expected) <- NULL
  expect_identical(actual, expected)

  df3Collect <- gapplyCollect(
    df,
    c("a", "c"),
    function(key, x) {
      y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
      colnames(y) <- c("a", "c", "avg")
      y
    })
  actual <- df3Collect[order(df3Collect$a), ]
  expect_identical(actual$avg, expected$avg)

  irisDF <- suppressWarnings(createDataFrame (iris))
  schema <-  structType(structField("Sepal_Length", "double"), structField("Avg", "double"))
  # Groups by `Sepal_Length` and computes the average for `Sepal_Width`
  df4 <- gapply(
    cols = "Sepal_Length",
    irisDF,
    function(key, x) {
      y <- data.frame(key, mean(x$Sepal_Width), stringsAsFactors = FALSE)
    },
    schema)
  actual <- collect(df4)
  actual <- actual[order(actual$Sepal_Length), ]
  rownames(actual) <- NULL
  agg_local_df <- data.frame(aggregate(iris$Sepal.Width, by = list(iris$Sepal.Length), FUN = mean),
                    stringsAsFactors = FALSE)
  colnames(agg_local_df) <- c("Sepal_Length", "Avg")
  expected <-  agg_local_df[order(agg_local_df$Sepal_Length), ]
  rownames(expected) <- NULL
  expect_identical(actual, expected)
})

test_that("Window functions on a DataFrame", {
  df <- createDataFrame(list(list(1L, "1"), list(2L, "2"), list(1L, "1"), list(2L, "2")),
                        schema = c("key", "value"))
  ws <- orderBy(windowPartitionBy("key"), "value")
  result <- collect(select(df, over(lead("key", 1), ws), over(lead("value", 1), ws)))
  names(result) <- c("key", "value")
  expected <- data.frame(key = c(1L, NA, 2L, NA),
                       value = c("1", NA, "2", NA),
                       stringsAsFactors = FALSE)
  expect_equal(result, expected)

  ws <- orderBy(windowPartitionBy(df$key), df$value)
  result <- collect(select(df, over(lead("key", 1), ws), over(lead("value", 1), ws)))
  names(result) <- c("key", "value")
  expect_equal(result, expected)

  ws <- partitionBy(windowOrderBy("value"), "key")
  result <- collect(select(df, over(lead("key", 1), ws), over(lead("value", 1), ws)))
  names(result) <- c("key", "value")
  expect_equal(result, expected)

  ws <- partitionBy(windowOrderBy(df$value), df$key)
  result <- collect(select(df, over(lead("key", 1), ws), over(lead("value", 1), ws)))
  names(result) <- c("key", "value")
  expect_equal(result, expected)
})

test_that("createDataFrame sqlContext parameter backward compatibility", {
  sqlContext <- suppressWarnings(sparkRSQL.init(sc))
  a <- 1:3
  b <- c("a", "b", "c")
  ldf <- data.frame(a, b)
  # Call function with namespace :: operator - SPARK-16538
  df <- suppressWarnings(SparkR::createDataFrame(sqlContext, ldf))
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
  expect_equal(count(df), 3)
  ldf2 <- collect(df)
  expect_equal(ldf$a, ldf2$a)

  df2 <- suppressWarnings(createDataFrame(sqlContext, iris))
  expect_equal(count(df2), 150)
  expect_equal(ncol(df2), 5)

  df3 <- suppressWarnings(read.df(sqlContext, jsonPath, "json"))
  expect_is(df3, "SparkDataFrame")
  expect_equal(count(df3), 3)

  before <- suppressWarnings(createDataFrame(sqlContext, iris))
  after <- suppressWarnings(createDataFrame(iris))
  expect_equal(collect(before), collect(after))

  # more tests for SPARK-16538
  createOrReplaceTempView(df, "table")
  SparkR::tables()
  SparkR::sql("SELECT 1")
  suppressWarnings(SparkR::sql(sqlContext, "SELECT * FROM table"))
  suppressWarnings(SparkR::dropTempTable(sqlContext, "table"))
})

test_that("randomSplit", {
  num <- 4000
  df <- createDataFrame(data.frame(id = 1:num))
  weights <- c(2, 3, 5)
  df_list <- randomSplit(df, weights)
  expect_equal(length(weights), length(df_list))
  counts <- sapply(df_list, count)
  expect_equal(num, sum(counts))
  expect_true(all(sapply(abs(counts / num - weights / sum(weights)), function(e) { e < 0.05 })))

  df_list <- randomSplit(df, weights, 0)
  expect_equal(length(weights), length(df_list))
  counts <- sapply(df_list, count)
  expect_equal(num, sum(counts))
  expect_true(all(sapply(abs(counts / num - weights / sum(weights)), function(e) { e < 0.05 })))
})

test_that("Setting and getting config on SparkSession", {
  # first, set it to a random but known value
  conf <- callJMethod(sparkSession, "conf")
  property <- paste0("spark.testing.", as.character(runif(1)))
  value1 <- as.character(runif(1))
  callJMethod(conf, "set", property, value1)

  # next, change the same property to the new value
  value2 <- as.character(runif(1))
  l <- list(value2)
  names(l) <- property
  sparkR.session(sparkConfig = l)

  newValue <- unlist(sparkR.conf(property, ""), use.names = FALSE)
  expect_equal(value2, newValue)

  value <- as.character(runif(1))
  sparkR.session(spark.app.name = "sparkSession test", spark.testing.r.session.r = value)
  allconf <- sparkR.conf()
  appNameValue <- allconf[["spark.app.name"]]
  testValue <- allconf[["spark.testing.r.session.r"]]
  expect_equal(appNameValue, "sparkSession test")
  expect_equal(testValue, value)
  expect_error(sparkR.conf("completely.dummy"), "Config 'completely.dummy' is not set")
})

test_that("enableHiveSupport on SparkSession", {
  setHiveContext(sc)
  unsetHiveContext()
  # if we are still here, it must be built with hive
  conf <- callJMethod(sparkSession, "conf")
  value <- callJMethod(conf, "get", "spark.sql.catalogImplementation", "")
  expect_equal(value, "hive")
})

test_that("Spark version from SparkSession", {
  ver <- callJMethod(sc, "version")
  version <- sparkR.version()
  expect_equal(ver, version)
})

test_that("Call DataFrameWriter.save() API in Java without path and check argument types", {
  df <- read.df(jsonPath, "json")
  # This tests if the exception is thrown from JVM not from SparkR side.
  # It makes sure that we can omit path argument in write.df API and then it calls
  # DataFrameWriter.save() without path.
  expect_error(write.df(df, source = "csv"),
               "Error in save : illegal argument - 'path' is not specified")

  # Arguments checking in R side.
  expect_error(write.df(df, "data.tmp", source = c(1, 2)),
               paste("source should be character, NULL or omitted. It is the datasource specified",
                     "in 'spark.sql.sources.default' configuration by default."))
  expect_error(write.df(df, path = c(3)),
               "path should be charactor, NULL or omitted.")
  expect_error(write.df(df, mode = TRUE),
               "mode should be charactor or omitted. It is 'error' by default.")
})

test_that("Call DataFrameWriter.load() API in Java without path and check argument types", {
  # This tests if the exception is thrown from JVM not from SparkR side.
  # It makes sure that we can omit path argument in read.df API and then it calls
  # DataFrameWriter.load() without path.
  expect_error(read.df(source = "json"),
               paste("Error in loadDF : analysis error - Unable to infer schema for JSON at .",
                     "It must be specified manually"))
  expect_error(read.df("arbitrary_path"), "Error in loadDF : analysis error - Path does not exist")

  # Arguments checking in R side.
  expect_error(read.df(path = c(3)),
               "path should be charactor, NULL or omitted.")
  expect_error(read.df(jsonPath, source = c(1, 2)),
               paste("source should be character, NULL or omitted. It is the datasource specified",
                     "in 'spark.sql.sources.default' configuration by default."))
})

unlink(parquetPath)
unlink(orcPath)
unlink(jsonPath)
unlink(jsonPathNa)

sparkR.session.stop()
