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

filesBefore <- list.files(path = sparkRDir, all.files = TRUE)
sparkSession <- if (windows_with_hadoop()) {
    sparkR.session(master = sparkRTestMaster)
  } else {
    sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)
  }
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)
# materialize the catalog implementation
listTables()

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

# For test map type and struct type in DataFrame
mockLinesMapType <- c("{\"name\":\"Bob\",\"info\":{\"age\":16,\"height\":176.5}}",
                      "{\"name\":\"Alice\",\"info\":{\"age\":20,\"height\":164.3}}",
                      "{\"name\":\"David\",\"info\":{\"age\":60,\"height\":180}}")
mapTypeJsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
writeLines(mockLinesMapType, mapTypeJsonPath)

if (is_windows()) {
  Sys.setenv(TZ = "GMT")
}

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

  testSchema <- structType("a STRING, b INT")
  expect_is(testSchema, "structType")
  expect_is(testSchema$fields()[[2]], "structField")
  expect_equal(testSchema$fields()[[1]]$dataType.toString(), "StringType")

  expect_error(structType("A stri"), "DataType stri is not supported.")
})

test_that("structField type strings", {
  # positive cases
  primitiveTypes <- list(byte = "ByteType",
                         integer = "IntegerType",
                         float = "FloatType",
                         double = "DoubleType",
                         string = "StringType",
                         binary = "BinaryType",
                         boolean = "BooleanType",
                         timestamp = "TimestampType",
                         date = "DateType",
                         tinyint = "ByteType",
                         smallint = "ShortType",
                         int = "IntegerType",
                         bigint = "LongType",
                         decimal = "DecimalType(10,0)")

  complexTypes <- list("map<string,integer>" = "MapType(StringType,IntegerType,true)",
                       "array<string>" = "ArrayType(StringType,true)",
                       "struct<a:string>" = "StructType(StructField(a,StringType,true))")

  typeList <- c(primitiveTypes, complexTypes)
  typeStrings <- names(typeList)

  for (i in seq_along(typeStrings)) {
    typeString <- typeStrings[i]
    expected <- typeList[[i]]
    testField <- structField("_col", typeString)
    expect_is(testField, "structField")
    expect_true(testField$nullable())
    expect_equal(testField$dataType.toString(), expected)
  }

  # negative cases
  primitiveErrors <- list(Byte = "Byte",
                          INTEGER = "INTEGER",
                          numeric = "numeric",
                          character = "character",
                          raw = "raw",
                          logical = "logical",
                          short = "short",
                          varchar = "varchar",
                          long = "long",
                          char = "char")

  complexErrors <- list("map<string, integer>" = " integer",
                        "array<String>" = "String",
                        "struct<a:string >" = "string ",
                        "map <string,integer>" = "map <string,integer>",
                        "array< string>" = " string",
                        "struct<a: string>" = " string")

  errorList <- c(primitiveErrors, complexErrors)
  typeStrings <- names(errorList)

  for (i in seq_along(typeStrings)) {
    typeString <- typeStrings[i]
    expected <- paste0("Unsupported type for SparkDataframe: ", errorList[[i]])
    expect_error(structField("_col", typeString), expected)
  }
})

test_that("create DataFrame from RDD", {
  rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
  df <- SparkR::createDataFrame(rdd, list("a", "b"))
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
  expect_equal(getNumPartitions(df), 1)

  df <- as.DataFrame(cars, numPartitions = 2)
  expect_equal(getNumPartitions(df), 2)
  df <- SparkR::createDataFrame(cars, numPartitions = 3)
  expect_equal(getNumPartitions(df), 3)
  # validate limit by num of rows
  df <- createDataFrame(cars, numPartitions = 60)
  expect_equal(getNumPartitions(df), 50)
  # validate when 1 < (length(coll) / numSlices) << length(coll)
  df <- createDataFrame(cars, numPartitions = 20)
  expect_equal(getNumPartitions(df), 20)

  df <- as.DataFrame(data.frame(0))
  expect_is(df, "SparkDataFrame")
  df <- createDataFrame(list(list(1)))
  expect_is(df, "SparkDataFrame")
  df <- as.DataFrame(data.frame(0), numPartitions = 2)
  # no data to partition, goes to 1
  expect_equal(getNumPartitions(df), 1)

  setHiveContext(sc)
  sql("CREATE TABLE people (name string, age double, height float)")
  df <- read.df(jsonPathNa, "json", schema)
  insertInto(df, "people")
  expect_equal(collect(SparkR::sql("SELECT age from people WHERE name = 'Bob'"))$age,
               c(16))
  expect_equal(collect(sql("SELECT height from people WHERE name ='Bob'"))$height,
               c(176.5))
  sql("DROP TABLE people")
  unsetHiveContext()
})

test_that("read/write csv as DataFrame", {
  if (windows_with_hadoop()) {
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
  }
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

test_that("SPARK-17902: collect() with stringsAsFactors enabled", {
  df <- suppressWarnings(collect(createDataFrame(iris), stringsAsFactors = TRUE))
  expect_equal(class(iris$Species), class(df$Species))
  expect_equal(iris$Species, df$Species)
})

test_that("SPARK-17811: can create DataFrame containing NA as date and time", {
  df <- data.frame(
    id = 1:2,
    time = c(as.POSIXlt("2016-01-10"), NA),
    date = c(as.Date("2016-10-01"), NA))

  DF <- collect(createDataFrame(df))
  expect_true(is.na(DF$date[2]))
  expect_equal(DF$date[1], as.Date("2016-10-01"))
  expect_true(is.na(DF$time[2]))
  expect_equal(DF$time[1], as.POSIXlt("2016-01-10"))
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

test_that("Collect DataFrame with complex types", {
  # ArrayType
  df <- read.json(complexTypeJsonPath)
  ldf <- collect(df)
  expect_equal(nrow(ldf), 3)
  expect_equal(ncol(ldf), 3)
  expect_equal(names(ldf), c("c1", "c2", "c3"))
  expect_equal(ldf$c1, list(list(1, 2, 3), list(4, 5, 6), list(7, 8, 9)))
  expect_equal(ldf$c2, list(list("a", "b", "c"), list("d", "e", "f"), list("g", "h", "i")))
  expect_equal(ldf$c3, list(list(1.0, 2.0, 3.0), list(4.0, 5.0, 6.0), list(7.0, 8.0, 9.0)))

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
  if (windows_with_hadoop()) {
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

    # Test errorifexists
    expect_error(write.df(df, jsonPath2, "json", mode = "errorifexists"),
                 "analysis error - path file:.*already exists")

    # Test write.json
    jsonPath3 <- tempfile(pattern = "jsonPath3", fileext = ".json")
    write.json(df, jsonPath3)

    # Test read.json() works with multiple input paths
    jsonDF1 <- read.json(c(jsonPath2, jsonPath3))
    expect_is(jsonDF1, "SparkDataFrame")
    expect_equal(count(jsonDF1), 6)

    unlink(jsonPath2)
    unlink(jsonPath3)
  }
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

test_that("test tableNames and tables", {
  count <- count(listTables())

  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  expect_equal(length(tableNames()), count + 1)
  expect_equal(length(tableNames("default")), count + 1)

  tables <- listTables()
  expect_equal(count(tables), count + 1)
  expect_equal(count(tables()), count(tables))
  expect_true("tableName" %in% colnames(tables()))
  expect_true(all(c("tableName", "namespace", "isTemporary") %in% colnames(tables())))

  suppressWarnings(registerTempTable(df, "table2"))
  tables <- listTables()
  expect_equal(count(tables), count + 2)
  suppressWarnings(dropTempTable("table1"))
  expect_true(dropTempView("table2"))

  tables <- listTables()
  expect_equal(count(tables), count + 0)

  count2 <- count(listTables())
  schema <- structType(structField("name", "string"), structField("age", "integer"),
                       structField("height", "float"))
  createTable("people", source = "json", schema = schema)

  expect_equal(length(tableNames()), count2 + 1)
  expect_equal(length(tableNames("default")), count2 + 1)
  expect_equal(length(tableNames("spark_catalog.default")), count2 + 1)

  tables <- listTables()
  expect_equal(count(tables), count2 + 1)
  expect_equal(count(tables()), count(tables))
  expect_equal(count(tables("default")), count2 + 1)
  expect_equal(count(tables("spark_catalog.default")), count2 + 1)
  sql("DROP TABLE IF EXISTS people")
})

test_that(
  "createOrReplaceTempView() results in a queryable table and sql() results in a new DataFrame", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  newdf <- sql("SELECT * FROM table1 where name = 'Michael'")
  expect_is(newdf, "SparkDataFrame")
  expect_equal(count(newdf), 1)
  expect_true(dropTempView("table1"))

  createOrReplaceTempView(df, "dfView")
  sqlCast <- collect(sql("select cast('2' as decimal) as x from dfView limit 1"))
  out <- capture.output(sqlCast)
  expect_true(is.data.frame(sqlCast))
  expect_equal(names(sqlCast)[1], "x")
  expect_equal(nrow(sqlCast), 1)
  expect_equal(ncol(sqlCast), 1)
  expect_equal(out[1], "  x")
  expect_equal(out[2], "1 2")
  expect_true(dropTempView("dfView"))
})

test_that("test tableExists, cache, uncache and clearCache", {
  schema <- structType(structField("name", "string"), structField("age", "integer"),
                       structField("height", "float"))
  createTable("table1", source = "json", schema = schema)

  cacheTable("default.table1")
  uncacheTable("spark_catalog.default.table1")
  clearCache()

  expect_error(uncacheTable("zxwtyswklpf"),
      "Error in uncacheTable : analysis error - Table or view not found: zxwtyswklpf")

  expect_true(tableExists("table1"))
  expect_true(tableExists("default.table1"))
  expect_true(tableExists("spark_catalog.default.table1"))

  sql("DROP TABLE IF EXISTS spark_catalog.default.table1")

  expect_false(tableExists("table1"))
  expect_false(tableExists("default.table1"))
  expect_false(tableExists("spark_catalog.default.table1"))
})

test_that("insertInto() on a registered table", {
  if (windows_with_hadoop()) {
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
    expect_true(dropTempView("table1"))

    createOrReplaceTempView(dfParquet, "table1")
    insertInto(dfParquet2, "table1", overwrite = TRUE)
    expect_equal(count(sql("select * from table1")), 2)
    expect_equal(first(sql("select * from table1 order by age"))$name, "Bob")
    expect_true(dropTempView("table1"))

    unlink(jsonPath2)
    unlink(parquetPath2)
  }
})

test_that("tableToDF() returns a new DataFrame", {
  df <- read.json(jsonPath)
  createOrReplaceTempView(df, "table1")
  tabledf <- tableToDF("table1")
  expect_is(tabledf, "SparkDataFrame")
  expect_equal(count(tabledf), 3)
  tabledf2 <- tableToDF("table1")
  expect_equal(count(tabledf2), 3)
  expect_true(dropTempView("table1"))
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
  saveAsObjectFile(coalesceRDD(dfRDD, 1L), objectPath)
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
  jsonPath <- file.path(
    Sys.getenv("SPARK_HOME"),
    "R", "pkg", "tests", "fulltests", "data",
    "test_utils_utf.json"
  )

  lines <- readLines(jsonPath, encoding = "UTF-8")

  expected <- regmatches(lines, gregexpr('(?<="name": ").*?(?=")', lines, perl = TRUE))

  df <- read.df(jsonPath, "json")
  rdf <- collect(df)
  expect_true(is.data.frame(rdf))
  expect_equal(rdf$name[1], expected[[1]])
  expect_equal(rdf$name[2], expected[[2]])
  expect_equal(rdf$name[3], expected[[3]])
  expect_equal(rdf$name[4], expected[[4]])

  df1 <- createDataFrame(rdf)
  expect_equal(
    collect(
      where(df1, df1$name == expected[[2]])
    )$name,
    expected[[2]]
  )
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

test_that("cache(), storageLevel(), persist(), and unpersist() on a DataFrame", {
  df <- read.json(jsonPath)
  expect_false(df@env$isCached)
  cache(df)
  expect_true(df@env$isCached)

  unpersist(df)
  expect_false(df@env$isCached)

  persist(df, "MEMORY_AND_DISK")
  expect_true(df@env$isCached)

  expect_equal(storageLevel(df),
    "MEMORY_AND_DISK - StorageLevel(disk, memory, deserialized, 1 replicas)")

  unpersist(df)
  expect_false(df@env$isCached)

  # make sure the data is collectable
  expect_true(is.data.frame(collect(df)))
})

test_that("setCheckpointDir(), checkpoint() on a DataFrame", {
  if (windows_with_hadoop()) {
    checkpointDir <- file.path(tempdir(), "cproot")
    expect_true(length(list.files(path = checkpointDir, all.files = TRUE)) == 0)

    setCheckpointDir(checkpointDir)
    df <- read.json(jsonPath)
    df <- checkpoint(df)
    expect_is(df, "SparkDataFrame")
    expect_false(length(list.files(path = checkpointDir, all.files = TRUE)) == 0)
  }
})

test_that("localCheckpoint() on a DataFrame", {
  if (windows_with_hadoop()) {
    # Checkpoint directory shouldn't matter in localCheckpoint.
    checkpointDir <- file.path(tempdir(), "lcproot")
    expect_true(length(list.files(path = checkpointDir, all.files = TRUE, recursive = TRUE)) == 0)
    setCheckpointDir(checkpointDir)

    textPath <- tempfile(pattern = "textPath", fileext = ".txt")
    writeLines(mockLines, textPath)
    # Read it lazily and then locally checkpoint eagerly.
    df <- read.df(textPath, "text")
    df <- localCheckpoint(df, eager = TRUE)
    # Here, we remove the source path to check eagerness.
    unlink(textPath)
    expect_is(df, "SparkDataFrame")
    expect_equal(colnames(df), c("value"))
    expect_equal(count(df), 3)

    expect_true(length(list.files(path = checkpointDir, all.files = TRUE, recursive = TRUE)) == 0)
  }
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

  expect_error(names(df) <- NULL, "Invalid column names.")
  expect_error(names(df) <- c("sepal.length", "sepal_width"),
               "Column names cannot contain the '.' symbol.")
  expect_error(names(df) <- c(1, 2), "Invalid column names.")
  expect_error(names(df) <- c("a"),
               "Column names must have the same length as the number of columns in the dataset.")
  expect_error(names(df) <- c("1", NA), "Column names cannot be NA.")

  expect_error(colnames(df) <- c("sepal.length", "sepal_width"),
               "Column names cannot contain the '.' symbol.")
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

  # Test subset assignment
  colnames(df)[1] <- "col5"
  expect_equal(colnames(df)[1], "col5")
  names(df)[2] <- "col6"
  expect_equal(names(df)[2], "col6")
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

  # Different arguments
  df <- createDataFrame(as.list(seq(10)))
  expect_equal(count(sample(df, fraction = 0.5, seed = 3)), 4)
  expect_equal(count(sample(df, withReplacement = TRUE, fraction = 0.5, seed = 3)), 2)
  expect_equal(count(sample(df, fraction = 1.0)), 10)
  expect_equal(count(sample(df, fraction = 1L)), 10)
  expect_equal(count(sample(df, FALSE, fraction = 1.0)), 10)

  expect_error(sample(df, fraction = "a"), "fraction must be numeric")
  expect_error(sample(df, "a", fraction = 0.1), "however, got character")
  expect_error(sample(df, fraction = 1, seed = NA), "seed must not be NULL or NA; however, got NA")
  expect_error(sample(df, fraction = -1.0),
               "illegal argument - requirement failed: Sampling fraction \\(-1.0\\)")

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

  expect_warning(df[[1:2]],
                 "Subset index has length > 1. Only the first index is used.")
  expect_is(suppressWarnings(df[[1:2]]), "Column")
  expect_warning(df[[c("name", "age")]],
                 "Subset index has length > 1. Only the first index is used.")
  expect_is(suppressWarnings(df[[c("name", "age")]]), "Column")

  expect_warning(df[[1:2]] <- df[[1]],
                 "Subset index has length > 1. Only the first index is used.")
  expect_warning(df[[c("name", "age")]] <- df[[1]],
                 "Subset index has length > 1. Only the first index is used.")

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
  df$age2 <- df[["age"]] * 3
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == df$age * 3)), 2)

  df$age2 <- 21
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == 21)), 3)

  df$age2 <- c(22)
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == 22)), 3)

  expect_error(df$age3 <- c(22, NA),
              "value must be a Column, literal value as atomic in length of 1, or NULL")

  df[["age2"]] <- 23
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == 23)), 3)

  df[[3]] <- 24
  expect_equal(columns(df), c("name", "age", "age2"))
  expect_equal(count(where(df, df$age2 == 24)), 3)

  df[[3]] <- df$age
  expect_equal(count(where(df, df$age2 == df$age)), 2)

  df[["age2"]] <- df[["name"]]
  expect_equal(count(where(df, df$age2 == df$name)), 3)

  expect_error(df[["age3"]] <- c(22, 23),
              "value must be a Column, literal value as atomic in length of 1, or NULL")

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

  # Test select with alias
  df5 <- alias(df, "table")

  expect_equal(columns(select(df5, column("table.name"))), "name")
  expect_equal(columns(select(df5, "table.name")), "name")

  # Test that stats::alias is not masked
  expect_is(alias(aov(yield ~ block + N * P * K, npk)), "listof")


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

  df1 <- drop(df, df$age, df$name)
  expect_equal(columns(df1), c("age2"))

  df1 <- drop(df, df$age, column("random"))
  expect_equal(columns(df1), c("name", "age2"))

  df1 <- drop(df, df$age, "random")
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
  if (windows_with_hadoop()) {
    setHiveContext(sc)

    schema <- structType(structField("name", "string"), structField("age", "integer"),
                         structField("height", "float"))
    createTable("spark_catalog.default.people", source = "json", schema = schema)
    df <- read.df(jsonPathNa, "json", schema)
    insertInto(df, "people")
    expect_equal(collect(sql("SELECT age from people WHERE name = 'Bob'"))$age, c(16))
    sql("DROP TABLE people")

    df <- createTable("json", jsonPath, "json")
    expect_is(df, "SparkDataFrame")
    expect_equal(count(df), 3)
    df2 <- sql("select * from json")
    expect_is(df2, "SparkDataFrame")
    expect_equal(count(df2), 3)

    jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
    saveAsTable(df, "json2", "json", "append", path = jsonPath2)
    df3 <- sql("select * from json2")
    expect_is(df3, "SparkDataFrame")
    expect_equal(count(df3), 3)
    unlink(jsonPath2)

    hivetestDataPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
    saveAsTable(df, "hivetestbl", path = hivetestDataPath)
    df4 <- sql("select * from hivetestbl")
    expect_is(df4, "SparkDataFrame")
    expect_equal(count(df4), 3)
    unlink(hivetestDataPath)

    parquetDataPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
    saveAsTable(df, "parquetest", "parquet", mode = "overwrite", path = parquetDataPath)
    df5 <- sql("select * from parquetest")
    expect_is(df5, "SparkDataFrame")
    expect_equal(count(df5), 3)
    unlink(parquetDataPath)

    # Invalid mode
    expect_error(saveAsTable(df, "parquetest", "parquet", mode = "abc", path = parquetDataPath),
                 "illegal argument - Unknown save mode: abc")
    unsetHiveContext()
  }
})

test_that("column operators", {
  c <- column("a")
  c2 <- (- c + 1 - 2) * 3 / 4.0
  c3 <- (c + c2 - c2) * c2 %% c2
  c4 <- (c > c2) & (c2 <= c3) | (c == c2) & (c2 != c3)
  c5 <- c2 ^ c3 ^ c4
  c6 <- c2 %<=>% c3
  c7 <- !c6
  expect_true(TRUE)
})

test_that("column functions", {
  c <- column("a")
  c1 <- abs(c) + acos(c) + approx_count_distinct(c) + ascii(c) + asin(c) + atan(c)
  c2 <- avg(c) + base64(c) + bin(c) + suppressWarnings(bitwiseNOT(c)) +
    bitwise_not(c) + cbrt(c) + ceil(c) + cos(c)
  c3 <- cosh(c) + count(c) + crc32(c) + hash(c) + exp(c)
  c4 <- explode(c) + expm1(c) + factorial(c) + first(c) + floor(c) + hex(c)
  c5 <- hour(c) + initcap(c) + last(c) + last_day(c) + length(c)
  c6 <- log(c) + (c) + log1p(c) + log2(c) + lower(c) + ltrim(c) + max(c) + md5(c)
  c7 <- mean(c) + min(c) + month(c) + negate(c) + posexplode(c) + quarter(c)
  c8 <- reverse(c) + rint(c) + round(c) + rtrim(c) + sha1(c) + monotonically_increasing_id()
  c9 <- signum(c) + sin(c) + sinh(c) + size(c) + stddev(c) + soundex(c) + sqrt(c) + sum(c)
  c10 <- suppressWarnings(sumDistinct(c)) + sum_distinct(c) + tan(c) + tanh(c) +
    degrees(c) + radians(c)
  c11 <- to_date(c) + trim(c) + unbase64(c) + unhex(c) + upper(c)
  c12 <- variance(c) + xxhash64(c) + ltrim(c, "a") + rtrim(c, "b") + trim(c, "c")
  c13 <- lead("col", 1) + lead(c, 1) + lag("col", 1) + lag(c, 1)
  c14 <- cume_dist() + ntile(1) + corr(c, c1)
  c15 <- dense_rank() + percent_rank() + rank() + row_number()
  c16 <- is.nan(c) + isnan(c) + isNaN(c)
  c17 <- cov(c, c1) + cov("c", "c1") + covar_samp(c, c1) + covar_samp("c", "c1")
  c18 <- covar_pop(c, c1) + covar_pop("c", "c1")
  c19 <- spark_partition_id() + coalesce(c) + coalesce(c1, c2, c3)
  c20 <- to_timestamp(c) + to_timestamp(c, "yyyy") + to_date(c, "yyyy")
  c21 <- posexplode_outer(c) + explode_outer(c)
  c22 <- not(c)
  c23 <- trunc(c, "year") + trunc(c, "yyyy") + trunc(c, "yy") +
    trunc(c, "month") + trunc(c, "mon") + trunc(c, "mm")
  c24 <- date_trunc("hour", c) + date_trunc("minute", c) + date_trunc("week", c) +
    date_trunc("quarter", c) + current_date() + current_timestamp()
  c25 <- overlay(c1, c2, c3, c3) + overlay(c1, c2, c3) + overlay(c1, c2, 1) +
    overlay(c1, c2, 3, 4)
  c26 <- timestamp_seconds(c1) + vector_to_array(c) +
    vector_to_array(c, "float32") + vector_to_array(c, "float64") +
    array_to_vector(c)
  c27 <- nth_value("x", 1L) + nth_value("y", 2, TRUE) +
    nth_value(column("v"), 3) + nth_value(column("z"), 4L, FALSE)
  c28 <- asc_nulls_first(c1) + asc_nulls_last(c1) +
    desc_nulls_first(c1) + desc_nulls_last(c1)
  c29 <- acosh(c1) + asinh(c1) + atanh(c1)
  c30 <- product(c1) + product(c1 * 0.5)
  c31 <- sec(c1) + csc(c1) + cot(c1)

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

  # Test that input_file_name()
  actual_names <- collect(distinct(select(df, input_file_name())))
  expect_equal(length(actual_names), 1)
  expect_equal(basename(actual_names[1, 1]), basename(jsonPath))

  df3 <- select(df, between(df$name, c("Apache", "Spark")))
  expect_equal(collect(df3)[[1, 1]], TRUE)
  expect_equal(collect(df3)[[2, 1]], FALSE)
  expect_equal(collect(df3)[[3, 1]], TRUE)

  df4 <- select(df, count_distinct(df$age, df$name))
  expect_equal(collect(df4)[[1, 1]], 2)
  df4 <- select(df, countDistinct(df$age, df$name))
  expect_equal(collect(df4)[[1, 1]], 2)

  expect_equal(collect(select(df, sum(df$age)))[1, 1], 49)
  expect_true(abs(collect(select(df, stddev(df$age)))[1, 1] - 7.778175) < 1e-6)
  expect_equal(collect(select(df, var_pop(df$age)))[1, 1], 30.25)

  df5 <- createDataFrame(list(list(a = "010101")))
  expect_equal(collect(select(df5, conv(df5$a, 2, 16)))[1, 1], "15")

  # Test months_between()
  df <- createDataFrame(list(list(a = as.Date("1997-02-28"),
                                  b = as.Date("1996-10-30"))))
  result1 <- collect(select(df, alias(months_between(df[[1]], df[[2]]), "month")))[[1]]
  expect_equal(result1, 3.93548387)
  result2 <- collect(select(df, alias(months_between(df[[1]], df[[2]], FALSE), "month")))[[1]]
  expect_equal(result2, 3.935483870967742)

  # Test array_contains(), array_max(), array_min(), array_position(), element_at() and reverse()
  df <- createDataFrame(list(list(list(1L, 2L, 3L)), list(list(6L, 5L, 4L))))
  result <- collect(select(df, array_contains(df[[1]], 1L)))[[1]]
  expect_equal(result, c(TRUE, FALSE))

  result <- collect(select(df, array_max(df[[1]])))[[1]]
  expect_equal(result, c(3, 6))

  result <- collect(select(df, array_min(df[[1]])))[[1]]
  expect_equal(result, c(1, 4))

  result <- collect(select(df, array_position(df[[1]], 1L)))[[1]]
  expect_equal(result, c(1, 0))

  result <- collect(select(df, element_at(df[[1]], 1L)))[[1]]
  expect_equal(result, c(1, 6))

  result <- collect(select(df, reverse(df[[1]])))[[1]]
  expect_equal(result, list(list(3L, 2L, 1L), list(4L, 5L, 6L)))

  df2 <- createDataFrame(list(list("abc")))
  result <- collect(select(df2, reverse(df2[[1]])))[[1]]
  expect_equal(result, "cba")

  # Test array_distinct() and array_remove()
  df <- createDataFrame(list(list(list(1L, 2L, 3L, 1L, 2L)), list(list(6L, 5L, 5L, 4L, 6L))))
  result <- collect(select(df, array_distinct(df[[1]])))[[1]]
  expect_equal(result, list(list(1L, 2L, 3L), list(6L, 5L, 4L)))

  result <- collect(select(df, array_remove(df[[1]], 2L)))[[1]]
  expect_equal(result, list(list(1L, 3L, 1L), list(6L, 5L, 5L, 4L, 6L)))

  # Test arrays_zip()
  df <- createDataFrame(list(list(list(1L, 2L), list(3L, 4L))), schema = c("c1", "c2"))
  result <- collect(select(df, arrays_zip(df[[1]], df[[2]])))[[1]]
  expected_entries <-  list(listToStruct(list(c1 = 1L, c2 = 3L)),
                            listToStruct(list(c1 = 2L, c2 = 4L)))
  expect_equal(result, list(expected_entries))

  # Test map_from_arrays()
  df <- createDataFrame(list(list(list("x", "y"), list(1, 2))), schema = c("k", "v"))
  result <- collect(select(df, map_from_arrays(df$k, df$v)))[[1]]
  expected_entries <- list(as.environment(list(x = 1, y = 2)))
  expect_equal(result, expected_entries)

  # Test map_from_entries()
  df <- createDataFrame(list(list(list(listToStruct(list(c1 = "x", c2 = 1L)),
                                       listToStruct(list(c1 = "y", c2 = 2L))))))
  result <- collect(select(df, map_from_entries(df[[1]])))[[1]]
  expected_entries <- list(as.environment(list(x = 1L, y = 2L)))
  expect_equal(result, expected_entries)

  # Test array_repeat()
  df <- createDataFrame(list(list("a", 3L), list("b", 2L)))
  result <- collect(select(df, array_repeat(df[[1]], df[[2]])))[[1]]
  expect_equal(result, list(list("a", "a", "a"), list("b", "b")))

  result <- collect(select(df, array_repeat(df[[1]], 2L)))[[1]]
  expect_equal(result, list(list("a", "a"), list("b", "b")))

  # Test arrays_overlap()
  df <- createDataFrame(list(list(list(1L, 2L), list(3L, 1L)),
                             list(list(1L, 2L), list(3L, 4L)),
                             list(list(1L, NA), list(3L, 4L))))
  result <- collect(select(df, arrays_overlap(df[[1]], df[[2]])))[[1]]
  expect_equal(result, c(TRUE, FALSE, NA))

  # Test array_join()
  df <- createDataFrame(list(list(list("Hello", "World!"))))
  result <- collect(select(df, array_join(df[[1]], "#")))[[1]]
  expect_equal(result, "Hello#World!")
  df2 <- createDataFrame(list(list(list("Hello", NA, "World!"))))
  result <- collect(select(df2, array_join(df2[[1]], "#", "Beautiful")))[[1]]
  expect_equal(result, "Hello#Beautiful#World!")
  result <- collect(select(df2, array_join(df2[[1]], "#")))[[1]]
  expect_equal(result, "Hello#World!")
  df3 <- createDataFrame(list(list(list("Hello", NULL, "World!"))))
  result <- collect(select(df3, array_join(df3[[1]], "#", "Beautiful")))[[1]]
  expect_equal(result, "Hello#Beautiful#World!")
  result <- collect(select(df3, array_join(df3[[1]], "#")))[[1]]
  expect_equal(result, "Hello#World!")

  # Test array_sort() and sort_array()
  df <- createDataFrame(list(list(list(2L, 1L, 3L, NA)), list(list(NA, 6L, 5L, NA, 4L))))

  result <- collect(select(df, array_sort(df[[1]])))[[1]]
  expect_equal(result, list(list(1L, 2L, 3L, NA), list(4L, 5L, 6L, NA, NA)))

  result <- collect(select(df, sort_array(df[[1]], FALSE)))[[1]]
  expect_equal(result, list(list(3L, 2L, 1L, NA), list(6L, 5L, 4L, NA, NA)))
  result <- collect(select(df, sort_array(df[[1]])))[[1]]
  expect_equal(result, list(list(NA, 1L, 2L, 3L), list(NA, NA, 4L, 5L, 6L)))

  # Test slice()
  df <- createDataFrame(list(list(list(1L, 2L, 3L)), list(list(4L, 5L))))
  result <- collect(select(df, slice(df[[1]], 2L, 2L)))[[1]]
  expect_equal(result, list(list(2L, 3L), list(5L)))

  # Test concat()
  df <- createDataFrame(list(list(list(1L, 2L, 3L), list(4L, 5L, 6L)),
                        list(list(7L, 8L, 9L), list(10L, 11L, 12L))))
  result <- collect(select(df, concat(df[[1]], df[[2]])))[[1]]
  expect_equal(result, list(list(1L, 2L, 3L, 4L, 5L, 6L), list(7L, 8L, 9L, 10L, 11L, 12L)))

  # Test flatten()
  df <- createDataFrame(list(list(list(list(1L, 2L), list(3L, 4L))),
                        list(list(list(5L, 6L), list(7L, 8L)))))
  result <- collect(select(df, flatten(df[[1]])))[[1]]
  expect_equal(result, list(list(1L, 2L, 3L, 4L), list(5L, 6L, 7L, 8L)))

  # Test map_concat
  df <- createDataFrame(list(list(map1 = as.environment(list(x = 1, y = 2)),
                                  map2 = as.environment(list(a = 3, b = 4)))))
  result <- collect(select(df, map_concat(df[[1]], df[[2]])))[[1]]
  expected_entries <- list(as.environment(list(x = 1, y = 2, a = 3, b = 4)))
  expect_equal(result, expected_entries)

  # Test map_entries(), map_keys(), map_values() and element_at()
  df <- createDataFrame(list(list(map = as.environment(list(x = 1, y = 2)))))
  result <- collect(select(df, map_entries(df$map)))[[1]]
  expected_entries <-  list(listToStruct(list(key = "x", value = 1)),
                            listToStruct(list(key = "y", value = 2)))
  expect_equal(result, list(expected_entries))

  result <- collect(select(df, map_keys(df$map)))[[1]]
  expect_equal(result, list(list("x", "y")))

  result <- collect(select(df, map_values(df$map)))[[1]]
  expect_equal(result, list(list(1, 2)))

  result <- collect(select(df, element_at(df$map, "y")))[[1]]
  expect_equal(result, 2)

  # Test array_except(), array_intersect() and array_union()
  df <- createDataFrame(list(list(list(1L, 2L, 3L), list(3L, 1L)),
                             list(list(1L, 2L), list(3L, 4L)),
                             list(list(1L, 2L, 3L), list(3L, 4L))))
  result1 <- collect(select(df, array_except(df[[1]], df[[2]])))[[1]]
  expect_equal(result1, list(list(2L), list(1L, 2L), list(1L, 2L)))

  result2 <- collect(select(df, array_intersect(df[[1]], df[[2]])))[[1]]
  expect_equal(result2, list(list(1L, 3L), list(), list(3L)))

  result3 <- collect(select(df, array_union(df[[1]], df[[2]])))[[1]]
  expect_equal(result3, list(list(1L, 2L, 3L), list(1L, 2L, 3L, 4L), list(1L, 2L, 3L, 4L)))

  # Test shuffle()
  df <- createDataFrame(list(list(list(1L, 20L, 3L, 5L)), list(list(4L, 5L, 6L, 7L))))
  result <- collect(select(df, shuffle(df[[1]])))[[1]]
  expect_true(setequal(result[[1]], c(1L, 20L, 3L, 5L)))
  expect_true(setequal(result[[2]], c(4L, 5L, 6L, 7L)))

  # Test that stats::lag is working
  expect_equal(length(lag(ldeaths, 12)), 72)

  # Test struct()
  df <- createDataFrame(list(list(1L, 2L, 3L), list(4L, 5L, 6L)),
                        schema = c("a", "b", "c"))
  result <- collect(select(df, alias(struct("a", "c"), "d")))
  expected <- data.frame(row.names = 1:2)
  expected$"d" <- list(listToStruct(list(a = 1L, c = 3L)),
                      listToStruct(list(a = 4L, c = 6L)))
  expect_equal(result, expected)

  result <- collect(select(df, alias(struct(df$a, df$b), "d")))
  expected <- data.frame(row.names = 1:2)
  expected$"d" <- list(listToStruct(list(a = 1L, b = 2L)),
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
  expect_equal(collect(select(df, first(df$age)))[[1]], NA_real_)
  expect_equal(collect(select(df, first(df$age, TRUE)))[[1]], 30)
  expect_equal(collect(select(df, first("age")))[[1]], NA_real_)
  expect_equal(collect(select(df, first("age", TRUE)))[[1]], 30)
  expect_equal(collect(select(df, last(df$age)))[[1]], 19)
  expect_equal(collect(select(df, last(df$age, TRUE)))[[1]], 19)
  expect_equal(collect(select(df, last("age")))[[1]], 19)
  expect_equal(collect(select(df, last("age", TRUE)))[[1]], 19)

  # Test bround()
  df <- createDataFrame(data.frame(x = c(2.5, 3.5)))
  expect_equal(collect(select(df, bround(df$x, 0)))[[1]][1], 2)
  expect_equal(collect(select(df, bround(df$x, 0)))[[1]][2], 4)

  # Test from_csv(), schema_of_csv()
  df <- as.DataFrame(list(list("col" = "1")))
  c <- collect(select(df, alias(from_csv(df$col, "a INT"), "csv")))
  expect_equal(c[[1]][[1]]$a, 1)
  c <- collect(select(df, alias(from_csv(df$col, lit("a INT")), "csv")))
  expect_equal(c[[1]][[1]]$a, 1)
  c <- collect(select(df, alias(from_csv(df$col, structType("a INT")), "csv")))
  expect_equal(c[[1]][[1]]$a, 1)
  c <- collect(select(df, alias(from_csv(df$col, schema_of_csv("1")), "csv")))
  expect_equal(c[[1]][[1]]$`_c0`, 1)
  c <- collect(select(df, alias(from_csv(df$col, schema_of_csv(lit("1"))), "csv")))
  expect_equal(c[[1]][[1]]$`_c0`, 1)

  df <- as.DataFrame(list(list("col" = "1")))
  c <- collect(select(df, schema_of_csv("Amsterdam,2018")))
  expect_equal(c[[1]], "STRUCT<_c0: STRING, _c1: INT>")
  c <- collect(select(df, schema_of_csv(lit("Amsterdam,2018"))))
  expect_equal(c[[1]], "STRUCT<_c0: STRING, _c1: INT>")

  # Test to_json(), from_json(), schema_of_json()
  df <- sql("SELECT array(named_struct('name', 'Bob'), named_struct('name', 'Alice')) as people")
  j <- collect(select(df, alias(to_json(df$people), "json")))
  expect_equal(j[order(j$json), ][1], "[{\"name\":\"Bob\"},{\"name\":\"Alice\"}]")

  df <- sql("SELECT map('name', 'Bob') as people")
  j <- collect(select(df, alias(to_json(df$people), "json")))
  expect_equal(j[order(j$json), ][1], "{\"name\":\"Bob\"}")

  df <- sql("SELECT array(map('name', 'Bob'), map('name', 'Alice')) as people")
  j <- collect(select(df, alias(to_json(df$people), "json")))
  expect_equal(j[order(j$json), ][1], "[{\"name\":\"Bob\"},{\"name\":\"Alice\"}]")

  df <- read.json(mapTypeJsonPath)
  j <- collect(select(df, alias(to_json(df$info), "json")))
  expect_equal(j[order(j$json), ][1], "{\"age\":16,\"height\":176.5}")
  df <- as.DataFrame(j)
  schemas <- list(structType(structField("age", "integer"), structField("height", "double")),
                  "age INT, height DOUBLE",
                  schema_of_json("{\"age\":16,\"height\":176.5}"),
                  schema_of_json(lit("{\"age\":16,\"height\":176.5}")))
  for (schema in schemas) {
    s <- collect(select(df, alias(from_json(df$json, schema), "structcol")))
    expect_equal(ncol(s), 1)
    expect_equal(nrow(s), 3)
    expect_is(s[[1]][[1]], "struct")
    expect_true(any(apply(s, 1, function(x) { x[[1]]$age == 16 })))
  }

  df <- as.DataFrame(list(list("col" = "1")))
  c <- collect(select(df, schema_of_json('{"name":"Bob"}')))
  expect_equal(c[[1]], "STRUCT<name: STRING>")
  c <- collect(select(df, schema_of_json(lit('{"name":"Bob"}'))))
  expect_equal(c[[1]], "STRUCT<name: STRING>")

  # Test to_json() supports arrays of primitive types and arrays
  df <- sql("SELECT array(19, 42, 70) as age")
  j <- collect(select(df, alias(to_json(df$age), "json")))
  expect_equal(j[order(j$json), ][1], "[19,42,70]")

  df <- sql("SELECT array(array(1, 2), array(3, 4)) as matrix")
  j <- collect(select(df, alias(to_json(df$matrix), "json")))
  expect_equal(j[order(j$json), ][1], "[[1,2],[3,4]]")

  # passing option
  df <- as.DataFrame(list(list("col" = "{\"date\":\"21/10/2014\"}")))
  schema2 <- structType(structField("date", "date"))
  s <- collect(select(df, from_json(df$col, schema2)))
  expect_equal(s[[1]][[1]]$date, NA)
  s <- collect(select(df, from_json(df$col, schema2, dateFormat = "dd/MM/yyyy")))
  expect_is(s[[1]][[1]]$date, "Date")
  expect_equal(as.character(s[[1]][[1]]$date), "2014-10-21")

  # check for unparseable
  df <- as.DataFrame(list(list("a" = "")))
  expect_equal(collect(select(df, from_json(df$a, schema)))[[1]][[1]], NA)

  # check if array type in string is correctly supported.
  jsonArr <- "[{\"name\":\"Bob\"}, {\"name\":\"Alice\"}]"
  df <- as.DataFrame(list(list("people" = jsonArr)))
  schemas <- list(structType(structField("name", "string")),
                  "name STRING",
                  schema_of_json("{\"name\":\"Alice\"}"),
                  schema_of_json(lit("{\"name\":\"Bob\"}")))
  for (schema in schemas) {
    arr <- collect(select(df, alias(from_json(df$people, schema, as.json.array = TRUE), "arrcol")))
    expect_equal(ncol(arr), 1)
    expect_equal(nrow(arr), 1)
    expect_is(arr[[1]][[1]], "list")
    expect_equal(length(arr$arrcol[[1]]), 2)
    expect_equal(arr$arrcol[[1]][[1]]$name, "Bob")
    expect_equal(arr$arrcol[[1]][[2]]$name, "Alice")
  }

  # Test to_csv()
  df <- sql("SELECT named_struct('name', 'Bob') as people")
  j <- collect(select(df, alias(to_csv(df$people), "csv")))
  expect_equal(j[order(j$csv), ][1], "Bob")

  # Test create_array() and create_map()
  df <- as.DataFrame(data.frame(
    x = c(1.0, 2.0), y = c(-1.0, 3.0), z = c(-2.0, 5.0)
  ))

  arrs <- collect(select(df, create_array(df$x, df$y, df$z)))
  expect_equal(arrs[, 1], list(list(1, -1, -2), list(2, 3, 5)))

  maps <- collect(select(
    df, create_map(lit("x"), df$x, lit("y"), df$y, lit("z"), df$z)))

  expect_equal(
    maps[, 1],
    lapply(
      list(list(x = 1, y = -1, z = -2), list(x = 2, y = 3,  z = 5)),
      as.environment))

  df <- as.DataFrame(data.frame(is_true = c(TRUE, FALSE, NA)))
  expect_equal(
    collect(select(df, alias(not(df$is_true), "is_false"))),
    data.frame(is_false = c(FALSE, TRUE, NA))
  )

  # Test percentile_approx
  actual <- lapply(
    list(
      percentile_approx(column("foo"), 0.5),
      percentile_approx(column("bar"), lit(0.25), lit(42L)),
      percentile_approx(column("bar"), c(0.25, 0.5, 0.75)),
      percentile_approx(column("foo"), c(0.05, 0.95), 100L),
      percentile_approx("foo", c(0.5)),
      percentile_approx("bar", c(0.1, 0.9), 10L)),
    function(x) SparkR:::callJMethod(x@jc, "toString"))

  expected <- list(
     "percentile_approx(foo, 0.5, 10000)",
     "percentile_approx(bar, 0.25, 42)",
     "percentile_approx(bar, array(0.25, 0.5, 0.75), 10000)",
     "percentile_approx(foo, array(0.05, 0.95), 100)",
     "percentile_approx(foo, 0.5, 10000)",
     "percentile_approx(bar, array(0.1, 0.9), 10)"
  )

  expect_equal(actual, expected)

  # Test withField
  lines <- c("{\"Person\": {\"name\":\"Bob\", \"age\":24, \"height\": 170}}")
  jsonPath <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(lines, jsonPath)
  df <- read.df(jsonPath, "json")
  result <- collect(
      select(
          select(df, alias(withField(df$Person, "dummy", lit(42)), "Person")),
          "Person.dummy"
      )
  )
  expect_equal(result, data.frame(dummy = 42))

  # Test dropFields
  expect_setequal(
    colnames(select(
      withColumn(df, "Person", dropFields(df$Person, "age")),
      column("Person.*")
    )),
    c("name", "height")
  )

  expect_equal(
    colnames(select(
      withColumn(df, "Person", dropFields(df$Person, "height", "name")),
      column("Person.*")
    )),
    "age"
  )
})

test_that("avro column functions", {
  skip_if_not(
    grepl("spark-avro", sparkR.conf("spark.jars", "")),
    "spark-avro jar not present"
  )

  schema <- '{"namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "favorite_color", "type": ["string", "null"]}
    ]
  }'

  c0 <- column("foo")
  c1 <- from_avro(c0, schema)
  expect_s4_class(c1, "Column")
  c2 <- from_avro("foo", schema)
  expect_s4_class(c2, "Column")
  c3 <- to_avro(c1)
  expect_s4_class(c3, "Column")
  c4 <- to_avro(c1, schema)
  expect_s4_class(c4, "Column")
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
  expect_equal(collect(select(df, shiftleft(df$b, 1)))[4, 1], 16)
  expect_equal(collect(select(df, shiftright(df$b, 1)))[4, 1], 4)
  expect_equal(collect(select(df, shiftrightunsigned(df$b, 1)))[4, 1], 4)
  expect_equal(collect(select(df, suppressWarnings(shiftLeft(df$b, 1))))[4, 1], 16)
  expect_equal(collect(select(df, suppressWarnings(shiftRight(df$b, 1))))[4, 1], 4)
  expect_equal(collect(select(df, suppressWarnings(shiftRightUnsigned(df$b, 1))))[4, 1], 4)
  expect_equal(class(collect(select(df, rand()))[2, 1]), "numeric")
  expect_equal(collect(select(df, rand(1)))[1, 1], 0.636, tolerance = 0.01)
  expect_equal(class(collect(select(df, randn()))[2, 1]), "numeric")
  expect_equal(collect(select(df, randn(1)))[1, 1], 1.68, tolerance = 0.01)
})

test_that("string operators", {
  df <- read.json(jsonPath)
  expect_equal(count(where(df, like(df$name, "A%"))), 1)
  expect_equal(count(where(df, startsWith(df$name, "A"))), 1)
  expect_true(first(select(df, startsWith(df$name, "M")))[[1]])
  expect_false(first(select(df, startsWith(df$name, "m")))[[1]])
  expect_true(first(select(df, endsWith(df$name, "el")))[[1]])
  expect_equal(first(select(df, substr(df$name, 1, 2)))[[1]], "Mi")
  expect_equal(first(select(df, substr(df$name, 4, 6)))[[1]], "hae")
  version <- packageVersion("base")
  if (as.numeric(version$major) >= 3 && as.numeric(version$minor) >= 3) {
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

  l4 <- list(list(a = "a.b@c.d   1\\b"))
  df4 <- createDataFrame(l4)
  expect_equal(
    collect(select(df4, split_string(df4$a, "\\s+")))[1, 1],
    list(list("a.b@c.d", "1\\b"))
  )
  expect_equal(
    collect(select(df4, split_string(df4$a, "\\.")))[1, 1],
    list(list("a", "b@c", "d   1\\b"))
  )
  expect_equal(
    collect(select(df4, split_string(df4$a, "@")))[1, 1],
    list(list("a.b", "c.d   1\\b"))
  )
  expect_equal(
    collect(select(df4, split_string(df4$a, "\\\\")))[1, 1],
    list(list("a.b@c.d   1", "b"))
  )
  expect_equal(
    collect(select(df4, split_string(df4$a, "\\.", 2)))[1, 1],
    list(list("a", "b@c.d   1\\b"))
  )
  expect_equal(
    collect(select(df4, split_string(df4$a, "b", 0)))[1, 1],
    list(list("a.", "@c.d   1\\", ""))
  )

  l5 <- list(list(a = "abc"))
  df5 <- createDataFrame(l5)
  expect_equal(
    collect(select(df5, repeat_string(df5$a, 1L)))[1, 1],
    "abc"
  )
  expect_equal(
    collect(select(df5, repeat_string(df5$a, 3)))[1, 1],
    "abcabcabc"
  )
  expect_equal(
    collect(select(df5, repeat_string(df5$a, -1)))[1, 1],
    ""
  )

  l6 <- list(list("cat"), list("\ud83d\udc08"))
  df6 <- createDataFrame(l6)
  expect_equal(
    collect(select(df6, octet_length(df6$"_1")))[, 1],
    c(3, 4)
  )
  expect_equal(
    collect(select(df6, bit_length(df6$"_1")))[, 1],
    c(24, 32)
  )
})

test_that("date functions on a DataFrame", {
  .originalTimeZone <- Sys.getenv("TZ")
  Sys.setenv(TZ = "UTC")
  l <- list(list(a = 1L, b = as.Date("2012-12-13")),
            list(a = 2L, b = as.Date("2013-12-14")),
            list(a = 3L, b = as.Date("2014-12-15")))
  df <- createDataFrame(l)
  expect_equal(collect(select(df, dayofweek(df$b)))[, 1], c(5, 7, 2))
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
               c(as.POSIXct("2012-12-13 21:34:00 UTC"), as.POSIXct("2014-12-15 10:24:34 UTC")))
  expect_equal(collect(select(df2, to_utc_timestamp(df2$b, "JST")))[, 1],
               c(as.POSIXct("2012-12-13 03:34:00 UTC"), as.POSIXct("2014-12-14 16:24:34 UTC")))
  expect_gt(collect(select(df2, unix_timestamp()))[1, 1], 0)
  expect_gt(collect(select(df2, unix_timestamp(df2$b)))[1, 1], 0)
  expect_gt(collect(select(df2, unix_timestamp(lit("2015-01-01"), "yyyy-MM-dd")))[1, 1], 0)
  expect_equal(collect(select(df2, month(date_trunc("yyyy", df2$b))))[, 1], c(1, 1))

  l3 <- list(list(a = 1000), list(a = -1000))
  df3 <- createDataFrame(l3)
  result31 <- collect(select(df3, from_unixtime(df3$a)))
  expect_equal(grep("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", result31[, 1], perl = TRUE),
               c(1, 2))
  result32 <- collect(select(df3, from_unixtime(df3$a, "yyyy")))
  expect_equal(grep("\\d{4}", result32[, 1]), c(1, 2))
  Sys.setenv(TZ = .originalTimeZone)
})

test_that("SPARK-37108: expose make_date expression in R", {
  ansiEnabled <- sparkR.conf("spark.sql.ansi.enabled")[[1]] == "true"
  df <- createDataFrame(
    c(
      list(list(2021, 10, 22), list(2020, 2, 29)),
      if (ansiEnabled) list() else list(list(2021, 13, 1), list(2021, 2, 29))
    ),
    list("year", "month", "day")
  )
  expect <- createDataFrame(
    c(
      list(list(as.Date("2021-10-22")), list(as.Date("2020-02-29"))),
      if (ansiEnabled) list() else list(NA, NA)
    ),
    list("make_date(year, month, day)")
  )
  actual <- select(df, make_date(df$year, df$month, df$day))
  expect_equal(collect(expect), collect(actual))
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

test_that("higher order functions", {
  df <- select(
    createDataFrame(data.frame(id = 1)),
    expr("CAST(array(1.0, 2.0, -3.0, -4.0) AS array<double>) xs"),
    expr("CAST(array(0.0, 3.0, 48.0) AS array<double>) ys"),
    expr("array('FAILED', 'SUCCEEDED') as vs"),
    expr("map('foo', 1, 'bar', 2) as mx"),
    expr("map('foo', 42, 'bar', -1, 'baz', 0) as my")
  )

  map_to_sorted_array <- function(x) {
    sort_array(arrays_zip(map_keys(x), map_values(x)))
  }

  result <- collect(select(
    df,
    array_transform("xs", function(x) x + 1) == expr("transform(xs, x -> x + 1)"),
    array_transform("xs", function(x, i) otherwise(when(i %% 2 == 0, x), -x)) ==
      expr("transform(xs, (x, i) -> CASE WHEN ((i % 2.0) = 0.0) THEN x ELSE (- x) END)"),
    array_exists("vs", function(v) rlike(v, "FAILED")) ==
      expr("exists(vs, v -> (v RLIKE 'FAILED'))"),
    array_exists("vs", function(v) ilike(v, "failed")) ==
      expr("exists(vs, v -> (v ILIKE 'failed'))"),
    array_forall("xs", function(x) x > 0) ==
      expr("forall(xs, x -> x > 0)"),
    array_filter("xs", function(x, i) x > 0 | i %% 2 == 0) ==
      expr("filter(xs, (x, i) ->  x > 0 OR i % 2 == 0)"),
    array_filter("xs", function(x) signum(x) > 0) ==
      expr("filter(xs, x -> signum(x) > 0)"),
    array_aggregate("xs", lit(0.0), function(x, y) otherwise(when(x > y, x), y)) ==
      expr("aggregate(xs, CAST(0.0 AS double), (x, y) -> CASE WHEN x > y THEN x ELSE y END)"),
    array_aggregate(
      "xs",
      struct(
        alias(lit(0.0), "count"),
        alias(lit(0.0), "sum")
      ),
      function(acc, x) {
        count <- getItem(acc, "count")
        sum <- getItem(acc, "sum")
        struct(alias(count + 1.0, "count"), alias(sum + x, "sum"))
      },
      function(acc) getItem(acc, "sum") / getItem(acc, "count")
    ) == expr(paste0(
      "aggregate(xs, struct(CAST(0.0 AS double) count, CAST(0.0 AS double) sum), ",
      "(acc, x) -> ",
      "struct(cast(acc.count + 1.0 AS double) count, CAST(acc.sum + x AS double) sum), ",
      "acc -> acc.sum / acc.count)"
    )),
    arrays_zip_with("xs", "ys", function(x, y) x + y) ==
      expr("zip_with(xs, ys, (x, y) -> x + y)"),
    map_to_sorted_array(transform_keys("mx", function(k, v) upper(k))) ==
      map_to_sorted_array(expr("transform_keys(mx, (k, v) -> upper(k))")),
    map_to_sorted_array(transform_values("mx", function(k, v) v * 2)) ==
      map_to_sorted_array(expr("transform_values(mx, (k, v) -> v * 2)")),
    map_to_sorted_array(map_filter(column("my"), function(k, v) lower(v) != "foo")) ==
      map_to_sorted_array(expr("map_filter(my, (k, v) -> lower(v) != 'foo')")),
    map_to_sorted_array(map_zip_with("mx", "my", function(k, vx, vy) vx * vy)) ==
      map_to_sorted_array(expr("map_zip_with(mx, my, (k, vx, vy) -> vx * vy)"))
  ))

  expect_true(all(unlist(result)))

  expect_error(array_transform("xs", function(...) 42))
})

test_that("SPARK-34794: lambda vars must be resolved properly in nested higher order functions", {
  df <- sql("SELECT array(1, 2, 3) as numbers, array('a', 'b', 'c') as letters")
  ret <- first(select(
    df,
    array_transform("numbers", function(number) {
      array_transform("letters", function(latter) {
        struct(alias(number, "n"), alias(latter, "l"))
      })
    })
  ))

  expect_equal(1, ret[[1]][[1]][[1]][[1]]$n)
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
  expect_true(is.na(df3_local[df3_local$name == "Andy", ][1, 2]))

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
  expect_true(is.na(df7_local[df7_local$name == "ID2", ][1, 2]))

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

  # Test collect_list and collect_set
  gd3_collections_local <- collect(
    agg(gd3, collect_set(df8$age), collect_list(df8$age))
  )

  expect_equal(
    unlist(gd3_collections_local[gd3_collections_local$name == "Andy", 2]),
    c(30)
  )

  expect_equal(
    unlist(gd3_collections_local[gd3_collections_local$name == "Andy", 3]),
    c(30, 30)
  )

  expect_equal(
    sort(unlist(
      gd3_collections_local[gd3_collections_local$name == "Justin", 3]
    )),
    c(1, 19)
  )

  unlink(jsonPath2)
  unlink(jsonPath3)
})

test_that("SPARK-36976: Add max_by/min_by API to SparkR", {
  df <- createDataFrame(
    list(list("Java", 2012, 20000), list("dotNET", 2012, 5000),
         list("dotNET", 2013, 48000), list("Java", 2013, 30000))
  )
  gd <- groupBy(df, df$"_1")

  actual1 <- agg(gd, "_2" = max_by(df$"_2", df$"_3"))
  expect1 <- createDataFrame(list(list("dotNET", 2013), list("Java", 2013)))
  expect_equal(collect(actual1), collect(expect1))

  actual2 <- agg(gd, "_2" = min_by(df$"_2", df$"_3"))
  expect2 <- createDataFrame(list(list("dotNET", 2012), list("Java", 2012)))
  expect_equal(collect(actual2), collect(expect2))
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

test_that("test multi-dimensional aggregations with cube and rollup", {
  df <- createDataFrame(data.frame(
    id = 1:6,
    year = c(2016, 2016, 2016, 2017, 2017, 2017),
    salary = c(10000, 15000, 20000, 22000, 32000, 21000),
    department = c("management", "rnd", "sales", "management", "rnd", "sales")
  ))

  actual_cube <- collect(
    orderBy(
      agg(
        cube(df, "year", "department"),
        expr("sum(salary) AS total_salary"),
        expr("avg(salary) AS average_salary"),
        alias(grouping_bit(df$year), "grouping_year"),
        alias(grouping_bit(df$department), "grouping_department"),
        alias(grouping_id(df$year, df$department), "grouping_id")
      ),
      "year", "department"
    )
  )

  expected_cube <- data.frame(
    year = c(rep(NA, 4), rep(2016, 4), rep(2017, 4)),
    department = rep(c(NA, "management", "rnd", "sales"), times = 3),
    total_salary = c(
      120000, # Total
      10000 + 22000, 15000 + 32000, 20000 + 21000, # Department only
      20000 + 15000 + 10000, # 2016
      10000, 15000, 20000, # 2016 each department
      21000 + 32000 + 22000, # 2017
      22000, 32000, 21000 # 2017 each department
    ),
    average_salary = c(
      # Total
      mean(c(20000, 15000, 10000, 21000, 32000, 22000)),
      # Mean by department
      mean(c(10000, 22000)), mean(c(15000, 32000)), mean(c(20000, 21000)),
      mean(c(10000, 15000, 20000)), # 2016
      10000, 15000, 20000, # 2016 each department
      mean(c(21000, 32000, 22000)), # 2017
      22000, 32000, 21000 # 2017 each department
    ),
    grouping_year = c(
      1, # global
      1, 1, 1, # by department
      0, # 2016
      0, 0, 0, # 2016 by department
      0, # 2017
      0, 0, 0 # 2017 by department
    ),
    grouping_department = c(
      1, # global
      0, 0, 0, # by department
      1, # 2016
      0, 0, 0, # 2016 by department
      1, # 2017
      0, 0, 0 # 2017 by department
    ),
    grouping_id = c(
      3, #  11
      2, 2, 2, # 10
      1, # 01
      0, 0, 0, # 00
      1, # 01
      0, 0, 0 # 00
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(actual_cube, expected_cube)

  # cube should accept column objects
  expect_equal(
    count(sum(cube(df, df$year, df$department), "salary")),
    12
  )

  # cube without columns should result in a single aggregate
  expect_equal(
    collect(agg(cube(df), expr("sum(salary) as total_salary"))),
    data.frame(total_salary = 120000)
  )

  actual_rollup <- collect(
    orderBy(
      agg(
        rollup(df, "year", "department"),
        expr("sum(salary) AS total_salary"), expr("avg(salary) AS average_salary"),
        alias(grouping_bit(df$year), "grouping_year"),
        alias(grouping_bit(df$department), "grouping_department"),
        alias(grouping_id(df$year, df$department), "grouping_id")
      ),
      "year", "department"
    )
  )

  expected_rollup <- data.frame(
    year = c(NA, rep(2016, 4), rep(2017, 4)),
    department = c(NA, rep(c(NA, "management", "rnd", "sales"), times = 2)),
    total_salary = c(
      120000, # Total
      20000 + 15000 + 10000, # 2016
      10000, 15000, 20000, # 2016 each department
      21000 + 32000 + 22000, # 2017
      22000, 32000, 21000 # 2017 each department
    ),
    average_salary = c(
      # Total
      mean(c(20000, 15000, 10000, 21000, 32000, 22000)),
      mean(c(10000, 15000, 20000)), # 2016
      10000, 15000, 20000, # 2016 each department
      mean(c(21000, 32000, 22000)), # 2017
      22000, 32000, 21000 # 2017 each department
    ),
    grouping_year = c(
      1, # global
      0, # 2016
      0, 0, 0, # 2016 each department
      0, # 2017
      0, 0, 0 # 2017 each department
    ),
    grouping_department = c(
      1, # global
      1, # 2016
      0, 0, 0, # 2016 each department
      1, # 2017
      0, 0, 0 # 2017 each department
    ),
    grouping_id = c(
      3, # 11
      1, # 01
      0, 0, 0, # 00
      1, # 01
      0, 0, 0 # 00
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(actual_rollup, expected_rollup)

  # cube should accept column objects
  expect_equal(
    count(sum(rollup(df, df$year, df$department), "salary")),
    9
  )

  # rollup without columns should result in a single aggregate
  expect_equal(
    collect(agg(rollup(df), expr("sum(salary) as total_salary"))),
    data.frame(total_salary = 120000)
  )
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

  df <- createDataFrame(cars, numPartitions = 10)
  expect_equal(getNumPartitions(df), 10)
  sorted8 <- arrange(df, "dist", withinPartitions = TRUE)
  expect_equal(collect(sorted8)[5:6, "dist"], c(22, 10))
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

  # test suites for %<=>%
  dfNa <- read.json(jsonPathNa)
  expect_equal(count(filter(dfNa, dfNa$age %<=>% 60)), 1)
  expect_equal(count(filter(dfNa, !(dfNa$age %<=>% 60))), 5 - 1)
  expect_equal(count(filter(dfNa, dfNa$age %<=>% NULL)), 3)
  expect_equal(count(filter(dfNa, !(dfNa$age %<=>% NULL))), 5 - 3)
  # match NA from two columns
  expect_equal(count(filter(dfNa, dfNa$age %<=>% dfNa$height)), 2)
  expect_equal(count(filter(dfNa, !(dfNa$age %<=>% dfNa$height))), 5 - 2)

  # Test stats::filter is working
  #expect_true(is.ts(filter(1:100, rep(1, 3)))) # nolint
})

test_that("join(), crossJoin() and merge() on a DataFrame", {
  df <- read.json(jsonPath)

  mockLines2 <- c("{\"name\":\"Michael\", \"test\": \"yes\"}",
                  "{\"name\":\"Andy\",  \"test\": \"no\"}",
                  "{\"name\":\"Justin\", \"test\": \"yes\"}",
                  "{\"name\":\"Bob\", \"test\": \"yes\"}")
  jsonPath2 <- tempfile(pattern = "sparkr-test", fileext = ".tmp")
  writeLines(mockLines2, jsonPath2)
  df2 <- read.json(jsonPath2)

  # inner join, not cartesian join
  expect_equal(count(where(join(df, df2), df$name == df2$name)), 3)

  conf <- callJMethod(sparkSession, "conf")
  crossJoinEnabled <- callJMethod(conf, "get", "spark.sql.crossJoin.enabled")
  callJMethod(conf, "set", "spark.sql.crossJoin.enabled", "false")
  tryCatch({
    # cartesian join
    expect_error(tryCatch(count(join(df, df2)), error = function(e) { stop(e) }),
                 paste0(".*(org.apache.spark.sql.AnalysisException: Detected implicit cartesian",
                        " product for INNER join between logical plans).*"))
  },
  finally = {
    # Resetting the conf back to default value
    callJMethod(conf, "set", "spark.sql.crossJoin.enabled", crossJoinEnabled)
  })

  joined <- crossJoin(df, df2)
  expect_equal(names(joined), c("age", "name", "name", "test"))
  expect_equal(count(joined), 12)
  expect_equal(names(collect(joined)), c("age", "name", "name", "test"))

  joined2 <- join(df, df2, df$name == df2$name)
  expect_equal(names(joined2), c("age", "name", "name", "test"))
  expect_equal(count(joined2), 3)

  joined3 <- join(df, df2, df$name == df2$name, "right")
  expect_equal(names(joined3), c("age", "name", "name", "test"))
  expect_equal(count(joined3), 4)
  expect_true(is.na(collect(orderBy(joined3, joined3$age))$age[2]))

  joined4 <- join(df, df2, df$name == df2$name, "right_outer")
  expect_equal(names(joined4), c("age", "name", "name", "test"))
  expect_equal(count(joined4), 4)
  expect_true(is.na(collect(orderBy(joined4, joined4$age))$age[2]))

  joined5 <- join(df, df2, df$name == df2$name, "rightouter")
  expect_equal(names(joined5), c("age", "name", "name", "test"))
  expect_equal(count(joined5), 4)
  expect_true(is.na(collect(orderBy(joined5, joined5$age))$age[2]))


  joined6 <- select(join(df, df2, df$name == df2$name, "outer"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined6), c("newAge", "name", "test"))
  expect_equal(count(joined6), 4)
  expect_equal(collect(orderBy(joined6, joined6$name))$newAge[3], 24)

  joined7 <- select(join(df, df2, df$name == df2$name, "full"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined7), c("newAge", "name", "test"))
  expect_equal(count(joined7), 4)
  expect_equal(collect(orderBy(joined7, joined7$name))$newAge[3], 24)

  joined8 <- select(join(df, df2, df$name == df2$name, "fullouter"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined8), c("newAge", "name", "test"))
  expect_equal(count(joined8), 4)
  expect_equal(collect(orderBy(joined8, joined8$name))$newAge[3], 24)

  joined9 <- select(join(df, df2, df$name == df2$name, "full_outer"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined9), c("newAge", "name", "test"))
  expect_equal(count(joined9), 4)
  expect_equal(collect(orderBy(joined9, joined9$name))$newAge[3], 24)

  joined10 <- join(df, df2, df$name == df2$name, "left")
  expect_equal(names(joined10), c("age", "name", "name", "test"))
  expect_equal(count(joined10), 3)
  expect_true(is.na(collect(orderBy(joined10, joined10$age))$age[1]))

  joined11 <- join(df, df2, df$name == df2$name, "leftouter")
  expect_equal(names(joined11), c("age", "name", "name", "test"))
  expect_equal(count(joined11), 3)
  expect_true(is.na(collect(orderBy(joined11, joined11$age))$age[1]))

  joined12 <- join(df, df2, df$name == df2$name, "left_outer")
  expect_equal(names(joined12), c("age", "name", "name", "test"))
  expect_equal(count(joined12), 3)
  expect_true(is.na(collect(orderBy(joined12, joined12$age))$age[1]))

  joined13 <- join(df, df2, df$name == df2$name, "inner")
  expect_equal(names(joined13), c("age", "name", "name", "test"))
  expect_equal(count(joined13), 3)

  joined14 <- join(df, df2, df$name == df2$name, "semi")
  expect_equal(names(joined14), c("age", "name"))
  expect_equal(count(joined14), 3)

  joined14 <- join(df, df2, df$name == df2$name, "leftsemi")
  expect_equal(names(joined14), c("age", "name"))
  expect_equal(count(joined14), 3)

  joined15 <- join(df, df2, df$name == df2$name, "left_semi")
  expect_equal(names(joined15), c("age", "name"))
  expect_equal(count(joined15), 3)

  joined16 <- join(df2, df, df2$name == df$name, "anti")
  expect_equal(names(joined16), c("name", "test"))
  expect_equal(count(joined16), 1)

  joined17 <- join(df2, df, df2$name == df$name, "leftanti")
  expect_equal(names(joined17), c("name", "test"))
  expect_equal(count(joined17), 1)

  joined18 <- join(df2, df, df2$name == df$name, "left_anti")
  expect_equal(names(joined18), c("name", "test"))
  expect_equal(count(joined18), 1)

  error_msg <- paste("joinType must be one of the following types:",
                 "'inner', 'cross', 'outer', 'full', 'fullouter', 'full_outer',",
                 "'left', 'leftouter', 'left_outer', 'right', 'rightouter', 'right_outer',",
                 "'semi', 'leftsemi', 'left_semi', 'anti', 'leftanti', 'left_anti'")
  expect_error(join(df2, df, df2$name == df$name, "invalid"), error_msg)

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
               paste0("The following column name: name_y occurs more than once in the 'DataFrame'.",
                      "Please use different suffixes for the intersected columns."))

  unlink(jsonPath2)
  unlink(jsonPath3)

  # Join with broadcast hint
  df1 <- sql("SELECT * FROM range(10e10)")
  df2 <- sql("SELECT * FROM range(10e10)")

  execution_plan <- capture.output(explain(join(df1, df2, df1$id == df2$id)))
  expect_false(any(grepl("BroadcastHashJoin", execution_plan)))

  execution_plan_hint <- capture.output(
    explain(join(df1, hint(df2, "broadcast"), df1$id == df2$id))
  )
  expect_true(any(grepl("BroadcastHashJoin", execution_plan_hint)))

  execution_plan_broadcast <- capture.output(
    explain(join(df1, broadcast(df2), df1$id == df2$id))
  )
  expect_true(any(grepl("BroadcastHashJoin", execution_plan_broadcast)))
})

test_that("test hint", {
  df <- sql("SELECT * FROM range(10e10)")
  hintList <- list("hint2", "hint3", "hint4")
  execution_plan_hint <- capture.output(
    explain(hint(df, "hint1", 1.23456, "aaaaaaaaaa", hintList), TRUE)
  )
  expect_true(any(grepl("1.23456, aaaaaaaaaa", execution_plan_hint)))
})

test_that("toJSON() on DataFrame", {
  df <- as.DataFrame(cars)
  df_json <- toJSON(df)
  expect_is(df_json, "SparkDataFrame")
  expect_equal(colnames(df_json), c("value"))
  expect_equal(head(df_json, 1),
              data.frame(value = "{\"speed\":4.0,\"dist\":2.0}", stringsAsFactors = FALSE))
})

test_that("showDF()", {
  df <- read.json(jsonPath)
  expected <- paste("+----+-------+",
                    "| age|   name|",
                    "+----+-------+",
                    "|null|Michael|",
                    "|  30|   Andy|",
                    "|  19| Justin|",
                    "+----+-------+\n", sep = "\n")
  expected2 <- paste("+---+----+",
                     "|age|name|",
                     "+---+----+",
                     "|nul| Mic|",
                     "| 30| And|",
                     "| 19| Jus|",
                     "+---+----+\n", sep = "\n")
  expect_output(showDF(df), expected)
  expect_output(showDF(df, truncate = 3), expected2)
})

test_that("isLocal()", {
  df <- read.json(jsonPath)
  expect_false(isLocal(df))
})

test_that("union(), unionByName(), rbind(), except(), and intersect() on a DataFrame", {
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
  expect_equal(count(arrange(suppressWarnings(union(df, df2)), df$age)), 6)
  expect_equal(count(arrange(suppressWarnings(unionAll(df, df2)), df$age)), 6)

  df1 <- select(df2, "age", "name")
  unioned1 <- arrange(unionByName(df1, df), df1$age)
  expect_is(unioned, "SparkDataFrame")
  expect_equal(count(unioned), 6)
  # Here, we test if 'Michael' in df is correctly mapped to the same name.
  expect_equal(first(unioned)$name, "Michael")

  unioned2 <- arrange(rbind(unioned, df, df2), df$age)
  expect_is(unioned2, "SparkDataFrame")
  expect_equal(count(unioned2), 12)
  expect_equal(first(unioned2)$name, "Michael")

  df3 <- df2
  names(df3)[1] <- "newName"
  expect_error(rbind(df, df3),
               "Names of input data frames are different.")
  expect_error(rbind(df, df2, df3),
               "Names of input data frames are different.")


  df4 <- unionByName(df2, select(df2, "age"), TRUE)

  expect_equal(
      sum(collect(
          select(df4, alias(isNull(df4$name), "missing_name")
      ))$missing_name),
      3
  )

  testthat::expect_error(unionByName(df2, select(df2, "age"), FALSE))
  testthat::expect_error(unionByName(df2, select(df2, "age")))

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

test_that("intersectAll() and exceptAll()", {
  df1 <- createDataFrame(list(list("a", 1), list("a", 1), list("a", 1),
                              list("a", 1), list("b", 3), list("c", 4)),
                         schema = c("a", "b"))
  df2 <- createDataFrame(list(list("a", 1), list("a", 1), list("b", 3)), schema = c("a", "b"))
  intersectAllExpected <- data.frame("a" = c("a", "a", "b"), "b" = c(1, 1, 3),
                                       stringsAsFactors = FALSE)
  exceptAllExpected <- data.frame("a" = c("a", "a", "c"), "b" = c(1, 1, 4),
                                    stringsAsFactors = FALSE)
  intersectAllDf <- arrange(intersectAll(df1, df2), df1$a)
  expect_is(intersectAllDf, "SparkDataFrame")
  exceptAllDf <- arrange(exceptAll(df1, df2), df1$a)
  expect_is(exceptAllDf, "SparkDataFrame")
  intersectAllActual <- collect(intersectAllDf)
  expect_identical(intersectAllActual, intersectAllExpected)
  exceptAllActual <- collect(exceptAllDf)
  expect_identical(exceptAllActual, exceptAllExpected)
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

  newDF <- withColumn(df, "age", 18)
  expect_equal(length(columns(newDF)), 2)
  expect_equal(first(newDF)$age, 18)

  expect_error(withColumn(df, "age", list("a")),
              "Literal value must be atomic in length of 1")

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

  # ensure long inferred names are handled without error (SPARK-26199)
  #   test implicitly assumes eval(formals(deparse)$width.cutoff) = 60
  #   (which has always been true as of 2020-11-15)
  newDF <- mutate(
    df,
    df$age + 12345678901234567890 + 12345678901234567890 + 12345678901234
  )
  expect_match(tail(columns(newDF), 1L), "234567890", fixed = TRUE)
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
  if (windows_with_hadoop()) {
    df <- read.df(jsonPath, "json")
    # Test write.df and read.df
    write.df(df, parquetPath, "parquet", mode = "overwrite")
    df2 <- read.df(parquetPath, "parquet")
    expect_is(df2, "SparkDataFrame")
    expect_equal(count(df2), 3)

    # Test write.parquet and read.parquet
    parquetPath2 <- tempfile(pattern = "parquetPath2", fileext = ".parquet")
    write.parquet(df, parquetPath2)
    parquetPath3 <- tempfile(pattern = "parquetPath3", fileext = ".parquet")
    write.parquet(df, parquetPath3)
    parquetDF <- read.parquet(c(parquetPath2, parquetPath3))
    expect_is(parquetDF, "SparkDataFrame")
    expect_equal(count(parquetDF), count(df) * 2)

    # Test if varargs works with variables
    saveMode <- "overwrite"
    mergeSchema <- "true"
    parquetPath4 <- tempfile(pattern = "parquetPath3", fileext = ".parquet")
    write.df(df, parquetPath3, "parquet", mode = saveMode, mergeSchema = mergeSchema)

    unlink(parquetPath2)
    unlink(parquetPath3)
    unlink(parquetPath4)
  }
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

  df3 <- createDataFrame(list(list(1L, "1"), list(2L, "2"), list(1L, "1"), list(2L, "2")),
                         schema = c("key", "value"))
  textPath3 <- tempfile(pattern = "textPath3", fileext = ".txt")
  write.df(df3, textPath3, "text", mode = "overwrite", partitionBy = "key")
  df4 <- read.df(textPath3, "text")
  expect_equal(count(df3), count(df4))

  unlink(textPath)
  unlink(textPath2)
  unlink(textPath3)
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

test_that("describe() and summary() on a DataFrame", {
  df <- read.json(jsonPath)
  stats <- describe(df, "age")
  expect_equal(collect(stats)[1, "summary"], "count")
  expect_equal(collect(stats)[2, "age"], "24.5")
  expect_equal(collect(stats)[3, "age"], "7.7781745930520225")
  stats <- describe(df)
  expect_equal(collect(stats)[4, "summary"], "min")
  expect_equal(collect(stats)[5, "age"], "30")

  stats2 <- summary(df)
  expect_equal(collect(stats2)[5, "summary"], "25%")
  expect_equal(collect(stats2)[5, "age"], "19")

  stats3 <- summary(df, "min", "max", "55.1%")

  expect_equal(collect(stats3)[1, "summary"], "min")
  expect_equal(collect(stats3)[2, "summary"], "max")
  expect_equal(collect(stats3)[3, "summary"], "55.1%")
  expect_equal(collect(stats3)[3, "age"], "30")

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
  rdf[input %% 3 == 0, ] <- c(1, "1", -1)
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
  expect_identical(as.list(result[2, ]), list(key = "1", count = 8))
})

test_that("approxQuantile() on a DataFrame", {
  l <- lapply(c(0:100), function(i) { list(i, 100 - i) })
  df <- createDataFrame(l, list("a", "b"))
  quantiles <- approxQuantile(df, "a", c(0.5, 0.8), 0.0)
  expect_equal(quantiles, list(50, 80))
  quantiles2 <- approxQuantile(df, c("a", "b"), c(0.5, 0.8), 0.0)
  expect_equal(quantiles2[[1]], list(50, 80))
  expect_equal(quantiles2[[2]], list(50, 80))

  dfWithNA <- createDataFrame(data.frame(a = c(NA, 30, 19, 11, 28, 15),
                                         b = c(-30, -19, NA, -11, -28, -15)))
  quantiles3 <- approxQuantile(dfWithNA, c("a", "b"), c(0.5), 0.0)
  expect_equal(quantiles3[[1]], list(19))
  expect_equal(quantiles3[[2]], list(-19))
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
  expect_equal(collect(stat)[8, "age"], "30")
  age <- age$age + 1
  expect_is(age, "Column")
  rm(age)
  stat2 <- summary(age)
  expect_equal(collect(stat2)[8, "age"], "30")
  detach("df")
  stat3 <- summary(df[, "age", drop = F])
  expect_equal(collect(stat3)[8, "age"], "30")
  expect_error(age)

  # attach method uses deparse(); ensure no errors from a very long input
  abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop <- df # nolint
  attach(abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop)
  expect_true(any(grepl("abcdefghijklmnopqrstuvwxyz", search())))
  detach("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop")
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

  dropTempView("dfView")
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

  dropTempView("irisView")
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

test_that("dapply() should show error message from R worker", {
  df <- createDataFrame(list(list(n = 1)))
  expect_error({
    collect(dapply(df, function(x) stop("custom error message"), structType("a double")))
  }, "custom error message")
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
  schemas <- list(structType(structField("a", "integer"), structField("b", "double"),
                             structField("c", "string"), structField("d", "integer")),
                  "a INT, b DOUBLE, c STRING, d INT")
  for (schema in schemas) {
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
  }

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
  # The tasks here launch R workers with shuffles. So, we decrease the number of shuffle
  # partitions to reduce the number of the tasks to speed up the test. This is particularly
  # slow on Windows because the R workers are unable to be forked. See also SPARK-21693.
  conf <- callJMethod(sparkSession, "conf")
  shufflepartitionsvalue <- callJMethod(conf, "get", "spark.sql.shuffle.partitions")
  callJMethod(conf, "set", "spark.sql.shuffle.partitions", "5")
  coalesceEnabled <- callJMethod(conf, "get", "spark.sql.adaptive.coalescePartitions.enabled")
  callJMethod(conf, "set", "spark.sql.adaptive.coalescePartitions.enabled", "false")
  tryCatch({
    df <- createDataFrame(
      list(list(1L, 1, "1", 0.1), list(1L, 2, "2", 0.2), list(3L, 3, "3", 0.3)),
      c("a", "b", "c", "d"))

    # no column and number of partitions specified
    retError <- tryCatch(repartition(df), error = function(e) e)
    expect_equal(grepl
      ("Please, specify the number of partitions and/or a column\\(s\\)", retError), TRUE)

    # repartition by column and number of partitions
    actual <- repartition(df, 3, col = df$"a")

    # Checking that at least the dimensions are identical
    expect_identical(dim(df), dim(actual))
    expect_equal(getNumPartitions(actual), 3L)

    # repartition by number of partitions
    actual <- repartition(df, 13L)
    expect_identical(dim(df), dim(actual))
    expect_equal(getNumPartitions(actual), 13L)

    expect_equal(getNumPartitions(coalesce(actual, 1L)), 1L)

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
  },
  finally = {
    # Resetting the conf back to default value
    callJMethod(conf, "set", "spark.sql.shuffle.partitions", shufflepartitionsvalue)
    callJMethod(conf, "set", "spark.sql.adaptive.coalescePartitions.enabled", coalesceEnabled)
  })
})

test_that("repartitionByRange on a DataFrame", {
  # The tasks here launch R workers with shuffles. So, we decrease the number of shuffle
  # partitions to reduce the number of the tasks to speed up the test. This is particularly
  # slow on Windows because the R workers are unable to be forked. See also SPARK-21693.
  conf <- callJMethod(sparkSession, "conf")
  shufflepartitionsvalue <- callJMethod(conf, "get", "spark.sql.shuffle.partitions")
  callJMethod(conf, "set", "spark.sql.shuffle.partitions", "5")
  tryCatch({
    df <- createDataFrame(mtcars)
    expect_error(repartitionByRange(df, "haha", df$mpg),
                 "numPartitions and col must be numeric and Column.*")
    expect_error(repartitionByRange(df),
                 ".*specify a column.*or the number of partitions with a column.*")
    expect_error(repartitionByRange(df, col = "haha"),
                 "col must be Column; however, got.*")
    expect_error(repartitionByRange(df, 3),
                 "At least one partition-by column must be specified.")

    # The order of rows should be different with a normal repartition.
    actual <- repartitionByRange(df, 3, df$mpg)
    expect_equal(getNumPartitions(actual), 3)
    expect_false(identical(collect(actual), collect(repartition(df, 3, df$mpg))))

    actual <- repartitionByRange(df, col = df$mpg)
    expect_false(identical(collect(actual), collect(repartition(df, col = df$mpg))))

    # They should have same data.
    actual <- collect(repartitionByRange(df, 3, df$mpg))
    actual <- actual[order(actual$mpg), ]
    expected <- collect(repartition(df, 3, df$mpg))
    expected <- expected[order(expected$mpg), ]
    expect_true(all(actual == expected))

    actual <- collect(repartitionByRange(df, col = df$mpg))
    actual <- actual[order(actual$mpg), ]
    expected <- collect(repartition(df, col = df$mpg))
    expected <- expected[order(expected$mpg), ]
    expect_true(all(actual == expected))
  },
  finally = {
    # Resetting the conf back to default value
    callJMethod(conf, "set", "spark.sql.shuffle.partitions", shufflepartitionsvalue)
  })
})

test_that("coalesce, repartition, numPartitions", {
  df <- as.DataFrame(cars, numPartitions = 5)
  expect_equal(getNumPartitions(df), 5)
  expect_equal(getNumPartitions(coalesce(df, 3)), 3)
  expect_equal(getNumPartitions(coalesce(df, 6)), 5)

  df1 <- coalesce(df, 3)
  expect_equal(getNumPartitions(df1), 3)
  expect_equal(getNumPartitions(coalesce(df1, 6)), 5)
  expect_equal(getNumPartitions(coalesce(df1, 4)), 4)
  expect_equal(getNumPartitions(coalesce(df1, 2)), 2)

  df2 <- repartition(df1, 10)
  expect_equal(getNumPartitions(df2), 10)
  expect_equal(getNumPartitions(coalesce(df2, 13)), 10)
  expect_equal(getNumPartitions(coalesce(df2, 7)), 7)
  expect_equal(getNumPartitions(coalesce(df2, 3)), 3)
})

test_that("gapply() and gapplyCollect() on a DataFrame", {
  # The tasks here launch R workers with shuffles. So, we decrease the number of shuffle
  # partitions to reduce the number of the tasks to speed up the test. This is particularly
  # slow on Windows because the R workers are unable to be forked. See also SPARK-21693.
  conf <- callJMethod(sparkSession, "conf")
  shufflepartitionsvalue <- callJMethod(conf, "get", "spark.sql.shuffle.partitions")
  # TODO: Lower number of 'spark.sql.shuffle.partitions' causes test failures
  # for an unknown reason. Probably we should fix it.
  callJMethod(conf, "set", "spark.sql.shuffle.partitions", "16")
  tryCatch({
    df <- createDataFrame(
      list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
      c("a", "b", "c", "d"))
    expected <- collect(df)
    df1 <- gapply(df, "a", function(key, x) { x }, schema(df))
    actual <- collect(df1)
    expect_identical(actual, expected)

    df1Collect <- gapplyCollect(df, list("a"), function(key, x) { x })
    expect_identical(df1Collect, expected)

    # gapply on empty grouping columns.
    df1 <- gapply(df, c(), function(key, x) { x }, schema(df))
    actual <- collect(df1)
    expect_identical(actual, expected)

    # Computes the sum of second column by grouping on the first and third columns
    # and checks if the sum is larger than 2
    schemas <- list(structType(structField("a", "integer"), structField("e", "boolean")),
                    "a INT, e BOOLEAN")
    for (schema in schemas) {
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
    }

    # Computes the arithmetic mean of the second column by grouping
    # on the first and third columns. Output the grouping value and the average.
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
    actual <- actual[order(actual$a), ]
    rownames(actual) <- NULL
    expected <- collect(select(df, "a", "b", "c"))
    expected <- data.frame(aggregate(expected$b, by = list(expected$a, expected$c), FUN = mean))
    colnames(expected) <- c("a", "c", "avg")
    expected <- expected[order(expected$a), ]
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

    irisDF <- suppressWarnings(createDataFrame(iris))
    schema <- structType(structField("Sepal_Length", "double"), structField("Avg", "double"))
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
    agg_local_df <- data.frame(aggregate(iris$Sepal.Width,
                                         by = list(iris$Sepal.Length),
                                         FUN = mean),
                               stringsAsFactors = FALSE)
    colnames(agg_local_df) <- c("Sepal_Length", "Avg")
    expected <- agg_local_df[order(agg_local_df$Sepal_Length), ]
    rownames(expected) <- NULL
    expect_identical(actual, expected)
  },
  finally = {
    # Resetting the conf back to default value
    callJMethod(conf, "set", "spark.sql.shuffle.partitions", shufflepartitionsvalue)
  })
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

test_that("Setting and getting config on SparkSession, sparkR.conf(), sparkR.uiWebUrl()", {
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

  url <- sparkR.uiWebUrl()
  expect_equal(substr(url, 1, 7), "http://")
})

test_that("enableHiveSupport on SparkSession", {
  setHiveContext(sc)
  unsetHiveContext()
  # if we are still here, it must be built with hive
  conf <- callJMethod(sparkSession, "conf")
  value <- callJMethod(conf, "get", "spark.sql.catalogImplementation")
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
              "Error in save : illegal argument - Expected exactly one path to be specified")
  expect_error(write.json(df, jsonPath),
              "Error in json : analysis error - path file:.*already exists")
  expect_error(write.text(df, jsonPath),
              "Error in text : analysis error - path file:.*already exists")
  expect_error(write.orc(df, jsonPath),
              "Error in orc : analysis error - path file:.*already exists")
  expect_error(write.parquet(df, jsonPath),
              "Error in parquet : analysis error - path file:.*already exists")
  expect_error(write.parquet(df, jsonPath, mode = 123), "mode should be character or omitted.")

  # Arguments checking in R side.
  expect_error(write.df(df, "data.tmp", source = c(1, 2)),
               paste("source should be character, NULL or omitted. It is the datasource specified",
                     "in 'spark.sql.sources.default' configuration by default."))
  expect_error(write.df(df, path = c(3)),
               "path should be character, NULL or omitted.")
  expect_error(write.df(df, mode = TRUE),
               "mode should be character or omitted. It is 'error' by default.")
})

test_that("Call DataFrameWriter.load() API in Java without path and check argument types", {
  # This tests if the exception is thrown from JVM not from SparkR side.
  # It makes sure that we can omit path argument in read.df API and then it calls
  # DataFrameWriter.load() without path.
  expect_error(read.df(source = "json"),
               paste("Error in load : analysis error - Unable to infer schema for JSON.",
                     "It must be specified manually"))
  expect_error(read.df("arbitrary_path"), "Error in load : analysis error - Path does not exist")
  expect_error(read.json("arbitrary_path"), "Error in json : analysis error - Path does not exist")
  expect_error(read.text("arbitrary_path"), "Error in text : analysis error - Path does not exist")
  expect_error(read.orc("arbitrary_path"), "Error in orc : analysis error - Path does not exist")
  expect_error(read.parquet("arbitrary_path"),
              "Error in parquet : analysis error - Path does not exist")

  # Arguments checking in R side.
  expect_error(read.df(path = c(3)),
               "path should be character, NULL or omitted.")
  expect_error(read.df(jsonPath, source = c(1, 2)),
               paste("source should be character, NULL or omitted. It is the datasource specified",
                     "in 'spark.sql.sources.default' configuration by default."))

  expect_warning(read.json(jsonPath, a = 1, 2, 3, "a"),
                 "Unnamed arguments ignored: 2, 3, a.")
})

test_that("Specify a schema by using a DDL-formatted string when reading", {
  # Test read.df with a user defined schema in a DDL-formatted string.
  df1 <- read.df(jsonPath, "json", "name STRING, age DOUBLE")
  expect_is(df1, "SparkDataFrame")
  expect_equal(dtypes(df1), list(c("name", "string"), c("age", "double")))

  expect_error(read.df(jsonPath, "json", "name stri"), "DataType stri is not supported.")

  # Test loadDF with a user defined schema in a DDL-formatted string.
  df2 <- loadDF(jsonPath, "json", "name STRING, age DOUBLE")
  expect_is(df2, "SparkDataFrame")
  expect_equal(dtypes(df2), list(c("name", "string"), c("age", "double")))

  expect_error(loadDF(jsonPath, "json", "name stri"), "DataType stri is not supported.")
})

test_that("Collect on DataFrame when NAs exists at the top of a timestamp column", {
  ldf <- data.frame(col1 = c(0, 1, 2),
                   col2 = c(as.POSIXct("2017-01-01 00:00:01"),
                            NA,
                            as.POSIXct("2017-01-01 12:00:01")),
                   col3 = c(as.POSIXlt("2016-01-01 00:59:59"),
                            NA,
                            as.POSIXlt("2016-01-01 12:01:01")))
  sdf1 <- createDataFrame(ldf)
  ldf1 <- collect(sdf1)
  expect_equal(dtypes(sdf1), list(c("col1", "double"),
                                  c("col2", "timestamp"),
                                  c("col3", "timestamp")))
  expect_equal(class(ldf1$col1), "numeric")
  expect_equal(class(ldf1$col2), c("POSIXct", "POSIXt"))
  expect_equal(class(ldf1$col3), c("POSIXct", "POSIXt"))

  # Columns with NAs at the top
  sdf2 <- filter(sdf1, "col1 > 1")
  ldf2 <- collect(sdf2)
  expect_equal(dtypes(sdf2), list(c("col1", "double"),
                                  c("col2", "timestamp"),
                                  c("col3", "timestamp")))
  expect_equal(class(ldf2$col1), "numeric")
  expect_equal(class(ldf2$col2), c("POSIXct", "POSIXt"))
  expect_equal(class(ldf2$col3), c("POSIXct", "POSIXt"))

  # Columns with only NAs, the type will also be cast to PRIMITIVE_TYPE
  sdf3 <- filter(sdf1, "col1 == 0")
  ldf3 <- collect(sdf3)
  expect_equal(dtypes(sdf3), list(c("col1", "double"),
                                  c("col2", "timestamp"),
                                  c("col3", "timestamp")))
  expect_equal(class(ldf3$col1), "numeric")
  expect_equal(class(ldf3$col2), c("POSIXct", "POSIXt"))
  expect_equal(class(ldf3$col3), c("POSIXct", "POSIXt"))
})

test_that("catalog APIs, listCatalogs, setCurrentCatalog, currentCatalog", {
  expect_equal(currentCatalog(), "spark_catalog")
  expect_error(setCurrentCatalog("spark_catalog"), NA)
  expect_error(setCurrentCatalog("zxwtyswklpf"),
               paste0("Error in setCurrentCatalog : ",
               "org.apache.spark.sql.connector.catalog.CatalogNotFoundException: ",
               "Catalog 'zxwtyswklpf' plugin class not found: ",
               "spark.sql.catalog.zxwtyswklpf is not defined"))
  catalogs <- collect(listCatalogs())
})

test_that("catalog APIs, currentDatabase, setCurrentDatabase, listDatabases, getDatabase", {
  expect_equal(currentDatabase(), "default")
  expect_error(setCurrentDatabase("default"), NA)
  expect_error(setCurrentDatabase("zxwtyswklpf"),
               paste0("Error in setCurrentDatabase : no such database - Database ",
               "'zxwtyswklpf' not found"))

  expect_true(databaseExists("default"))
  expect_true(databaseExists("spark_catalog.default"))
  expect_false(databaseExists("some_db"))
  expect_false(databaseExists("spark_catalog.some_db"))

  dbs <- collect(listDatabases())
  expect_equal(names(dbs), c("name", "catalog", "description", "locationUri"))
  expect_equal(which(dbs[, 1] == "default"), 1)

  db <- getDatabase("spark_catalog.default")
  expect_equal(db$name, "default")
  expect_equal(db$catalog, "spark_catalog")
})

test_that("catalog APIs, listTables, getTable, listColumns, listFunctions, functionExists", {
  tb <- listTables()
  count <- count(tables())
  expect_equal(nrow(listTables("default")), count)
  expect_equal(nrow(listTables("spark_catalog.default")), count)
  expect_equal(nrow(tb), count)
  expect_equal(colnames(tb),
               c("name", "catalog", "namespace", "description", "tableType", "isTemporary"))

  createOrReplaceTempView(as.DataFrame(cars), "cars")

  tb <- SparkR::listTables()
  expect_equal(nrow(tb), count + 1)
  tbs <- collect(tb)
  expect_true(nrow(tbs[tbs$name == "cars", ]) > 0)
  expect_error(listTables("bar"),
               "Error in listTables : no such database - Database 'bar' not found")

  c <- listColumns("cars")
  expect_equal(nrow(c), 2)
  expect_equal(colnames(c),
               c("name", "description", "dataType", "nullable", "isPartition", "isBucket"))
  expect_equal(collect(c)[[1]][[1]], "speed")
  expect_error(listColumns("zxwtyswklpf", "default"),
               paste("Table or view not found: spark_catalog.default.zxwtyswklpf"))

  f <- listFunctions()
  expect_true(nrow(f) >= 200) # 250
  expect_equal(colnames(f),
               c("name", "catalog", "namespace", "description", "className", "isTemporary"))
  expect_equal(take(orderBy(filter(f, "className IS NOT NULL"), "className"), 1)$className,
               "org.apache.spark.sql.catalyst.expressions.Abs")
  expect_error(listFunctions("zxwtyswklpf_db"),
               paste("Error in listFunctions : no such database - Database",
                     "'zxwtyswklpf_db' not found"))

  expect_true(functionExists("abs"))
  expect_false(functionExists("aabbss"))

  func0 <- getFunc("abs")
  expect_equal(func0$name, "abs")
  expect_equal(func0$className, "org.apache.spark.sql.catalyst.expressions.Abs")
  expect_true(func0$isTemporary)

  sql("CREATE FUNCTION func1 AS 'org.apache.spark.sql.catalyst.expressions.Add'")

  func1 <- getFunc("spark_catalog.default.func1")
  expect_equal(func1$name, "func1")
  expect_equal(func1$catalog, "spark_catalog")
  expect_equal(length(func1$namespace), 1)
  expect_equal(func1$namespace[[1]], "default")
  expect_equal(func1$className, "org.apache.spark.sql.catalyst.expressions.Add")
  expect_false(func1$isTemporary)

  expect_true(functionExists("func1"))
  expect_true(functionExists("default.func1"))
  expect_true(functionExists("spark_catalog.default.func1"))

  expect_false(functionExists("func2"))
  expect_false(functionExists("default.func2"))
  expect_false(functionExists("spark_catalog.default.func2"))

  sql("DROP FUNCTION func1")

  expect_false(functionExists("func1"))
  expect_false(functionExists("default.func1"))
  expect_false(functionExists("spark_catalog.default.func1"))

  # recoverPartitions does not work with temporary view
  expect_error(recoverPartitions("cars"),
               paste("Error in recoverPartitions : analysis error - cars is a temp view.",
                     "'recoverPartitions()' expects a table"), fixed = TRUE)
  expect_error(refreshTable("cars"), NA)
  expect_error(refreshByPath("/"), NA)

  view <- getTable("cars")
  expect_equal(view$name, "cars")
  expect_equal(view$tableType, "TEMPORARY")
  expect_true(view$isTemporary)

  dropTempView("cars")

  schema <- structType(structField("name", "string"), structField("age", "integer"),
                       structField("height", "float"))
  createTable("default.people", source = "json", schema = schema)

  tbl <- getTable("spark_catalog.default.people")
  expect_equal(tbl$name, "people")
  expect_equal(tbl$catalog, "spark_catalog")
  expect_equal(length(tbl$namespace), 1)
  expect_equal(tbl$namespace[[1]], "default")
  expect_equal(tbl$tableType, "MANAGED")
  expect_false(tbl$isTemporary)

  sql("DROP TABLE IF EXISTS people")
})

test_that("assert_true, raise_error", {
  df <- read.json(jsonPath)
  filtered <- filter(df, "age < 20")

  expect_equal(collect(select(filtered, assert_true(filtered$age < 20)))$age, c(NULL))
  expect_equal(collect(select(filtered, assert_true(filtered$age < 20, "error message")))$age,
               c(NULL))
  expect_equal(collect(select(filtered, assert_true(filtered$age < 20, filtered$name)))$age,
               c(NULL))
  expect_error(collect(select(df, assert_true(df$age < 20))), "is not true!")
  expect_error(collect(select(df, assert_true(df$age < 20, "error message"))),
               "error message")
  expect_error(collect(select(df, assert_true(df$age < 20, df$name))), "Michael")

  expect_error(collect(select(filtered, raise_error("error message"))), "error message")
  expect_error(collect(select(filtered, raise_error(filtered$name))), "Justin")
})

compare_list <- function(list1, list2) {
  # get testthat to show the diff by first making the 2 lists equal in length
  expect_equal(length(list1), length(list2))
  l <- max(length(list1), length(list2))
  length(list1) <- l
  length(list2) <- l
  expect_equal(sort(list1, na.last = TRUE), sort(list2, na.last = TRUE))
}

# This should always be the **very last test** in this test file.
test_that("No extra files are created in SPARK_HOME by starting session and making calls", {
  # Check that it is not creating any extra file.
  # Does not check the tempdir which would be cleaned up after.
  filesAfter <- list.files(path = sparkRDir, all.files = TRUE)

  expect_true(length(sparkRFilesBefore) > 0)
  # first, ensure derby.log is not there
  expect_false("derby.log" %in% filesAfter)
  # second, ensure only spark-warehouse is created when calling SparkSession, enableHiveSupport = F
  # note: currently all other test files have enableHiveSupport = F, so we capture the list of files
  # before creating a SparkSession with enableHiveSupport = T at the top of this test file
  # (filesBefore). The test here is to compare that (filesBefore) against the list of files before
  # any test is run in run-all.R (sparkRFilesBefore).
  # sparkRAllowedSQLDirs is also defined in run-all.R, and should contain only 2 allowed dirs,
  # here allow the first value, spark-warehouse, in the diff, everything else should be exactly the
  # same as before any test is run.
  compare_list(sparkRFilesBefore, setdiff(filesBefore, sparkRAllowedSQLDirs[[1]]))
  # third, ensure only spark-warehouse and metastore_db are created when enableHiveSupport = T
  # note: as the note above, after running all tests in this file while enableHiveSupport = T, we
  # check the list of files again. This time we allow both dirs to be in the diff.
  compare_list(sparkRFilesBefore, setdiff(filesAfter, sparkRAllowedSQLDirs))
})

unlink(parquetPath)
unlink(orcPath)
unlink(jsonPath)
unlink(jsonPathNa)
unlink(complexTypeJsonPath)
unlink(mapTypeJsonPath)

sparkR.session.stop()
