library(testthat)
library(devtools)
library(SparkR)

context("SparkSQL functions")

# Tests for jsonFile, registerTempTable, sql, count, table

sc <- sparkR.init()

sqlCtx <- sparkRSQL.init(sc)

mockLines <- c("{\"name\":\"Michael\"}",
               "{\"name\":\"Andy\", \"age\":30}",
               "{\"name\":\"Justin\", \"age\":19}")
jsonPath <- tempfile(pattern="sparkr-test", fileext=".tmp")
parquetPath <- tempfile(pattern="sparkr-test", fileext=".parquet")
writeLines(mockLines, jsonPath)

test_that("jsonFile() on a local file returns a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  expect_true(inherits(df, "DataFrame"))
  expect_true(count(df) == 3)
})

test_that("registerTempTable() results in a queryable table and sql() results in a new DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  registerTempTable(df, "table1")
  newdf <- sql(sqlCtx, "SELECT * FROM table1 where name = 'Michael'")
  expect_true(inherits(newdf, "DataFrame"))
  expect_true(count(newdf) == 1)
})

test_that("table() returns a new DataFrame", {
  tabledf <- table(sqlCtx, "table1")
  expect_true(inherits(tabledf, "DataFrame"))
  expect_true(count(tabledf) == 3)
})

test_that("toRDD() returns an RRDD", {
  df <- jsonFile(sqlCtx, jsonPath)
  testRDD <- toRDD(df)
  expect_true(inherits(testRDD, "RDD"))
  expect_true(count(testRDD) == 3)
})

test_that("union on two RDDs created from DataFrames returns an RRDD", {
  df <- jsonFile(sqlCtx, jsonPath)
  RDD1 <- toRDD(df)
  RDD2 <- toRDD(df)
  unioned <- unionRDD(RDD1, RDD2)
  expect_true(inherits(unioned, "RDD"))
  expect_true(SparkR:::getSerializedMode(unioned) == "byte")
  expect_true(collect(unioned)[[2]]$name == "Andy")
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

  df <- jsonFile(sqlCtx, jsonPath)
  dfRDD <- toRDD(df)

  unionByte <- unionRDD(rdd, dfRDD)
  expect_true(inherits(unionByte, "RDD"))
  expect_true(SparkR:::getSerializedMode(unionByte) == "byte")
  expect_true(collect(unionByte)[[1]] == 1)
  expect_true(collect(unionByte)[[12]]$name == "Andy")

  unionString <- unionRDD(textRDD, dfRDD)
  expect_true(inherits(unionString, "RDD"))
  expect_true(SparkR:::getSerializedMode(unionString) == "byte")
  expect_true(collect(unionString)[[1]] == "Michael")
  expect_true(collect(unionString)[[5]]$name == "Andy")
})

test_that("objectFile() works with row serialization", {
  objectPath <- tempfile(pattern="spark-test", fileext=".tmp")
  df <- jsonFile(sqlCtx, jsonPath)
  dfRDD <- toRDD(df)
  saveAsObjectFile(dfRDD, objectPath)
  objectIn <- objectFile(sc, objectPath)

  expect_true(inherits(objectIn, "RDD"))
  expect_true(SparkR:::getSerializedMode(objectIn) == "byte")
  expect_true(collect(objectIn)[[2]]$age == 30)
})

test_that("lapply() on a DataFrame returns an RDD with the correct columns", {
  df <- jsonFile(sqlCtx, jsonPath)
  testRDD <- lapply(df, function(row) {
    row$newCol <- row$age + 5
    row
    })
  expect_true(inherits(testRDD, "RDD"))
  collected <- collect(testRDD)
  expect_true(collected[[1]]$name == "Michael")
  expect_true(collected[[2]]$newCol == "35")
})

test_that("collect() returns a data.frame", {
  df <- jsonFile(sqlCtx, jsonPath)
  rdf <- collect(df)
  expect_true(is.data.frame(rdf))
  expect_true(names(rdf)[1] == "age")
  expect_true(nrow(rdf) == 3)
  expect_true(ncol(rdf) == 2)
})

test_that("limit() returns DataFrame with the correct number of rows", {
  df <- jsonFile(sqlCtx, jsonPath)
  dfLimited <- limit(df, 2)
  expect_true(inherits(dfLimited, "DataFrame"))
  expect_true(count(dfLimited) == 2)
})

test_that("collect() and take() on a DataFrame return the same number of rows and columns", {
  df <- jsonFile(sqlCtx, jsonPath)
  expect_true(nrow(collect(df)) == nrow(take(df, 10)))
  expect_true(ncol(collect(df)) == ncol(take(df, 10)))
})

test_that("multiple pipeline transformations starting with a DataFrame result in an RDD with the correct values", {
  df <- jsonFile(sqlCtx, jsonPath)
  first <- lapply(df, function(row) {
    row$age <- row$age + 5
    row
  })
  second <- lapply(first, function(row) {
    row$testCol <- if (row$age == 35 && !is.na(row$age)) TRUE else FALSE
    row
  })
  expect_true(inherits(second, "RDD"))
  expect_true(count(second) == 3)
  expect_true(collect(second)[[2]]$age == 35)
  expect_true(collect(second)[[2]]$testCol)
  expect_false(collect(second)[[3]]$testCol)
})

test_that("cache(), persist(), and unpersist() on a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
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
  df <- jsonFile(sqlCtx, jsonPath)
  testSchema <- schema(df)
  expect_true(length(testSchema$fields()) == 2)
  expect_true(testSchema$fields()[[1]]$dataType.toString() == "LongType")
  expect_true(testSchema$fields()[[2]]$dataType.simpleString() == "string")
  expect_true(testSchema$fields()[[1]]$name() == "age")

  testTypes <- dtypes(df)
  expect_true(length(testTypes[[1]]) == 2)
  expect_true(testTypes[[1]][1] == "age")

  testCols <- columns(df)
  expect_true(length(testCols) == 2)
  expect_true(testCols[2] == "name")

  testNames <- names(df)
  expect_true(length(testNames) == 2)
  expect_true(testNames[2] == "name")
})

test_that("head() and first() return the correct data", {
  df <- jsonFile(sqlCtx, jsonPath)
  testHead <- head(df)
  expect_true(nrow(testHead) == 3)
  expect_true(ncol(testHead) == 2)

  testHead2 <- head(df, 2)
  expect_true(nrow(testHead2) == 2)
  expect_true(ncol(testHead2) == 2)

  testFirst <- first(df)
  expect_true(nrow(testFirst) == 1)
})

test_that("distinct() on DataFrames", {
  lines <- c("{\"name\":\"Michael\"}",
             "{\"name\":\"Andy\", \"age\":30}",
             "{\"name\":\"Justin\", \"age\":19}",
             "{\"name\":\"Justin\", \"age\":19}")
  jsonPathWithDup <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(lines, jsonPathWithDup)

  df <- jsonFile(sqlCtx, jsonPathWithDup)
  uniques <- distinct(df)
  expect_true(inherits(uniques, "DataFrame"))
  expect_true(count(uniques) == 3)
})

test_that("sampleDF on a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  sampled <- sampleDF(df, FALSE, 1.0)
  expect_equal(nrow(collect(sampled)), count(df))
  expect_true(inherits(sampled, "DataFrame"))
  sampled2 <- sampleDF(df, FALSE, 0.1)
  expect_true(count(sampled2) < 3)
})

test_that("select with column", {
  df <- jsonFile(sqlCtx, jsonPath)
  df1 <- select(df, "name")
  expect_true(columns(df1) == c("name"))
  expect_true(count(df1) == 3)

  df2 <- select(df, df$age)
  expect_true(columns(df2) == c("age"))
  expect_true(count(df2) == 3)
})

test_that("column calculation", {
  df <- jsonFile(sqlCtx, jsonPath)
  d <- collect(select(df, alias(df$age + 1, "age2")))
  expect_true(names(d) == c("age2"))
  df2 <- select(df, lower(df$name), abs(df$age))
  expect_true(inherits(df2, "DataFrame"))
  expect_true(count(df2) == 3)
})

test_that("load() from json file", {
  df <- loadDF(sqlCtx, jsonPath, "json")
  expect_true(inherits(df, "DataFrame"))
  expect_true(count(df) == 3)
})

test_that("save() as parquet file", {
  df <- loadDF(sqlCtx, jsonPath, "json")
  saveDF(df, parquetPath, "parquet", mode="overwrite")
  df2 <- loadDF(sqlCtx, parquetPath, "parquet")
  expect_true(inherits(df2, "DataFrame"))
  expect_true(count(df2) == 3)
})

#test_that("test HiveContext", {
#  hiveCtx <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils",
#                         "createTestHiveContext",
#                         sc)
#  df <- createExternalTable(hiveCtx, "json", jsonPath, "json")
#  expect_true(inherits(df, "DataFrame"))
#  expect_true(count(df) == 3)
#  df2 <- sql(hiveCtx, "select * from json")
#  expect_true(inherits(df2, "DataFrame"))
#  expect_true(count(df2) == 3)
#
#  jsonPath2 <- tempfile(pattern="sparkr-test", fileext=".tmp")
#  saveAsTable(df, "json", "json", "append", path = jsonPath2)
#  df3 <- sql(hiveCtx, "select * from json")
#  expect_true(inherits(df3, "DataFrame"))
#  expect_true(count(df3) == 6)
#})

test_that("column operators", {
  c <- SparkR:::col("a")
  c2 <- (- c + 1 - 2) * 3 / 4.0
  c3 <- (c + c2 - c2) * c2 %% c2
  c4 <- (c > c2) & (c2 <= c3) | (c == c2) & (c2 != c3)
})

test_that("column functions", {
  c <- SparkR:::col("a")
  c2 <- min(c) + max(c) + sum(c) + avg(c) + count(c) + abs(c) + sqrt(c)
  c3 <- lower(c) + upper(c) + first(c) + last(c)
  c4 <- approxCountDistinct(c) + countDistinct(c) + cast(c, "string")
})

test_that("sortDF() and orderBy() on a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  sorted <- sortDF(df, df$age)
  expect_true(collect(sorted)[1,2] == "Michael")

  sorted2 <- sortDF(df, "name")
  expect_true(collect(sorted2)[2,"age"] == 19)

  sorted3 <- orderBy(df, asc(df$age))
  expect_true(is.na(first(sorted3)$age))
  expect_true(collect(sorted3)[2, "age"] == 19)
  
  sorted4 <- orderBy(df, desc(df$name))
  expect_true(first(sorted4)$name == "Michael")
  expect_true(collect(sorted4)[3,"name"] == "Andy")
})

test_that("filter() on a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  filtered <- filter(df, "age > 20")
  expect_true(count(filtered) == 1)
  expect_true(collect(filtered)$name == "Andy")
  filtered2 <- where(df, df$name != "Michael")
  expect_true(count(filtered2) == 2)
  expect_true(collect(filtered2)$age[2] == 19)
})

test_that("join() on a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  
  mockLines2 <- c("{\"name\":\"Michael\", \"test\": \"yes\"}",
                  "{\"name\":\"Andy\",  \"test\": \"no\"}",
                  "{\"name\":\"Justin\", \"test\": \"yes\"}",
                  "{\"name\":\"Bob\", \"test\": \"yes\"}")
  jsonPath2 <- tempfile(pattern="sparkr-test", fileext=".tmp")
  writeLines(mockLines2, jsonPath2)
  df2 <- jsonFile(sqlCtx, jsonPath2)
  
  joined <- join(df, df2)
  expect_equal(names(joined), c("age", "name", "name", "test"))
  expect_true(count(joined) == 12)
  
  joined2 <- join(df, df2, df$name == df2$name)
  expect_equal(names(joined2), c("age", "name", "name", "test"))
  expect_true(count(joined2) == 3)
  
  joined3 <- join(df, df2, df$name == df2$name, "right_outer")
  expect_equal(names(joined3), c("age", "name", "name", "test"))
  expect_true(count(joined3) == 4)
  expect_true(is.na(collect(joined3)$age[4]))
  
  joined4 <- select(join(df, df2, df$name == df2$name, "outer"),
                    alias(df$age + 5, "newAge"), df$name, df2$test)
  expect_equal(names(joined4), c("newAge", "name", "test"))
  expect_true(count(joined4) == 4)
  expect_true(first(joined4)$newAge == 24)
})

unlink(jsonPath)
