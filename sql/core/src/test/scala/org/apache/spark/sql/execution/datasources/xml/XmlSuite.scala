/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.xml

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.nio.file.{Files, Path, Paths}
import java.sql.{Date, Timestamp}
import java.time.Instant
import java.util.TimeZone

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Dataset, Encoders, QueryTest, Row, RowFactory, SaveMode}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlInferSchema, XmlOptions}
import org.apache.spark.sql.catalyst.xml.XmlOptions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.xml.TestUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class XmlSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val resDir = "test-data/xml-resources/"

  private val keepInnerXmlDir = "keep-inner-xml-as-raw/"

  private var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("XmlSuite")
    tempDir.toFile.deleteOnExit()
  }

  private def getEmptyTempDir(): Path = {
    Files.createTempDirectory(tempDir, "test")
  }

  // Tests

  test("DSL test") {
    val results = spark.read.format("xml")
      .option("rowTag", "ROW")
      .option("multiLine", "true")
      .load(getTestResourcePath(resDir + "cars.xml"))
      .select("year")
      .collect()

    assert(results.length === 3)
  }

  test("DSL test with xml having unbalanced datatypes") {
    val results = spark.read
      .option("rowTag", "ROW")
      .option("nullValue", "")
      .option("multiLine", "true")
      .xml(getTestResourcePath(resDir + "gps-empty-field.xml"))

    assert(results.collect().length === 2)
  }

  test("DSL test with mixed elements (attributes, no child)") {
    val results = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars-mixed-attr-no-child.xml"))
      .select("date")
      .collect()

    val attrValOne = results(0).getStruct(0).getString(1)
    val attrValTwo = results(1).getStruct(0).getString(1)
    assert(attrValOne == "string")
    assert(attrValTwo == "struct")
    assert(results.length === 3)
  }

  test("DSL test for inconsistent element attributes as fields") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-attributes-in-no-child.xml"))
      .select("price")

    // This should not throw an exception `java.lang.ArrayIndexOutOfBoundsException`
    // as non-existing values are represented as `null`s.
    val res = results.collect();
    assert(res(0).getStruct(0).get(1) === null)
  }

  test("DSL test with mixed elements (struct, string)") {
    val results = spark.read
      .option("rowTag", "person")
      .xml(getTestResourcePath(resDir + "ages-mixed-types.xml"))
      .collect()
    assert(results.length === 3)
  }

  test("DSL test with elements in array having attributes") {
    val results = spark.read
      .option("rowTag", "person")
      .xml(getTestResourcePath(resDir + "ages.xml"))
      .collect()
    val attrValOne = results(0).getStruct(0).getAs[Date](1)
    val attrValTwo = results(1).getStruct(0).getAs[Date](1)
    assert(attrValOne.toString === "1990-02-24")
    assert(attrValTwo.toString === "1985-01-01")
    assert(results.length === 3)
  }

  test("DSL test for iso-8859-1 encoded file") {
    val dataFrame = spark.read
      .option("rowTag", "ROW")
      .option("charset", StandardCharsets.ISO_8859_1.name)
      .xml(getTestResourcePath(resDir + "cars-iso-8859-1.xml"))
    assert(dataFrame.select("year").collect().length === 3)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.head() === Row("No comment", 2012))
  }

  test("DSL test compressed file") {
    val results = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars.xml.gz"))
      .select("year")
      .collect()

    assert(results.length === 3)
  }

  test("DSL test splittable compressed file") {
    val results = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars.xml.bz2"))
      .select("year")
      .collect()

    assert(results.length === 3)
  }

  test("DSL test bad charset name") {
    // val exception = intercept[UnsupportedCharsetException] {
    val exception = intercept[SparkException] {
      spark.read
        .option("rowTag", "ROW")
        .option("charset", "1-9588-osi")
        .xml(getTestResourcePath(resDir + "cars.xml"))
        .select("year")
        .collect()
    }
    ExceptionUtils.getRootCause(exception).isInstanceOf[UnsupportedCharsetException]
    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("DDL test") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW carsTable1
         |USING org.apache.spark.sql.execution.datasources.xml
         |OPTIONS (rowTag "ROW", path "${getTestResourcePath(resDir + "cars.xml")}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable1").collect().length === 3)
  }

  test("DDL test with alias name") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW carsTable2
         |USING xml
         |OPTIONS (rowTag "ROW", path "${getTestResourcePath(resDir + "cars.xml")}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable2").collect().length === 3)
  }

  test("DSL test for parsing a malformed XML file") {
    val results = spark.read
      .option("rowTag", "ROW")
      .option("mode", DropMalformedMode.name)
      .xml(getTestResourcePath(resDir + "cars-malformed.xml"))

    assert(results.count() === 1)
  }

  test("DSL test for dropping malformed rows") {
    val cars = spark.read
      .option("rowTag", "ROW")
      .option("mode", DropMalformedMode.name)
      .xml(getTestResourcePath(resDir + "cars-malformed.xml"))

    assert(cars.count() == 1)
    assert(cars.head() === Row("Chevy", "Volt", 2015))
  }

  test("DSL test for failing fast") {
    val exceptionInParse = intercept[SparkException] {
      spark.read
        .option("rowTag", "ROW")
        .option("mode", FailFastMode.name)
        .xml(getTestResourcePath(resDir + "cars-malformed.xml"))
        .collect()
    }
    checkError(
      // TODO: Exception was nested two level deep as opposed to just one like json/csv
      exception = exceptionInParse.getCause.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[null,null,null]",
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("test FAILFAST with unclosed tag") {
    val exceptionInParse = intercept[SparkException] {
      spark.read
        .option("rowTag", "book")
        .option("mode", FailFastMode.name)
        .xml(getTestResourcePath(resDir + "unclosed_tag.xml"))
        .show()
    }
    checkError(
      // TODO: Exception was nested two level deep as opposed to just one like json/csv
      exception = exceptionInParse.getCause.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[empty row]",
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("DSL test for permissive mode for corrupt records") {
    val carsDf = spark.read
      .option("rowTag", "ROW")
      .option("mode", PermissiveMode.name)
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(getTestResourcePath(resDir + "cars-malformed.xml"))
    val cars = carsDf.collect()
    assert(cars.length === 3)

    val malformedRowOne = carsDf.cache().select("_malformed_records").first().get(0).toString
    val malformedRowTwo = carsDf.cache().select("_malformed_records").take(2).last.get(0).toString
    val expectedMalformedRowOne = "<ROW><year>2012</year><make>Tesla</make><model>>S" +
      "<comment>No comment</comment></ROW>"
    val expectedMalformedRowTwo = "<ROW></year><make>Ford</make><model>E350</model>model></model>" +
      "<comment>Go get one now they are going fast</comment></ROW>"

    assert(malformedRowOne.replaceAll("\\s", "") === expectedMalformedRowOne.replaceAll("\\s", ""))
    assert(malformedRowTwo.replaceAll("\\s", "") === expectedMalformedRowTwo.replaceAll("\\s", ""))
    assert(cars(2)(0) === null)
    assert(cars(0).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(1).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(2).toSeq.takeRight(3) === Seq("Chevy", "Volt", 2015))
  }

  test("DSL test with empty file and known schema") {
    val results = spark.read
      .option("rowTag", "ROW")
      .schema(buildSchema(field("column", StringType, false)))
      .xml(getTestResourcePath(resDir + "empty.xml"))
      .count()

    assert(results === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val schema = buildSchema(
      field("color"),
      field("year"),
      field("make"),
      field("model"),
      field("comment"))
    val results = spark.read.schema(schema)
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars-unbalanced-elements.xml"))
      .count()

    assert(results === 3)
  }

  test("DDL test with empty file") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW carsTable3
         |(year double, make string, model string, comments string, grp string)
         |USING xml
         |OPTIONS (rowTag "ROW", path "${getTestResourcePath(resDir + "empty.xml")}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT count(*) FROM carsTable3").collect().head(0) === 0)
  }

  test("SQL test insert overwrite") {
    val tempPath = getEmptyTempDir()
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW booksTableIO
         |USING xml
         |OPTIONS (path "${getTestResourcePath(resDir + "books.xml")}", rowTag "book")
      """.stripMargin.replaceAll("\n", " "))
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW booksTableEmpty
         |(author string, description string, genre string,
         |id string, price double, publish_date string, title string)
         |USING xml
         |OPTIONS (rowTag "ROW", path "$tempPath")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT * FROM booksTableIO").collect().length === 12)
    assert(spark.sql("SELECT * FROM booksTableEmpty").collect().isEmpty)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE booksTableEmpty
         |SELECT * FROM booksTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(spark.sql("SELECT * FROM booksTableEmpty").collect().length == 12)
  }

  test("DSL save with gzip compression codec") {
    val copyFilePath = getEmptyTempDir().resolve("cars-copy.xml")

    val cars = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars.xml"))
    cars.write
      .mode(SaveMode.Overwrite)
      .options(Map("rowTag" -> "ROW", "compression" -> classOf[GzipCodec].getName))
      .xml(copyFilePath.toString)
    // Check that the part file has a .gz extension
    assert(Files.list(copyFilePath).iterator().asScala
      .count(_.getFileName.toString().endsWith(".xml.gz")) === 1)

    val carsCopy = spark.read.option("rowTag", "ROW").xml(copyFilePath.toString)

    assert(carsCopy.count() === cars.count())
    assert(carsCopy.collect().map(_.toString).toSet === cars.collect().map(_.toString).toSet)
  }

  test("DSL save with gzip compression codec by shorten name") {
    val copyFilePath = getEmptyTempDir().resolve("cars-copy.xml")

    val cars = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars.xml"))
    cars.write
      .mode(SaveMode.Overwrite)
      .options(Map("rowTag" -> "ROW", "compression" -> "gZiP"))
      .xml(copyFilePath.toString)

    // Check that the part file has a .gz extension
    assert(Files.list(copyFilePath).iterator().asScala
      .count(_.getFileName.toString().endsWith(".xml.gz")) === 1)

    val carsCopy = spark.read.option("rowTag", "ROW").xml(copyFilePath.toString)

    assert(carsCopy.count() === cars.count())
    assert(carsCopy.collect().map(_.toString).toSet === cars.collect().map(_.toString).toSet)
  }

  test("DSL save") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated.xml"))
    books.write
      .options(Map("rootTag" -> "books", "rowTag" -> "book"))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", "book")
      .xml(copyFilePath.toString)
    assert(booksCopy.count() === books.count())
    assert(booksCopy.collect().map(_.toString).toSet === books.collect().map(_.toString).toSet)
  }

  test("DSL save with declaration") {
    val copyFilePath1 = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated.xml"))

    books.write
      .options(Map("rootTag" -> "books", "rowTag" -> "book", "declaration" -> ""))
      .xml(copyFilePath1.toString)

    val xmlFile1 =
      Files.list(copyFilePath1).iterator.asScala
        .filter(_.getFileName.toString.startsWith("part-")).next()
    val firstLine = getLines(xmlFile1).head
    assert(firstLine === "<books>")

    val copyFilePath2 = getEmptyTempDir().resolve("books-copy.xml")

    books.write
      .options(Map("rootTag" -> "books", "rowTag" -> "book"))
      .xml(copyFilePath2.toString)

    val xmlFile2 =
      Files.list(copyFilePath2).iterator.asScala
        .filter(_.getFileName.toString.startsWith("part-")).next()
    assert(getLines(xmlFile2).head ===
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
  }

  test("DSL save with item") {
    val tempPath = getEmptyTempDir().resolve("items-temp.xml")
    val items = spark.createDataFrame(Seq(Tuple1(Array(Array(3, 4))))).toDF("thing").repartition(1)
    items.write
      .option("rowTag", "ROW")
      .option("arrayElementName", "foo").xml(tempPath.toString)

    val xmlFile =
      Files.list(tempPath).iterator.asScala
        .filter(_.getFileName.toString.startsWith("part-")).next()
    assert(getLines(xmlFile).count(_.contains("<foo>")) === 2)
  }

  test("DSL save with nullValue") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated.xml"))
    books.write
      .options(Map("rootTag" -> "books", "rowTag" -> "book", "nullValue" -> ""))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", "book")
      .option("nullValue", "")
      .xml(copyFilePath.toString)

    assert(booksCopy.count() === books.count())
    assert(booksCopy.collect().map(_.toString).toSet === books.collect().map(_.toString).toSet)
  }

  test("Write values properly as given to valueTag even if it starts with attributePrefix") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val rootTag = "catalog"
    val books = spark.read
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-attributes-in-no-child.xml"))

    books.write
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rootTag", rootTag)
      .option("rowTag", "book")
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "_")
      .option("rowTag", "book")
      .xml(copyFilePath.toString)

    assert(booksCopy.count() === books.count())
    assert(booksCopy.collect().map(_.toString).toSet === books.collect().map(_.toString).toSet)
  }

  test("DSL save dataframe not read from a XML file") {
    val copyFilePath = getEmptyTempDir().resolve("data-copy.xml")

    val schema = buildSchema(arrayField("a", ArrayType(StringType)))
    val data = spark.sparkContext.parallelize(
      List(List(List("aa", "bb"), List("aa", "bb"))).map(Row(_)))
    val df = spark.createDataFrame(data, schema)
    df.write.option("rowTag", "ROW").xml(copyFilePath.toString)

    // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is the element
    // name for XML file. Now, it is "item" by default. So, "item" field is additionally added
    // to wrap the element.
    val schemaCopy = buildSchema(
      structArray("a",
        field(XmlOptions.DEFAULT_ARRAY_ELEMENT_NAME, ArrayType(StringType))))
    val dfCopy = spark.read.option("rowTag", "ROW").xml(copyFilePath.toString)

    assert(dfCopy.count() === df.count())
    assert(dfCopy.schema === schemaCopy)
  }

  test("DSL save dataframe with data types correctly") {
    val copyFilePath = getEmptyTempDir().resolve("data-copy.xml")

    // Create the schema.
    val dataTypes = Array(
      StringType, NullType, BooleanType,
      ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType(25, 3), DecimalType(6, 5),
      DateType, TimestampType, MapType(StringType, StringType))
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      field(s"col$index", dataType)
    }
    val schema = StructType(fields)

    val currentTZ = TimeZone.getDefault
    try {
      // Tests will depend on default timezone, so set it to UTC temporarily
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
      // Create the data
      val timestamp = "2015-01-01 00:00:00"
      val date = "2015-01-01"
      val row =
        Row(
          "aa", null, true,
          1.toByte, 1.toShort, 1, 1.toLong,
          1.toFloat, 1.toDouble, Decimal(1, 25, 3), Decimal(1, 6, 5),
          Date.valueOf(date), Timestamp.valueOf(timestamp), Map("a" -> "b"))
      val data = spark.sparkContext.parallelize(Seq(row))

      val df = spark.createDataFrame(data, schema)
      df.write.option("rowTag", "ROW").xml(copyFilePath.toString)

      val dfCopy = spark.read.option("rowTag", "ROW").schema(schema)
        .xml(copyFilePath.toString)

      assert(dfCopy.collect() === df.collect())
      assert(dfCopy.schema === df.schema)
    } finally {
      TimeZone.setDefault(currentTZ)
    }
  }

  test("DSL test schema inferred correctly") {
    val results = spark.read.option("rowTag", "book").xml(getTestResourcePath(resDir + "books.xml"))

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date", DateType),
      field("title")))

    assert(results.collect().length === 12)
  }

  test("DSL test schema inferred correctly with sampling ratio") {
    val results = spark.read
      .option("rowTag", "book")
      .option("samplingRatio", 0.5)
      .xml(getTestResourcePath(resDir + "books.xml"))

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date", DateType),
      field("title")))

    assert(results.collect().length === 12)
  }

  test("DSL test schema (object) inferred correctly") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-nested-object.xml"))

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      structField("publish_dates",
        field("publish_date", DateType)),
      field("title")))

    assert(results.collect().length === 12)
  }

  test("DSL test schema (array) inferred correctly") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-nested-array.xml"))

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      arrayField("publish_date", DateType),
      field("title")))

    assert(results.collect().length === 12)
  }

  test("DSL test schema (complicated) inferred correctly") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated.xml"))

    assert(results.schema == buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      structField("genre",
        field("genreid", LongType),
        field("name")),
      field("price", DoubleType),
      structField("publish_dates",
        arrayField("publish_date",
          structField(
            field(s"${DEFAULT_ATTRIBUTE_PREFIX}tag"),
            field("day", LongType),
            field("month", LongType),
            field("year", LongType)))),
      field("title")))

    assert(results.collect().length === 3)
  }

  test("DSL test parsing and inferring attribute in elements having no child element") {
    // Default value.
    val resultsOne = spark.read.option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-attributes-in-no-child.xml"))

    val schemaOne = buildSchema(
      field("_id"),
      field("author"),
      structField("price",
        field("_VALUE"),
        field(s"_unit")),
      field("publish_date", DateType),
      field("title"))

    assert(resultsOne.schema === schemaOne)
    assert(resultsOne.count() === 12)

    // Explicitly set
    val attributePrefix = "@#"
    val valueTag = "#@@value"
    val resultsTwo = spark.read
      .option("rowTag", "book")
      .option("attributePrefix", attributePrefix)
      .option("valueTag", valueTag)
      .xml(getTestResourcePath(resDir + "books-attributes-in-no-child.xml"))

    val schemaTwo = buildSchema(
      field(s"${attributePrefix}id"),
      field("author"),
      structField("price",
        field(valueTag),
        field(s"${attributePrefix}unit")),
      field("publish_date", DateType),
      field("title"))

    assert(resultsTwo.schema === schemaTwo)
    assert(resultsTwo.count() === 12)
  }

  test("DSL test schema (excluding tags) inferred correctly") {
    val results = spark.read
      .option("excludeAttribute", true)
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books.xml"))

    val schema = buildSchema(
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date", DateType),
      field("title"))

    assert(results.schema === schema)
  }

  test("DSL test with custom schema") {
    val schema = buildSchema(
      field("make"),
      field("model"),
      field("comment"),
      field("color"),
      field("year", IntegerType))
    val results = spark.read.option("rowTag", "ROW").schema(schema)
      .xml(getTestResourcePath(resDir + "cars-unbalanced-elements.xml"))
      .count()

    assert(results === 3)
  }

  test("DSL test inferred schema passed through") {
    val dataFrame = spark.read.option("rowTag", "ROW").xml(getTestResourcePath(resDir + "cars.xml"))

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.head() === Row("No comment", 2012))
  }

  test("DSL test nullable fields") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age"))
    val results = spark.read.option("rowTag", "ROW").schema(schema)
      .xml(getTestResourcePath(resDir + "null-numbers.xml"))
      .select("name", "age")
      .collect()

    assert(results(0) === Row("alice", "35"))
    assert(results(1) === Row("bob", "    "))
    assert(results(2) === Row("coc", "24"))
  }

  test("DSL test for treating empty string as null value") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age", IntegerType))
    val results = spark.read.schema(schema)
      .option("rowTag", "ROW")
      .option("nullValue", "")
      .xml(getTestResourcePath(resDir + "null-numbers.xml"))
      .select("name", "age")
      .collect()

    assert(results(1) === Row("bob", null))
  }

  test("DSL test with namespaces ignored") {
    val results = spark.read
      .option("rowTag", "Topic")
      .xml(getTestResourcePath(resDir + "topics-namespaces.xml"))
      .collect()

    assert(results.length === 1)
  }

  test("xs_any array matches single element") {
    val schema = buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date"),
      field("xs_any"))
    val results = spark.read.schema(schema).option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books.xml"))
      // .select("xs_any")
      .collect()
    results.foreach { r =>
      assert(r.getString(0) != null)
    }
  }

  test("xs_any array matches multiple elements") {
    val schema = buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      arrayField("xs_any", StringType))
    val results = spark.read.schema(schema).option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books.xml"))
      .collect()
    results.foreach { r =>
      assert(r.getAs[Seq[String]]("xs_any").size === 3)
    }
  }

  test("Missing nested struct represented as Row of nulls instead of null") {
    val result = spark.read
      .option("rowTag", "item")
      .xml(getTestResourcePath(resDir + "null-nested-struct.xml"))
      .select("b.es")
      .collect()

    assert(result(1).getStruct(0) !== null)
    assert(result(1).getStruct(0)(0) === null)
  }

  test("Produces correct result for empty vs non-existent rows") {
    val schema = buildSchema(
      structField("b",
        structField("es",
          field("e"),
          field("f"))))
    val result = spark.read
      .option("rowTag", "item")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "null-nested-struct-2.xml"))
      .collect()

    assert(result(0) === Row(Row(null)))
    assert(result(1) === Row(Row(Row(null, null))))
    assert(result(2) === Row(Row(Row("E", null))))
    assert(result(3) === Row(Row(Row("E", " "))))
    assert(result(4) === Row(Row(Row("E", ""))))
  }

  test("Produces correct order of columns for nested rows when user specifies a schema") {
    val schema = buildSchema(
      structField("c",
        field("b", IntegerType),
        field("a", IntegerType)))

    val result = spark.read.schema(schema)
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "simple-nested-objects.xml"))
      .select("c.a", "c.b")
      .collect()

    assert(result(0) === Row(111, 222))
  }

  private[this] def testNextedElementFromFile(xmlFile: String): Unit = {
    val lines = getLines(Paths.get(xmlFile.replace("file:/", "/"))).toList
    val firstExpected = lines(2).trim
    val lastExpected = lines(3).trim
    val config = new Configuration(spark.sessionState.newHadoopConf())
    config.set(XmlInputFormat.START_TAG_KEY, "<parent>")
    config.set(XmlInputFormat.END_TAG_KEY, "</parent>")
    val records = spark.sparkContext.newAPIHadoopFile(
      xmlFile,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      config)
    val list = records.values.map(_.toString).collect().toList
    assert(list.length === 2)
    val firstActual = list.head
    val lastActual = list.last
    assert(firstActual === firstExpected)
    assert(lastActual === lastExpected)
  }

  test("Nested element with same name as parent delineation") {
    testNextedElementFromFile(getTestResourcePath(resDir +
      "nested-element-with-name-of-parent.xml"))
  }

  test("Nested element including attribute with same name as parent delineation") {
    testNextedElementFromFile(getTestResourcePath(resDir +
      "nested-element-with-attributes-and-name-of-parent.xml"))
  }

  test("Nested element with same name as parent schema inference") {
    val df = spark.read.option("rowTag", "parent")
      .xml(getTestResourcePath(resDir + "nested-element-with-name-of-parent.xml"))

    val schema = buildSchema(
      field("child"),
      structField("parent",
        field("child")))
    assert(df.schema === schema)
  }

  test("Skip and project currently XML files without indentation") {
    val df = spark.read
      .option("rowTag", "ROW")
      .xml(getTestResourcePath(resDir + "cars-no-indentation.xml"))
    val results = df.select("model").collect()
    val years = results.map(_(0)).toSet
    assert(years === Set("S", "E350", "Volt"))
  }

  test("Select correctly all child fields regardless of pushed down projection") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated.xml"))
      .selectExpr("publish_dates")
      .collect()
    results.foreach { row =>
      // All nested fields should not have nulls but arrays.
      assert(!row.anyNull)
    }
  }

  test("Empty string not allowed for rowTag, attributePrefix and valueTag.") {
    val messageOne = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageOne === "requirement failed: 'rowTag' option should not be an empty string.")

    val messageThree = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "ROW")
        .option("valueTag", "").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageThree === "requirement failed: 'valueTag' option should not be empty string.")
  }

  test("'rowTag' and 'rootTag' should not include angle brackets") {
    val messageOne = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "ROW>").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageOne === "requirement failed: 'rowTag' should not include angle brackets")

    val messageTwo = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "ROW")
        .option("rowTag", "<ROW").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(
      messageTwo === "requirement failed: 'rowTag' should not include angle brackets")

    val messageThree = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "ROW")
        .option("rootTag", "ROWSET>").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageThree === "requirement failed: 'rootTag' should not include angle brackets")

    val messageFour = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "ROW")
        .option("rootTag", "<ROWSET").xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageFour === "requirement failed: 'rootTag' should not include angle brackets")
  }

  test("valueTag and attributePrefix should not be the same.") {
    val messageOne = intercept[IllegalArgumentException] {
      spark.read
        .option("rowTag", "ROW")
        .option("valueTag", "#abc")
        .option("attributePrefix", "#abc")
        .xml(getTestResourcePath(resDir + "cars.xml"))
    }.getMessage
    assert(messageOne ===
      "requirement failed: 'valueTag' and 'attributePrefix' options should not be the same.")
  }

  test("nullValue test") {
    val resultsOne = spark.read
      .option("rowTag", "ROW")
      .option("nullValue", "")
      .xml(getTestResourcePath(resDir + "gps-empty-field.xml"))
    assert(resultsOne.selectExpr("extensions.TrackPointExtension").head().getStruct(0) !== null)
    assert(resultsOne.selectExpr("extensions.TrackPointExtension")
      .head().getStruct(0)(0) === null)
    // Is the behavior below consistent? see line above.
    assert(resultsOne.selectExpr("extensions.TrackPointExtension.hr").head().getStruct(0) === null)
    assert(resultsOne.collect().length === 2)

    val resultsTwo = spark.read
      .option("rowTag", "ROW")
      .option("nullValue", "2013-01-24T06:18:43Z")
      .xml(getTestResourcePath(resDir + "gps-empty-field.xml"))
    assert(resultsTwo.selectExpr("time").head().getStruct(0) === null)
    assert(resultsTwo.collect().length === 2)
  }

  test("ignoreSurroundingSpace with string types") {
    val df = spark.read
      .option("inferSchema", true)
      .option("rowTag", "entry")
      .option("ignoreSurroundingSpaces", true)
      .xml(getTestResourcePath(resDir + "feed-with-spaces.xml"))
    val results = df.collect().map(_.getString(0))
    assert(results === Array("A", "B", "C", "D"))
  }

  test("ignoreSurroundingSpaces with non-string types") {
    val results = spark.read
      .option("ignoreSurroundingSpaces", true)
      .option("rowTag", "person")
      .xml(getTestResourcePath(resDir + "ages-with-spaces.xml"))
      .collect()
    val attrValOne = results(0).getStruct(0)(1)
    val attrValTwo = results(1).getStruct(0)(0)
    assert(attrValOne.toString === "1990-02-24")
    assert(attrValTwo === 30)
    assert(results.length === 3)
  }

  test("DSL test with malformed attributes") {
    val results = spark.read
      .option("mode", DropMalformedMode.name)
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-malformed-attributes.xml"))
      .collect()

    assert(results.length === 2)
    assert(results(0)(0) === "bk111")
    assert(results(1)(0) === "bk112")
  }

  test("read utf-8 encoded file with empty tag") {
    val df = spark.read
      .option("excludeAttribute", "false")
      .option("rowTag", "House")
      .xml(getTestResourcePath(resDir + "fias_house.xml"))

    assert(df.collect().length === 37)
    assert(df.select().where("_HOUSEID is null").count() == 0)
  }

  test("attributes start with new line") {
    val schema = buildSchema(
      field("_schemaLocation"),
      field("_xmlns"),
      field("_xsi"),
      field("body"),
      field("from"),
      field("heading"),
      field("to"))

    val rowsCount = 1

    Seq("attributesStartWithNewLine.xml",
      "attributesStartWithNewLineCR.xml",
      "attributesStartWithNewLineLF.xml").foreach { file =>
      val df = spark.read
        .option("ignoreNamespace", "true")
        .option("excludeAttribute", "false")
        .option("rowTag", "note")
        .xml(getTestResourcePath(resDir + file))
      assert(df.schema === schema)
      assert(df.count() === rowsCount)
    }
  }

  test("Produces correct result for a row with a self closing tag inside") {
    val schema = buildSchema(
      field("non-empty-tag", IntegerType),
      field("self-closing-tag", IntegerType))

    val result = spark.read.option("rowTag", "ROW").schema(schema)
      .xml(getTestResourcePath(resDir + "self-closing-tag.xml"))
      .collect()

    assert(result(0) === Row(1, null))
  }

  test("DSL save with null attributes") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-complicated-null-attribute.xml"))
    books.write
      .options(Map("rootTag" -> "books", "rowTag" -> "book"))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", "book")
      .xml(copyFilePath.toString)
    assert(booksCopy.count() === books.count())
    assert(booksCopy.collect().map(_.toString).toSet === books.collect().map(_.toString).toSet)
  }

  test("DSL test nulls out invalid values when set to permissive and given explicit schema") {
    val schema = buildSchema(
      structField("integer_value",
        field("_VALUE", IntegerType),
        field("_int", IntegerType)),
      structField("long_value",
        field("_VALUE", LongType),
        field("_int", StringType)),
      field("float_value", FloatType),
      field("double_value", DoubleType),
      field("boolean_value", BooleanType),
      field("string_value"), arrayField("integer_array", IntegerType),
      field("integer_map", MapType(StringType, IntegerType)),
      field("_malformed_records", StringType))
    val results = spark.read
      .option("rowTag", "ROW")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "datatypes-valid-and-invalid.xml"))

    assert(results.schema === schema)

    val Array(valid, invalid) = results.take(2)

    assert(valid.toSeq.toArray.take(schema.length - 1) ===
      Array(Row(10, 10), Row(10, "Ten"), 10.0, 10.0, true,
        "Ten", Array(1, 2), Map("a" -> 123, "b" -> 345)))
    assert(invalid.toSeq.toArray.take(schema.length - 1) ===
      Array(null, null, null, null, null,
        "Ten", Array(2), null))

    assert(valid.toSeq.toArray.last === null)
    assert(invalid.toSeq.toArray.last.toString.contains(
      <integer_value int="Ten">Ten</integer_value>.toString))
  }

  test("empty string to null and back") {
    val fruit = spark.read
      .option("rowTag", "row")
      .option("nullValue", "")
      .xml(getTestResourcePath(resDir + "null-empty-string.xml"))
    assert(fruit.head().getAs[String]("color") === null)
  }

  test("test all string data type infer strategy") {
    val text = spark.read
      .option("rowTag", "ROW")
      .option("inferSchema", "false")
      .xml(getTestResourcePath(resDir + "textColumn.xml"))
    assert(text.head().getAs[String]("col1") === "00010")

  }

  test("test default data type infer strategy") {
    val default = spark.read
      .option("rowTag", "ROW")
      .option("inferSchema", "true")
      .xml(getTestResourcePath(resDir + "textColumn.xml"))
    assert(default.head().getAs[Int]("col1") === 10)
  }

  test("test XML with processing instruction") {
    val processingDF = spark.read
      .option("rowTag", "foo")
      .option("inferSchema", "true")
      .xml(getTestResourcePath(resDir + "processing.xml"))
    assert(processingDF.count() === 1)
  }

  test("test mixed text and element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(getTestResourcePath(resDir + "mixed_children.xml"))
    val mixedRow = mixedDF.head()
    assert(mixedRow.getAs[Row](0).toSeq === Seq(" lorem "))
    assert(mixedRow.getString(1) === " ipsum ")
  }

  test("test mixed text and complex element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(getTestResourcePath(resDir + "mixed_children_2.xml"))
    assert(mixedDF.select("foo.bar").head().getString(0) === " lorem ")
    assert(mixedDF.select("foo.baz.bing").head().getLong(0) === 2)
    assert(mixedDF.select("missing").head().getString(0) === " ipsum ")
  }

  test("test XSD validation") {
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", getTestResourcePath(resDir + "basket.xsd")
        .replace("file:/", "/"))
      .xml(getTestResourcePath(resDir + "basket.xml"))
    // Mostly checking it doesn't fail
    assert(basketDF.selectExpr("entry[0].key").head().getLong(0) === 9027)
  }

  test("test XSD validation with validation error") {
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", getTestResourcePath(resDir + "basket.xsd")
        .replace("file:/", "/"))
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(getTestResourcePath(resDir + "basket_invalid.xml")).cache()
    assert(basketDF.filter($"_malformed_records".isNotNull).count() == 1)
    assert(basketDF.filter($"_malformed_records".isNull).count() == 1)
    val rec = basketDF.select("_malformed_records").collect()(1).getString(0)
    assert(rec.startsWith("<basket>") && rec.indexOf("<extra>123</extra>") != -1 &&
      rec.endsWith("</basket>"))
  }

  test("test XSD validation with addFile() with validation error") {
    spark.sparkContext.addFile(getTestResourcePath(resDir + "basket.xsd"))
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", "basket.xsd")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(getTestResourcePath(resDir + "basket_invalid.xml")).cache()
    assert(basketDF.filter($"_malformed_records".isNotNull).count() == 1)
    assert(basketDF.filter($"_malformed_records".isNull).count() == 1)
    val rec = basketDF.select("_malformed_records").collect()(1).getString(0)
    assert(rec.startsWith("<basket>") && rec.indexOf("<extra>123</extra>") != -1 &&
      rec.endsWith("</basket>"))
  }

  test("test xmlDataset") {
    val data = Seq(
      "<ROW><year>2012</year><make>Tesla</make><model>S</model><comment>No comment</comment></ROW>",
      "<ROW><year>1997</year><make>Ford</make><model>E350</model><comment>Get one</comment></ROW>",
      "<ROW><year>2015</year><make>Chevy</make><model>Volt</model><comment>No</comment></ROW>")
    val xmlRDD = spark.sparkContext.parallelize(data)
    val ds = spark.createDataset(xmlRDD)(Encoders.STRING)
    assert(spark.read.xml(ds).collect().length === 3)
  }

  test("from_xml basic test") {
    val xmlData =
      """<parent foo="bar"><pid>14ft3</pid>
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val df = Seq((8, xmlData)).toDF("number", "payload")
    val xmlSchema = schema_of_xml(xmlData)
    val schema = buildSchema(
      field("_foo", StringType),
      field("name", StringType),
      field("pid", StringType))
    val expectedSchema = df.schema.add("decoded", schema)
    val result = df.withColumn("decoded",
      from_xml(df.col("payload"), xmlSchema, Map[String, String]().asJava))

    assert(expectedSchema === result.schema)
    assert(result.select("decoded.pid").head().getString(0) === "14ft3")
    assert(result.select("decoded._foo").head().getString(0) === "bar")
  }

  /*
  test("from_xml array basic test") {
    val xmlData =
      """<parent><pid>12345</pid><name>dave guy</name></parent>
        |<parent><pid>67890</pid><name>other guy</name></parent>""".stripMargin
    val df = Seq((8, xmlData)).toDF("number", "payload")
    val xmlSchema = ArrayType(
      StructType(
        StructField("pid", IntegerType) ::
          StructField("name", StringType) :: Nil))
    val expectedSchema = df.schema.add("decoded", xmlSchema)
    val result = df.withColumn("decoded",
      from_xml(df.col("payload"), xmlSchema))
    assert(expectedSchema === result.schema)
    // TODO: ArrayType and MapType support in from_xml
    // assert(result.selectExpr("decoded[0].pid").head().getInt(0) === 12345)
    // assert(result.selectExpr("decoded[1].pid").head().getInt(1) === 67890)
  }
  */

  test("from_xml error test") {
    // XML contains error
    val xmlData =
      """<parent foo="bar"><pid>14ft3
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val xmlSchema = schema_of_xml(xmlData)
    val result = df.withColumn("decoded",
      from_xml(df.col("payload"), xmlSchema, Map[String, String]().asJava))
    assert(result.select("decoded._corrupt_record").head().getString(0).nonEmpty)
  }

  test("from_xml with PERMISSIVE parse mode with no corrupt col schema") {
    // XML contains error
    val xmlData =
      """<parent foo="bar"><pid>14ft3
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val xmlDataNoError =
      """<parent foo="bar">
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val dfNoError = spark.createDataFrame(Seq((8, xmlDataNoError))).toDF("number", "payload")
    val xmlSchema = schema_of_xml(xmlDataNoError)
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val result = df.withColumn("decoded",
      from_xml(df.col("payload"), xmlSchema, Map[String, String]().asJava))
    assert(result.select("decoded").head().get(0) === Row(null, null))
  }

  test("from_xml to to_xml round trip") {
    val xmlData = Seq(
      "<person><age>100</age><name>Alice</name></person>",
      "<person><age>100</age><name>Alice</name></person>",
      "<person><age>100</age><name>Alice</name></person>")
    val df = xmlData.toDF("xmlString")
    val xmlSchema = schema_of_xml(xmlData.head)

    val df2 = df.withColumn("parsed",
      from_xml(df.col("xmlString"), xmlSchema))
    val df3 = df2.select(to_xml($"parsed", Map("rowTag" -> "person").asJava))
    val xmlResult = df3.collect().map(_.getString(0).replaceAll("\\s+", ""))
    assert(xmlData.sortBy(_.toString) === xmlResult.sortBy(_.toString))
  }

  test("to_xml to from_xml round trip") {
    val df = spark.read.option("rowTag", "ROW").xml(getTestResourcePath(resDir + "cars.xml"))
    val df1 = df.select(to_xml(struct("*")).as("xmlString"))
    val schema = schema_of_xml(df1.select("xmlString").head().getString(0))
    val df2 = df1.select(from_xml($"xmlString", schema).as("fromXML"))
    val df3 = df2.select(col("fromXML.*"))
    assert(df3.collect().length === 3)
    checkAnswer(df3, df)
  }

  test("decimals with scale greater than precision") {
    val spark = this.spark;
    import spark.implicits._
    val schema = buildSchema(field("Number", DecimalType(7, 4)))
    val outputDF = Seq("0.0000", "0.01")
      .map { n => s"<Row> <Number>$n</Number> </Row>" }
      .toDF("xml")
      .withColumn("parsed", from_xml($"xml", schema, Map("rowTag" -> "Row").asJava))
      .select("parsed.Number")

    val results = outputDF.collect()
    assert(results(0).getAs[java.math.BigDecimal](0).toString === "0.0000")
    assert(results(1).getAs[java.math.BigDecimal](0).toString === "0.0100")
  }

  test("double field encounters whitespace-only value") {
    val schema = buildSchema(structField("Book", field("Price", DoubleType)),
      field("_corrupt_record"))
    val whitespaceDF = spark.read
      .option("rowTag", "Books")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "whitespace_error.xml"))

    assert(whitespaceDF.count() === 1)
    assert(whitespaceDF.take(1).head.getAs[String]("_corrupt_record") !== null)
  }

  test("struct with only attributes and no value tag does not crash") {
    val schema = buildSchema(structField("book", field("_id", StringType)),
      field("_corrupt_record"))
    val booksDF = spark.read
      .option("rowTag", "book")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "books.xml"))

    assert(booksDF.count() === 12)
  }

  test("XML in String field preserves attributes") {
    val schema = buildSchema(field("ROW"))
    val result = spark.read
      .option("rowTag", "ROWSET")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "cars-attribute.xml"))
      .collect()
    assert(result.head.getString(0).contains("<comment foo=\"bar\">No</comment>"))
  }

  test("rootTag with simple attributes") {
    val xmlPath = getEmptyTempDir().resolve("simple_attributes")
    val df = spark.createDataFrame(Seq((42, "foo"))).toDF("number", "value").repartition(1)
    df.write
      .option("rowTag", "ROW")
      .option("rootTag", "root foo='bar' bing=\"baz\"")
      .option("declaration", "")
      .xml(xmlPath.toString)

    val xmlFile =
      Files.list(xmlPath).iterator.asScala.filter(_.getFileName.toString.startsWith("part-")).next()
    val firstLine = getLines(xmlFile).head
    assert(firstLine === "<root foo=\"bar\" bing=\"baz\">")
  }

  test("test ignoreNamespace") {
    val results = spark.read
      .option("rowTag", "book")
      .option("ignoreNamespace", true)
      .xml(getTestResourcePath(resDir + "books-namespaces.xml"))
    assert(results.filter("author IS NOT NULL").count() === 3)
    assert(results.filter("_id IS NOT NULL").count() === 3)
  }

  test("MapType field with attributes") {
    val schema = buildSchema(
      field("_startTime"),
      field("_interval"),
      field("PMTarget", MapType(StringType, StringType)))
    val df = spark.read.option("rowTag", "PMSetup").
      schema(schema).
      xml(getTestResourcePath(resDir + "map-attribute.xml")).
      select("PMTarget")
    val map = df.collect().head.getAs[Map[String, String]](0)
    assert(map.contains("_measurementType"))
    assert(map.contains("M1"))
    assert(map.contains("M2"))
  }

  test("StructType with missing optional StructType child") {
    val df = spark.read.option("rowTag", "Foo")
      .xml(getTestResourcePath(resDir + "struct_with_optional_child.xml"))
    val res = df.collect()
    assert(res.length == 1)
    assert(df.selectExpr("SIZE(Bar)").collect().head.getInt(0) === 2)
  }

  test("Manual schema with corrupt record field works on permissive mode failure") {
    // See issue #517
    val schema = StructType(List(
      StructField("_id", StringType),
      StructField("_space", StringType),
      StructField("c2", DoubleType),
      StructField("c3", StringType),
      StructField("c4", StringType),
      StructField("c5", StringType),
      StructField("c6", StringType),
      StructField("c7", StringType),
      StructField("c8", StringType),
      StructField("c9", DoubleType),
      StructField("c11", DoubleType),
      StructField("c20", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c46", StringType),
      StructField("c76", StringType),
      StructField("c78", StringType),
      StructField("c85", DoubleType),
      StructField("c93", StringType),
      StructField("c95", StringType),
      StructField("c99", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c100", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c108", StringType),
      StructField("c192", DoubleType),
      StructField("c193", StringType),
      StructField("c194", StringType),
      StructField("c195", StringType),
      StructField("c196", StringType),
      StructField("c197", DoubleType),
      StructField("_corrupt_record", StringType)))

    val df = spark.read
      .option("inferSchema", false)
      .option("rowTag", "row")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "manual_schema_corrupt_record.xml"))

    // Assert it works at all
    assert(df.collect().head.getAs[String]("_corrupt_record") !== null)
  }

  test("Test date parsing") {
    val schema = buildSchema(field("author"), field("date", DateType), field("date2", StringType))
    val df = spark.read
      .option("rowTag", "book")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "date.xml"))
    assert(df.collect().head.getAs[Date](1).toString === "2021-02-01")
  }

  test("Test date type inference") {
    val df = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "date.xml"))
    val expectedSchema =
      buildSchema(field("author"), field("date", DateType), field("date2", StringType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Date](1).toString === "2021-02-01")
  }

  test("Test timestamp parsing") {
    val schema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", StringType))
    val df = spark.read
      .option("rowTag", "book")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "time.xml"))
    assert(df.collect().head.getAs[Timestamp](1).getTime === 1322907330000L)
  }

  test("Test timestamp type inference") {
    val df = spark.read
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "time.xml"))
    val expectedSchema =
      buildSchema(
        field("author"),
        field("time", TimestampType),
        field("time2", StringType),
        field("time3", StringType),
        field("time4", StringType)
      )
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Timestamp](1).getTime === 1322907330000L)
  }

  test("Test dateFormat") {
    val df = spark.read
      .option("rowTag", "book")
      .option("dateFormat", "MM-dd-yyyy")
      .xml(getTestResourcePath(resDir + "date.xml"))
    val expectedSchema =
      buildSchema(field("author"), field("date", TimestampType), field("date2", DateType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Timestamp](1) === Timestamp.valueOf("2021-02-01 00:00:00"))
    assert(df.collect().head.getAs[Date](2).toString === "2021-02-01")
  }

  test("Test timestampFormat") {
    val df = spark.read
      .option("rowTag", "book")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss z")
      .xml(getTestResourcePath(resDir + "time.xml"))
    val expectedSchema =
      buildSchema(
        field("author"),
        field("time", StringType),
        field("time2", TimestampType),
        field("time3", StringType),
        field("time4", StringType)
      )
    assert(df.schema === expectedSchema)
    assert(df.collect().head.get(1) === "2011-12-03T10:15:30Z")
    assert(df.collect().head.getAs[Timestamp](2).getTime === 1322936130000L)
  }

  test("Test custom timestampFormat without timezone") {
    val xml =
      s"""<book>
         |    <author>John Smith</author>
         |    <time>2011-12-03T10:15:30Z</time>
         |    <time2>12-03-2011 10:15:30 PST</time2>
         |    <time3>2011/12/03 06:15:30</time3>
         |</book>""".stripMargin
    val input = spark.createDataset(Seq(xml))
    val df = spark.read
      .option("rowTag", "book")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .xml(input)
    val expectedSchema =
      buildSchema(
        field("author"),
        field("time", StringType),
        field("time2", StringType),
        field("time3", TimestampType)
      )
    assert(df.schema === expectedSchema)
    val res = df.collect()
    assert(res.head.get(1) === "2011-12-03T10:15:30Z")
    assert(res.head.get(2) === "12-03-2011 10:15:30 PST")
    assert(res.head.getAs[Timestamp](3) === Timestamp.valueOf("2011-12-03 06:15:30"))
  }

  test("Test custom timestampFormat with offset") {
    val df = spark.read
      .option("rowTag", "book")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss xx")
      .xml(getTestResourcePath(resDir + "time.xml"))
    val expectedSchema =
      buildSchema(
        field("author"),
        field("time", StringType),
        field("time2", StringType),
        field("time3", StringType),
        field("time4", TimestampType)
      )
    assert(df.schema === expectedSchema)
    assert(df.collect().head.get(1) === "2011-12-03T10:15:30Z")
    assert(df.collect().head.getAs[Timestamp](4).getTime === 1322892930000L)
  }

  test("Test null number type is null not 0.0") {
    val schema = buildSchema(
      structField("Header",
        field("_Name"), field("_SequenceNumber", LongType)),
      structArray("T",
        field("_Number", LongType), field("_VALUE", DoubleType), field("_Volume", DoubleType)))

    val df = spark.read.option("rowTag", "TEST")
      .option("nullValue", "")
      .schema(schema)
      .xml(getTestResourcePath(resDir + "null-numbers-2.xml"))
      .select(explode(column("T")))

    assert(df.collect()(1).getStruct(0).get(2) === null)
  }

  test("read multiple xml files in parallel") {
    val failedAgesSet = mutable.Set[Long]()
    val threads_ages = (1 to 10).map { i =>
      new Thread {
        override def run(): Unit = {
          val df = spark.read.option("rowTag", "person").format("xml")
            .load(getTestResourcePath(resDir + "ages.xml"))
          if (df.schema.fields.isEmpty) {
            failedAgesSet.add(i)
          }
        }
      }
    }

    val failedBooksSet = mutable.Set[Long]()
    val threads_books = (11 to 20).map { i =>
      new Thread {
        override def run(): Unit = {
          val df = spark.read.option("rowTag", "book").format("xml")
            .load(getTestResourcePath(resDir + "books.xml"))
          if (df.schema.fields.isEmpty) {
            failedBooksSet.add(i)
          }
        }
      }
    }

    threads_ages.foreach(_.start())
    threads_books.foreach(_.start())
    threads_ages.foreach(_.join())
    threads_books.foreach(_.join())
    assert(failedBooksSet.isEmpty)
    assert(failedAgesSet.isEmpty)
  }

  test("Issue 588: Ensure fails when data is not present, with or without schema") {
    val exception1 = intercept[AnalysisException] {
      spark.read.xml("/this/file/does/not/exist")
    }
    checkError(
      exception = exception1,
      errorClass = "PATH_NOT_FOUND",
      parameters = Map("path" -> "file:/this/file/does/not/exist")
    )
    val exception2 = intercept[AnalysisException] {
      spark.read.schema(buildSchema(field("dummy"))).xml("/this/file/does/not/exist")
    }
    checkError(
      exception = exception2,
      errorClass = "PATH_NOT_FOUND",
      parameters = Map("path" -> "file:/this/file/does/not/exist")
    )
  }

  test("Issue 614: mixed content element parsed as string in schema") {
    val textResults = spark.read
      .schema(buildSchema(field("text")))
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "mixed_children_as_string.xml"))
    val textHead = textResults.select("text").head().getString(0)
    assert(textHead.contains(
      "Lorem ipsum dolor sit amet. Ut <i>voluptas</i> distinctio et impedit deserunt"))
    assert(textHead.contains(
      "<i>numquam</i> incidunt cum autem temporibus."))

    val bookResults = spark.read
      .schema(buildSchema(field("book")))
      .option("rowTag", "books")
      .xml(getTestResourcePath(resDir + "mixed_children_as_string.xml"))
    val bookHead = bookResults.select("book").head().getString(0)
    assert(bookHead.contains(
      "Lorem ipsum dolor sit amet. Ut <i>voluptas</i> distinctio et impedit deserunt"))
    assert(bookHead.contains(
      "<i>numquam</i> incidunt cum autem temporibus."))
  }

  private def getLines(path: Path): Seq[String] = {
    val source = Source.fromFile(path.toFile)
    try {
      source.getLines().toList
    } finally {
      source.close()
    }
  }

  test("read utf-8 encoded file") {
    val df = spark.read
      .option("charset", StandardCharsets.UTF_8.name)
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books.xml"))
    assert(df.collect().length === 12)
  }

  test("read file with unicode chars in row tag name") {
    val df = spark.read
      .option("charset", StandardCharsets.UTF_8.name)
      .option("rowTag", "\u66F8") // scalastyle:ignore
      .xml(getTestResourcePath(resDir + "books-unicode-in-tag-name.xml"))
    assert(df.collect().length === 3)
  }

  test("read utf-8 encoded file with empty tag 2") {
    val df = spark.read
      .option("charset", StandardCharsets.UTF_8.name)
      .option("rowTag", "House")
      .xml(getTestResourcePath(resDir + "fias_house.xml"))
    assert(df.collect().length === 37)
  }

  test("SPARK-45488: root-level value tag for attributes-only object") {
    val schema = buildSchema(field("_attr"), field("_VALUE"))
    val results = Seq(
      // user specified schema
      spark.read
        .option("rowTag", "ROW")
        .schema(schema)
        .xml(getTestResourcePath(resDir + "root-level-value.xml")).collect(),
      // schema inference
      spark.read
        .option("rowTag", "ROW")
        .xml(getTestResourcePath(resDir + "root-level-value.xml")).collect())
    results.foreach { result =>
      assert(result.length === 3)
      assert(result(0).getAs[String]("_VALUE") == "value1")
      assert(result(1).getAs[String]("_attr") == "attr1"
        && result(1).getAs[String]("_VALUE") == "value2")
      // comments aren't included in valueTag
      assert(result(2).getAs[String]("_VALUE") == "\n        value3\n        ")
    }
  }

  test("SPARK-45488: root-level value tag for not attributes-only object") {
    val ATTRIBUTE_NAME = "_attr"
    val TAG_NAME = "tag"
    val VALUETAG_NAME = "_VALUE"
    val schema = buildSchema(
      field(ATTRIBUTE_NAME),
      field(TAG_NAME, LongType),
      field(VALUETAG_NAME))
    val dfs = Seq(
      // user specified schema
      spark.read
        .option("rowTag", "ROW")
        .schema(schema)
        .xml(getTestResourcePath(resDir + "root-level-value-none.xml")),
      // schema inference
      spark.read
        .option("rowTag", "ROW")
        .xml(getTestResourcePath(resDir + "root-level-value-none.xml"))
    )
    dfs.foreach { df =>
      val result = df.collect()
      assert(result.length === 5)
      assert(result(0).get(0) == null && result(0).get(1) == null)
      assert(
        result(1).getAs[String](ATTRIBUTE_NAME) == "attr1"
          && result(1).getAs[Any](TAG_NAME) == null
      )
      assert(
        result(2).getAs[Long](TAG_NAME) == 5L
          && result(2).getAs[Any](ATTRIBUTE_NAME) == null
      )
      assert(
        result(3).getAs[Long](TAG_NAME) == 6L
          && result(3).getAs[Any](ATTRIBUTE_NAME) == null
      )
      assert(
        result(4).getAs[String](ATTRIBUTE_NAME) == "8"
          && result(4).getAs[Any](TAG_NAME) == null
      )
    }
  }

  test("SPARK-45488: root-level value tag for attributes-only object - from xml") {
    val xmlData = """<ROW attr="attr1">123456</ROW>"""
    val df = Seq((1, xmlData)).toDF("number", "payload")
    val xmlSchema = schema_of_xml(xmlData)
    val schema = buildSchema(
      field("_VALUE", LongType),
      field("_attr"))
    val expectedSchema = df.schema.add("decoded", schema)
    val result = df.withColumn("decoded",
      from_xml(df.col("payload"), xmlSchema, Map[String, String]().asJava))
    assert(expectedSchema == result.schema)
    assert(result.select("decoded._VALUE").head().getLong(0) === 123456L)
    assert(result.select("decoded._attr").head().getString(0) === "attr1")
  }

  test("Test XML Options Error Messages") {
    def checkXmlOptionErrorMessage(
      parameters: Map[String, String] = Map.empty,
      msg: String,
      exception: Throwable = new IllegalArgumentException().getCause): Unit = {
      val e = intercept[Exception] {
        spark.read
          .options(parameters)
          .xml(getTestResourcePath(resDir + "ages.xml"))
          .collect()
      }
      assert(e.getCause === exception)
      assert(e.getMessage.contains(msg))
    }

    checkXmlOptionErrorMessage(Map.empty,
      "[XML_ROW_TAG_MISSING] `rowTag` option is required for reading files in XML format.",
      QueryCompilationErrors.xmlRowTagRequiredError(XmlOptions.ROW_TAG).getCause)
    checkXmlOptionErrorMessage(Map("rowTag" -> ""),
      "'rowTag' option should not be an empty string.")
    checkXmlOptionErrorMessage(Map("rowTag" -> " "),
      "'rowTag' option should not be an empty string.")
    checkXmlOptionErrorMessage(Map("rowTag" -> "person",
      "declaration" -> s"<${XmlOptions.DEFAULT_DECLARATION}>"),
      "'declaration' should not include angle brackets")
  }

  def dataTypeTest(data: String,
                   dt: DataType): Unit = {
    val xmlString = s"""<ROW>$data</ROW>"""
    val schema = new StructType().add(XmlOptions.VALUE_TAG, dt)
    val df = spark.read
      .option("rowTag", "ROW")
      .schema(schema)
      .xml(spark.createDataset(Seq(xmlString)))
  }

  test("Primitive field casting") {
    val ts = Seq("2002-05-30 21:46:54", "2002-05-30T21:46:54", "2002-05-30T21:46:54.1234",
      "2002-05-30T21:46:54Z", "2002-05-30T21:46:54.1234Z", "2002-05-30T21:46:54-06:00",
      "2002-05-30T21:46:54+06:00", "2002-05-30T21:46:54.1234-06:00",
      "2002-05-30T21:46:54.1234+06:00", "2002-05-30T21:46:54+00:00", "2002-05-30T21:46:54.0000Z")

    val tsXMLStr = ts.map(t => s"<TimestampType>$t</TimestampType>").mkString("\n")
    val tsResult = ts.map(t =>
      Timestamp.from(Instant.ofEpochSecond(0, DateTimeUtils.stringToTimestamp(
        UTF8String.fromString(t), DateTimeUtils.getZoneId(conf.sessionLocalTimeZone)).get * 1000))
    )

    val primitiveFieldAndType: Dataset[String] =
      spark.createDataset(spark.sparkContext.parallelize(
        s"""<ROW>
          <decimal>10.05</decimal>
          <decimal>1,000.01</decimal>
          <decimal>158,058,049.001</decimal>
          <emptyString></emptyString>
          <ByteType>10</ByteType>
          <ShortType>10</ShortType>
          <ShortType>+10</ShortType>
          <ShortType>-10</ShortType>
          <IntegerType>10</IntegerType>
          <IntegerType>+10</IntegerType>
          <IntegerType>-10</IntegerType>
          <LongType>10</LongType>
          <LongType>+10</LongType>
          <LongType>-10</LongType>
          <FloatType>1.00</FloatType>
          <FloatType>+1.00</FloatType>
          <FloatType>-1.00</FloatType>
          <DoubleType>1.00</DoubleType>
          <DoubleType>+1.00</DoubleType>
          <DoubleType>-1.00</DoubleType>
          <BooleanType>true</BooleanType>
          <BooleanType>1</BooleanType>
          <BooleanType>false</BooleanType>
          <BooleanType>0</BooleanType>
          $tsXMLStr
          <DateType>2002-09-24</DateType>
        </ROW>""".stripMargin :: Nil))(Encoders.STRING)

    val decimalType = DecimalType(20, 3)

    val schema = StructType(
      StructField("decimal", ArrayType(decimalType), true) ::
        StructField("emptyString", StringType, true) ::
        StructField("ByteType", ByteType, true) ::
        StructField("ShortType", ArrayType(ShortType), true) ::
        StructField("IntegerType", ArrayType(IntegerType), true) ::
        StructField("LongType", ArrayType(LongType), true) ::
        StructField("FloatType", ArrayType(FloatType), true) ::
        StructField("DoubleType", ArrayType(DoubleType), true) ::
        StructField("BooleanType", ArrayType(BooleanType), true) ::
        StructField("TimestampType", ArrayType(TimestampType), true) ::
        StructField("DateType", DateType, true) :: Nil)

    val df = spark.read.schema(schema).xml(primitiveFieldAndType)

    checkAnswer(
      df,
      Seq(Row(Array(
        Decimal(BigDecimal("10.05"), decimalType.precision, decimalType.scale).toJavaBigDecimal,
        Decimal(BigDecimal("1000.01"), decimalType.precision, decimalType.scale).toJavaBigDecimal,
        Decimal(BigDecimal("158058049.001"), decimalType.precision, decimalType.scale)
          .toJavaBigDecimal),
        "",
        10.toByte,
        Array(10.toShort, 10.toShort, -10.toShort),
        Array(10, 10, -10),
        Array(10L, 10L, -10L),
        Array(1.0.toFloat, 1.0.toFloat, -1.0.toFloat),
        Array(1.0, 1.0, -1.0),
        Array(true, true, false, false),
        tsResult,
        Date.valueOf("2002-09-24")
      ))
    )
  }

  test("Nullable types are handled") {
    val dataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      BooleanType, TimestampType, DateType, StringType)

    val dataXMLString = dataTypes.map { dt =>
      s"""<${dt.toString}>-</${dt.toString}>"""
    }.mkString("\n")

    val fields = dataTypes.map { dt =>
      StructField(dt.toString, dt, true)
    }
    val schema = StructType(fields)

    val res = dataTypes.map { dt => null }

    val nullDataset: Dataset[String] =
      spark.createDataset(spark.sparkContext.parallelize(
        s"""<ROW>
          $dataXMLString
        </ROW>""".stripMargin :: Nil))(Encoders.STRING)

    val df = spark.read.option("nullValue", "-").schema(schema).xml(nullDataset)
    checkAnswer(df, Row.fromSeq(res))

    val df2 = spark.read.xml(nullDataset)
    checkAnswer(df2, Row.fromSeq(dataTypes.map { dt => "-" }))
  }

  test("Custom timestamp format is used to parse correctly") {
    val schema = StructType(
      StructField("ts", TimestampType, true) :: Nil)

    Seq(
      ("12-03-2011 10:15:30", "2011-12-03 10:15:30", "MM-dd-yyyy HH:mm:ss", "UTC"),
      ("2011/12/03 10:15:30", "2011-12-03 10:15:30", "yyyy/MM/dd HH:mm:ss", "UTC"),
      ("2011/12/03 10:15:30", "2011-12-03 10:15:30", "yyyy/MM/dd HH:mm:ss", "Asia/Shanghai")
    ).foreach { case (ts, resTS, fmt, zone) =>
      val tsDataset: Dataset[String] =
        spark.createDataset(spark.sparkContext.parallelize(
          s"""<ROW>
          <ts>$ts</ts>
        </ROW>""".stripMargin :: Nil))(Encoders.STRING)
      val timestampResult = Timestamp.from(Instant.ofEpochSecond(0,
        DateTimeUtils.stringToTimestamp(UTF8String.fromString(resTS),
          DateTimeUtils.getZoneId(zone)).get * 1000))

      val df = spark.read.option("timestampFormat", fmt).option("timezone", zone)
        .schema(schema).xml(tsDataset)
      checkAnswer(df, Row(timestampResult))
    }
  }

  test("Schema Inference for primitive types") {
    val dataset: Dataset[String] =
      spark.createDataset(spark.sparkContext.parallelize(
        s"""<ROW>
          <bool1>true</bool1>
          <double1>+10.1</double1>
          <long1>-10</long1>
          <long2>10</long2>
          <string1>8E9D</string1>
          <string2>8E9F</string2>
          <ts1>2015-01-01 00:00:00</ts1>
        </ROW>""".stripMargin :: Nil))(Encoders.STRING)

    val expectedSchema = StructType(StructField("bool1", BooleanType, true) ::
      StructField("double1", DoubleType, true) ::
      StructField("long1", LongType, true) ::
      StructField("long2", LongType, true) ::
      StructField("string1", StringType, true) ::
      StructField("string2", StringType, true) ::
      StructField("ts1", TimestampType, true) :: Nil)

    val df = spark.read.xml(dataset)
    assert(df.schema.toSet === expectedSchema.toSet)
    checkAnswer(df, Row(true, 10.1, -10, 10, "8E9D", "8E9F",
      Timestamp.valueOf("2015-01-01 00:00:00")))
  }

  test("case sensitivity test - attributes-only object") {
    val schemaCaseSensitive = new StructType()
      .add("array", ArrayType(
        new StructType()
          .add("_Attr2", LongType)
          .add("_VALUE", LongType)
          .add("_aTTr2", LongType)
          .add("_attr2", LongType)))
      .add("struct", new StructType()
        .add("_Attr1", LongType)
        .add("_VALUE", LongType)
        .add("_attr1", LongType))

    val dfCaseSensitive = Seq(
      Row(
        Array(
          Row(null, 2, null, 2),
          Row(3, 3, null, null),
          Row(null, 4, 4, null)),
        Row(null, 1, 1)
      ),
      Row(
        null,
        Row(5, 5, null)
      )
    )
    val schemaCaseInSensitive = new StructType()
      .add("array", ArrayType(new StructType().add("_VALUE", LongType).add("_attr2", LongType)))
      .add("struct", new StructType().add("_VALUE", LongType).add("_attr1", LongType))
    val dfCaseInsensitive =
      Seq(
        Row(
          Array(Row(2, 2), Row(3, 3), Row(4, 4)),
          Row(1, 1)),
        Row(null, Row(5, 5)))
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val df = spark.read
          .option("rowTag", "ROW")
          .xml(getTestResourcePath(resDir + "attributes-case-sensitive.xml"))
        assert(df.schema == (if (caseSensitive) schemaCaseSensitive else schemaCaseInSensitive))
        checkAnswer(
          df,
          if (caseSensitive) dfCaseSensitive else dfCaseInsensitive)
      }
    }
  }

  testCaseSensitivity(
    "basic",
    writeData = Seq(Row(1L, null), Row(null, 2L)),
    writeSchema = new StructType()
      .add("A1", LongType)
      .add("a1", LongType),
    expectedSchema = new StructType()
      .add("A1", LongType),
    readDataCaseInsensitive = Seq(Row(1L), Row(2L)))

  testCaseSensitivity(
    "nested struct",
    writeData = Seq(Row(Row(1L), null), Row(null, Row(2L))),
    writeSchema = new StructType()
      .add("A1", new StructType().add("B1", LongType))
      .add("a1", new StructType().add("b1", LongType)),
    expectedSchema = new StructType()
      .add("A1", new StructType().add("B1", LongType)),
    readDataCaseInsensitive = Seq(Row(Row(1L)), Row(Row(2L)))
  )

  testCaseSensitivity(
    "convert fields into array",
    writeData = Seq(Row(1L, 2L)),
    writeSchema = new StructType()
      .add("A1", LongType)
      .add("a1", LongType),
    expectedSchema = new StructType()
      .add("A1", ArrayType(LongType)),
    readDataCaseInsensitive = Seq(Row(Array(1L, 2L))))

  testCaseSensitivity(
    "basic array",
    writeData = Seq(Row(Array(1L, 2L), Array(3L, 4L))),
    writeSchema = new StructType()
      .add("A1", ArrayType(LongType))
      .add("a1", ArrayType(LongType)),
    expectedSchema = new StructType()
      .add("A1", ArrayType(LongType)),
    readDataCaseInsensitive = Seq(Row(Array(1L, 2L, 3L, 4L))))

  testCaseSensitivity(
    "nested array",
    writeData =
      Seq(Row(Array(Row(1L, 2L), Row(3L, 4L)), null), Row(null, Array(Row(5L, 6L), Row(7L, 8L)))),
    writeSchema = new StructType()
      .add("A1", ArrayType(new StructType().add("B1", LongType).add("d", LongType)))
      .add("a1", ArrayType(new StructType().add("b1", LongType).add("c", LongType))),
    expectedSchema = new StructType()
      .add(
        "A1",
        ArrayType(
          new StructType()
            .add("B1", LongType)
            .add("c", LongType)
            .add("d", LongType))),
    readDataCaseInsensitive = Seq(
      Row(Array(Row(1L, null, 2L), Row(3L, null, 4L))),
      Row(Array(Row(5L, 6L, null), Row(7L, 8L, null)))))

  def testCaseSensitivity(
                           name: String,
                           writeData: Seq[Row],
                           writeSchema: StructType,
                           expectedSchema: StructType,
                           readDataCaseInsensitive: Seq[Row]): Unit = {
    test(s"case sensitivity test - $name") {
      withTempDir { dir =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          spark
            .createDataFrame(writeData.asJava, writeSchema)
            .repartition(1)
            .write
            .option("rowTag", "ROW")
            .format("xml")
            .mode("overwrite")
            .save(dir.getCanonicalPath)
        }

        Seq(true, false).foreach { caseSensitive =>
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
            val df = spark.read.option("rowTag", "ROW").xml(dir.getCanonicalPath)
            assert(df.schema == (if (caseSensitive) writeSchema else expectedSchema))
            checkAnswer(df, if (caseSensitive) writeData else readDataCaseInsensitive)
          }
        }
      }
    }
  }

  test("keepInnerXmlAsRaw: xml with only rowTag(TEAMS)") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml1.xml"))

    // <TEAMS Create="03.12.2020 16:09:21" Count="300"></TEAMS>

    val schema = xmlLoad.schema
    assert(2 === schema.size)
    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
    }
  }


  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and inner <PERSON>tag. Nested tag's depth is 1") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml2.xml"))

    // <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //  <PERSON Name="a" Surname="b" SecId="1"></PERSON>
    // </TEAMS>

    val schema = xmlLoad.schema
    assert(3 === schema.size)
    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")
    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"></PERSON>"
        === elem.get(personIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and " +
    "nested tags (<PERSON> <TASK> </TASK> </PERSON>). Nested tag's depth is 2") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml3.xml"))

    //    <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //      <PERSON Name="a" Surname="b" SecId="1">
    //        <TASK Name="x" Duration="100"></TASK>
    //      </PERSON>
    //    </TEAMS>

    val schema = xmlLoad.schema
    assert(3 === schema.size)
    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")

    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")

    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"><TASK Name=\"x\" " +
        "Duration=\"100\"></TASK></PERSON>"
        === elem.get(personIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and " +
    "nested tags (<PERSON> <TASK> </TASK> </PERSON>). Nested tag's depth is 4") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml5.xml"))

    //    <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //      <PERSON Name="a" Surname="b" SecId="1">
    //        <TASK Name="x" Duration="100">
    //          <COMMENT Desc="TEST">
    //            <DESC Assignee="x"></DESC>
    //          </COMMENT>
    //        </TASK>
    //      </PERSON>
    //    </TEAMS>

    val schema = xmlLoad.schema
    assert(3 === schema.size)
    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")


    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")

    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"> " +
        "<TASK Name=\"x\" Duration=\"100\"> " +
        "<COMMENT Desc=\"TEST\"> " +
        "<DESC Assignee=\"x\"></DESC> " +
        "</COMMENT> " +
        "</TASK> </PERSON>"
        !== elem.get(personIndexAsOption.get))
      assert("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"><TASK Name=\"x\" Duration=\"100\">" +
        "<COMMENT Desc=\"TEST\">" +
        "<DESC Assignee=\"x\">" +
        "</DESC>" +
        "</COMMENT>" +
        "</TASK>" +
        "</PERSON>"
        === elem.get(personIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and " +
    "nested tags with the same name as the rowTag " +
    "(<TASK> <TASK> </TASK> </TASK>). Nested tag's depth is 2") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml8.xml"))

    //    <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //      <TEAMS Duration="400">
    //        <TEAMS Name="400"></TEAMS>
    //      </TEAMS>
    //    </TEAMS>

    val schema = xmlLoad.schema
    assert(3 === schema.size)

    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")
    val teamsIndexAsOption = getFieldIndexAsOption(schema, "TEAMS")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === teamsIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(teamsIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<TEAMS Duration=\"400\"><TEAMS Name=\"400\"></TEAMS></TEAMS>"
        === elem.get(teamsIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and " +
    "multiple nested tags (<PERSON> <TASK> </TASK> </PERSON> " +
    "<MANAGER> <SECTION> </SECTION> </MANAGER> " +
    "<DIRECTOR> <Directorate> </Directorate></DIRECTOR>). Nested tag's depth is 2") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml9.xml"))

    //    <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //      <PERSON Name="a" Surname="b" SecId="1">
    //        <TASK Name="x" Duration="100"></TASK>
    //      </PERSON>
    //      <MANAGER Name="a" Surname="b" SecId="1" priority="high">
    //        <SECTION Name="a" ID="5"></SECTION>
    //      </MANAGER>
    //      <DIRECTOR Name="a" Surname="b" SecId="1" priority="high">
    //        <Directorate Name="b" count="12"></Directorate>
    //      </DIRECTOR>
    //    </TEAMS>

    val schema = xmlLoad.schema
    assert(5 === schema.size)

    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")
    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")
    val managerIndexAsOption = getFieldIndexAsOption(schema, "MANAGER")
    val directorIndexAsOption = getFieldIndexAsOption(schema, "DIRECTOR")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)
    assert(true === managerIndexAsOption.nonEmpty)
    assert(true === directorIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(managerIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(directorIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"><TASK Name=\"x\" " +
        "Duration=\"100\"></TASK></PERSON>"
        === elem.get(personIndexAsOption.get))
      assert("<MANAGER Name=\"a\" Surname=\"b\" SecId=\"1\" priority=\"high\">" +
        "<SECTION Name=\"a\" ID=\"5\"></SECTION></MANAGER>"
        === elem.get(managerIndexAsOption.get))
      assert("<DIRECTOR Name=\"a\" Surname=\"b\" SecId=\"1\" priority=\"high\">" +
        "<Directorate Name=\"b\" count=\"12\"></Directorate></DIRECTOR>"
        === elem.get(directorIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and " +
    "multiple nested tags with same name (<PERSON> <PERSON> <PERSON> " +
    "</PERSON></PERSON> </PERSON> ). Nested tag's depth is 3") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml10.xml"))

    //    <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //      <PERSON Duration="400">
    //        <PERSON Name="a">
    //          <PERSON Surname="b"></PERSON>
    //        </PERSON>
    //      </PERSON>
    //    </TEAMS>

    val schema = xmlLoad.schema
    assert(3 === schema.size)
    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")
    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get).dataType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      assert("<PERSON Duration=\"400\"><PERSON Name=\"a\"><PERSON Surname=\"b\"></PERSON>" +
        "</PERSON></PERSON>"
        === elem.get(personIndexAsOption.get))
    }
  }

  test("keepInnerXmlAsRaw: Comparing the results of " +
    "keepInnerXmlAsRaw option and xml source load. Nested tag's depth is 3") {


    //      <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //        <PERSON Name="a" Surname="b" SecId="1">
    //          <TASK Name="x" Duration="100">
    //            <COMMENT Desc="TEST"></COMMENT>
    //          </TASK>
    //        </PERSON>
    //      </TEAMS>


    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml11.xml")
    // 0th depth
    val xmlWithKeepInnerXmlAsRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")
      .xml(filePath)

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    val persisted = xmlWithKeepInnerXmlAsRaw.persist()

    compareOriginalAndRawXmlDataFrames(xmlTeams, persisted, List("PERSON"))


    // 1st depth
    val personFromRaw = persisted.select("PERSON")
    val personDF = applyMapFunctionToDataFrame(personFromRaw, "PERSON")
    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)
    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List("TASK"))

    // 2nd depth
    val taskFromRaw = personDF.select("TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    val taskDF = applyMapFunctionToDataFrame(taskFromRaw, "TASK")

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List("COMMENT"))

    // 3rd depth

    val commentFromRaw = taskDF.select("COMMENT")
    val commentDF = applyMapFunctionToDataFrame(commentFromRaw, "COMMENT")
    val xmlComment = spark.read
      .option("rowTag", "COMMENT")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlComment, commentDF, List.empty[String])
    persisted.unpersist()
  }

  test("keepInnerXmlAsRaw: Test for malformed xml. Missing end tag") {
    val exception = intercept[SparkException] {
      val xmlLoad = spark.read
        .option("rowTag", "TEAMS")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("keepInnerXmlAsRaw", "true")
        .option("mode", FailFastMode.name)
        .xml(getTestResourcePath(resDir + keepInnerXmlDir + "single/malformedxml1.xml")).count()
    }

    checkError(
      exception = exception.getCause.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[empty row]",
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and inner Array<Person> Tag") {
    val xmlLoad = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml1.xml"))

    val schema = xmlLoad.schema

    assert(3 === schema.size)

    val countIndexAsOption = getFieldIndexAsOption(schema, "_Count")
    val createIndexAsOption = getFieldIndexAsOption(schema, "_Create")
    val personIndexAsOption = getFieldIndexAsOption(schema, "PERSON")

    assert(true === countIndexAsOption.nonEmpty)
    assert(true === createIndexAsOption.nonEmpty)
    assert(true === personIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === schema.fields(countIndexAsOption.get).dataType)
    assert(DataTypes.StringType === schema.fields(createIndexAsOption.get).dataType)
    assert(true === schema.fields(personIndexAsOption.get).dataType.isInstanceOf[ArrayType])
    assert(DataTypes.StringType === schema.fields(personIndexAsOption.get)
      .dataType.asInstanceOf[ArrayType].elementType)
    val rows = xmlLoad.collect()
    assert(1 === rows.length)
    for (elem <- rows) {
      assert(300 === elem.get(countIndexAsOption.get))
      assert("03.12.2020 16:09:21" === elem.get(createIndexAsOption.get))
      val arr = elem.get(personIndexAsOption.get).asInstanceOf[mutable.ArraySeq[String]]
      assert(arr.contains("<PERSON Name=\"a\" Surname=\"b\" SecId=\"1\"></PERSON>"))
      assert(arr.contains("<PERSON Name=\"c\" Surname=\"d\" SecId=\"2\"></PERSON>"))
    }

    val personDF = xmlLoad.select("PERSON")
    var structType = new StructType
    structType = structType.add(StructField("PERSON", DataTypes.StringType, true, Metadata.empty))

    val encoder = Encoders.row(structType)
    val personFlatMap = personDF.flatMap(row => row.getSeq[AnyRef](0)
      .map(r => {
        RowFactory.create(r)
      }))(encoder)

    val options = new XmlOptions(Map("nullValue" -> "", "inferSchema" -> "true"
      , "rowTag" -> "PERSON", "keepInnerXmlAsRaw" -> "true"))

    val xmlInferSchema = new XmlInferSchema(options, spark.sessionState.conf.caseSensitiveAnalysis)
    val inferredSchema = xmlInferSchema.infer(personFlatMap.as(Encoders.STRING).rdd)


    val personFinal = applyMapFunctionToDataFrame(personFlatMap, "PERSON")

    assert(3 === personFinal.schema.size)

    val nameIndexAsOption = getFieldIndexAsOption(inferredSchema, "_Name")
    val secIdIndexAsOption = getFieldIndexAsOption(inferredSchema, "_SecId")
    val surnameIndexAsOption = getFieldIndexAsOption(inferredSchema, "_Surname")

    assert(true === nameIndexAsOption.nonEmpty)
    assert(true === secIdIndexAsOption.nonEmpty)
    assert(true === surnameIndexAsOption.nonEmpty)

    assert(DataTypes.LongType === inferredSchema.fields(secIdIndexAsOption.get).dataType)
    assert(DataTypes.StringType === inferredSchema.fields(nameIndexAsOption.get).dataType)
    assert(DataTypes.StringType === inferredSchema.fields(surnameIndexAsOption.get).dataType)

    val sortedDf = personFinal.orderBy(("_SecId")).collect()
    assert(2 === sortedDf.length)

    val range = Range(0, 2)
    assert(1 === range.max)
    assert(0 === range.min)
    for (i <- range) {
      val row = sortedDf(i)
      if (i == 0) {

        assert(1 === row.get(secIdIndexAsOption.get))
        assert("a" === row.get(nameIndexAsOption.get))
        assert("b" === row.get(surnameIndexAsOption.get))
      } else {
        assert(2 === row.get(secIdIndexAsOption.get))
        assert("c" === row.get(nameIndexAsOption.get))
        assert("d" === row.get(surnameIndexAsOption.get))
      }
    }
  }

  test("keepInnerXmlAsRaw: xml with rowTag(TEAMS) and inner Array<Person> Tag. Compare " +
    "with keepInnerXmlAsRaw option is false") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml1.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)

    val xmlTeam = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    // 0th depth
    compareOriginalAndRawXmlDataFrames(xmlTeam, xmlRaw, List("PERSON"))

    // 1st depth

    val personDFAsRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personDFAsRaw, "PERSON")

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List.empty[String])
  }

  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 2. Array<Person> on 1st depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml2.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)

    // 1st depth
    val personDFAsRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personDFAsRaw, "PERSON")
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List("TASK"))

    // 2nd depth
    val taskDFAsRaw = personDF.select("TASK")
    val taskDF = applyMapFunctionToDataFrame(taskDFAsRaw, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List.empty[String])

  }

  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 2. Array<TASK> on 2nd depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml3.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)

    // 1st depth
    val personDFAsRaw = xmlRaw.select("PERSON")
    val personDF = applyMapFunctionToDataFrame(personDFAsRaw, "PERSON")

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List("TASK"))

    // 2nd depth
    val taskDFAsRaw = personDF.select("TASK")
    val taskFlatMap = applyFlatMapToDataFrame(taskDFAsRaw, "TASK")
    val taskDF = applyMapFunctionToDataFrame(taskFlatMap, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List.empty[String])
  }

  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 0. Array<TEAMS> on 0th depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml4.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)
    // 0th depth

    val xmlPerson = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, xmlRaw, List.empty[String])
  }

  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 4. Array<PERSON> on 1st depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml6.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)
    // 0th depth

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw, List("PERSON"))

    // 1st depth

    val personRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personRaw, "PERSON")
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")
    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF,
      List("TASK"))

    // 2nd depth

    val taskRaw = personDF.select("TASK")
    val taskDF = applyMapFunctionToDataFrame(taskRaw, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF,
      List("COMMENT"))

    // 3rd depth

    val commentRaw = taskDF.select("COMMENT")
    val commentDF = applyMapFunctionToDataFrame(commentRaw, "COMMENT")
    val xmlComment = spark.read
      .option("rowTag", "COMMENT")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlComment, commentDF,
      List("DESC"))

    // 4th depth

    val descRaw = commentDF.select("DESC")
    val descDF = applyMapFunctionToDataFrame(descRaw, "DESC")

    val xmlDesc = spark.read
      .option("rowTag", "DESC")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)
    compareOriginalAndRawXmlDataFrames(xmlDesc, descDF,
      List.empty[String])
  }


  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 2. Array<PERSON> on 1st depth and Array<TASK> on 2nd depth ") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml7.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)
    // 0th depth

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw, List("PERSON"))

    // 1st depth

    val personRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personRaw, "PERSON")
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")
    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF,
      List("TASK"))

    // 2nd depth

    val taskRaw = personDF.select("TASK")
    val taskFlatMap = applyFlatMapToDataFrame(taskRaw, "TASK")
    val taskDF = applyMapFunctionToDataFrame(taskFlatMap, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF,
      List.empty[String])
  }


  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 2 with different tags. " +
    "Array<PERSON>, Array<MANAGER>, Array<DIRECTOR> " +
    "and company tags on 1st depth ") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml8.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)
    // 0th depth

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw,
      List("PERSON", "MANAGER", "DIRECTOR", "company"))

    // 1st depth

    val personRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personRaw, "PERSON")
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")
    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF,
      List("TASK"))

    // 2nd depth

    val taskRaw = personDF.select("TASK")
    val taskDF = applyMapFunctionToDataFrame(taskRaw, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF,
      List.empty[String])


    // 1st depth
    val managerRaw = xmlRaw.select("MANAGER")
    val managerFlatMap = applyFlatMapToDataFrame(managerRaw, "MANAGER")
    val managerDF = applyMapFunctionToDataFrame(managerFlatMap, "MANAGER")

    val xmlManager = spark.read
      .option("rowTag", "MANAGER")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlManager, managerDF, List("SECTION"))

    // 2nd depth

    val sectionRaw = managerDF.select("SECTION")
    val sectionDF = applyMapFunctionToDataFrame(sectionRaw, "SECTION")

    val xmlSection = spark.read
      .option("rowTag", "SECTION")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlSection, sectionDF, List.empty[String])

    // 1st depth

    val directorRaw = xmlRaw.select("DIRECTOR")
    val directorFlatMap = applyFlatMapToDataFrame(directorRaw, "DIRECTOR")
    val directorDF = applyMapFunctionToDataFrame(directorFlatMap, "DIRECTOR")

    val xmlDirector = spark.read
      .option("rowTag", "DIRECTOR")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlDirector, directorDF, List("Directorate"))

    // 2nd depth

    val directorateRaw = directorDF.select("Directorate")
    val directorateDF = applyMapFunctionToDataFrame(directorateRaw, "Directorate")

    val xmlDirectorate = spark.read
      .option("rowTag", "Directorate")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlDirectorate, directorateDF, List.empty[String])

    // 1st depth

    val companyRaw = xmlRaw.select("company")
    val companyDF = applyMapFunctionToDataFrame(companyRaw, "company")


    val xmlCompany = spark.read
      .option("rowTag", "company")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlCompany, companyDF, List.empty[String])

  }


  test("keepInnerXmlAsRaw: Comparing the results of keepInnerXmlAsRaw option " +
    "and xml source load. " +
    "Nested tag's depth is 4. " +
    "Array<PERSON> on 1st depth, Array<TASK> on 2nd, <Comment> on 3rd depth " +
    "and Array<desc> on 4th depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml9.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)
    // 0th depth

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw,
      List("PERSON"))

    // 1st depth

    val personRaw = xmlRaw.select("PERSON")
    val personFlatMap = applyFlatMapToDataFrame(personRaw, "PERSON")
    val personDF = applyMapFunctionToDataFrame(personFlatMap, "PERSON")
    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF,
      List("TASK"))

    // 2nd depth

    val taskRaw = personDF.select("TASK")
    val taskFlatMap = applyFlatMapToDataFrame(taskRaw, "TASK")
    val taskDF = applyMapFunctionToDataFrame(taskFlatMap, "TASK")
    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF,
      List("Comment"))


    // 3st depth
    val commentRaw = taskDF.select("Comment")
    val commentDF = applyMapFunctionToDataFrame(commentRaw, "Comment")

    val xmlComment = spark.read
      .option("rowTag", "Comment")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlComment, commentDF, List("desc"))

    // 4th depth

    val descRaw = commentDF.select("desc")
    val descFlatMap = applyFlatMapToDataFrame(descRaw, "desc")
    val descDF = applyMapFunctionToDataFrame(descFlatMap, "desc")

    val xmlDesc = spark.read
      .option("rowTag", "desc")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlDesc, descDF, List.empty[String])

  }


  test("keepInnerXmlAsRaw: Malformed xml. Nested tag's depth is 4. " +
    "Array<PERSON> on 1st depth, Array<TASK> on 2nd, <Comment> on 3rd depth " +
    "and Array<desc> on 4th depth") {
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml10.xml")
    val exception = intercept[SparkException] {
      val xmlLoad = spark.read
        .option("rowTag", "TEAMS")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("keepInnerXmlAsRaw", "true")
        .option("mode", FailFastMode.name)
        .xml(filePath).count()
    }

    checkError(
      exception = exception.getCause.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[empty row]",
        "failFastMode" -> FailFastMode.name)
    )

  }

  test("keepInnerXmlAsRaw: Xml with depth 2. End element as /> " +
    "without element name's itself on depth 2.") {
    //    <?xml version="1.0" encoding="UTF-8"?>
    //      <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //        <PERSON Name="a" Surname="b" SecId="1">
    //          <TASK Name="x" Duration="100"/>
    //        </PERSON>
    //      </TEAMS>

    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "single/testxml12.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    // 0th depth
    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw, List("PERSON"))

    val personRaw = xmlRaw.select("PERSON")
    val personDF = applyMapFunctionToDataFrame(personRaw, "PERSON")

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    // 1st depth
    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List("TASK"));

    val taskRaw = personDF.select("TASK")
    val taskDF = applyMapFunctionToDataFrame(taskRaw, "TASK")

    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List.empty[String])

  }


  test("keepInnerXmlAsRaw: Xml with depth 2. End element as /> " +
    "without element name's itself on depth 2. Array<TASK> on 2nd depth") {
    //    <?xml version="1.0" encoding="UTF-8"?>
    //      <TEAMS Create="03.12.2020 16:09:21" Count="300">
    //        <PERSON Name="a" Surname="b" SecId="1">
    //          <TASK Name="x" Duration="100" />
    //          <TASK Name="x1" Duration="1001"/>
    //        </PERSON>
    //      </TEAMS>


    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml11.xml")
    val xmlRaw = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")

      .xml(filePath)

    val xmlTeams = spark.read
      .option("rowTag", "TEAMS")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    // 0th depth
    compareOriginalAndRawXmlDataFrames(xmlTeams, xmlRaw, List("PERSON"))

    val personRaw = xmlRaw.select("PERSON")
    val personDF = applyMapFunctionToDataFrame(personRaw, "PERSON")

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    // 1st depth
    compareOriginalAndRawXmlDataFrames(xmlPerson, personDF, List("TASK"));

    val taskRaw = personDF.select("TASK")
    val taskFlatMap = applyFlatMapToDataFrame(taskRaw, "TASK")
    val taskDF = applyMapFunctionToDataFrame(taskFlatMap, "TASK")

    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List.empty[String])
    // TEST

  }


  test("keepInnerXmlAsRaw: Xml with depth 2. End element as /> " +
    "without element name's itself on depth 2. Array<TASK> on 2nd depth. " +
    "Read directly <Task> Element") {

    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "array/testarrayxml11.xml")
    val taskRaw = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")
      .xml(filePath)

    val xmlTask = spark.read
      .option("rowTag", "TASK")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTask, taskRaw, List.empty[String])


    val personRaw = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")
      .xml(filePath)

    val xmlPerson = spark.read
      .option("rowTag", "PERSON")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlPerson, personRaw, List("TASK"))

    val taskRaw1 = personRaw.select("TASK")

    val taskFlatMap = applyFlatMapToDataFrame(taskRaw1, "TASK")
    val taskDF = applyMapFunctionToDataFrame(taskFlatMap, "TASK")

    compareOriginalAndRawXmlDataFrames(xmlTask, taskDF, List.empty[String])
  }


  test("keepInnerXmlAsRaw: Xml with max depth 2. Nested tags with same name") {
    //    <?xml version="1.0" encoding="UTF-8"?>
    //      <tag1 Create="03.12.2020 16:09:21" Count="300">
    //        <tag2 Name="a" Surname="b" SecId="1">
    //          <tag3 Name="x" Duration="100">
    //            <tag2 field="test" field2="test2" field3="500"></tag2>
    //          </tag3>
    //          <tag3 Name="x1" Duration="1001">
    //            <tag3 innerf="a" innerf1="b" innerf2="2000">
    //              <tag2 col4="a" col5="b"></tag2>
    //            </tag3>
    //          </tag3>
    //          <tag3 col1="x" col2="y" col3="5"></tag3>
    //          <tag3 Name="x2" Duration="1002">
    //            <tag2 field="test1" field2="test21" field3="5001"></tag2>
    //          </tag3>
    //        </tag2>
    //      </tag1>
    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "/xmlsample1.xml")

    val tag1Raw = spark.read
      .option("rowTag", "tag1")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")
      .xml(filePath)

    val xmlTag1 = spark.read
      .option("rowTag", "tag1")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTag1, tag1Raw, List("tag2"))

    val tag2Raw = tag1Raw.select("tag2")
    val tag2DF = applyMapFunctionToDataFrame(tag2Raw, "tag2")
    val xmlTag2 = spark.read
      .option("rowTag", "tag2")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)
    compareOriginalAndRawXmlDataFrames(xmlTag2, tag2DF, List("tag3"))

    val tag3Raw = tag2DF.select("tag3")
    val tag3FlatMap = applyFlatMapToDataFrame(tag3Raw, "tag3")
    val tag3DF = applyMapFunctionToDataFrame(tag3FlatMap, "tag3")

    val xmlTag3 = spark.read
      .option("rowTag", "tag3")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTag3, tag3DF, List("tag2", "tag3"))

    val tag3Persisted = tag3DF.persist()
    val innerTag2Raw = tag3Persisted.select("tag2").filter("tag2 is not null")
    val innerTag3Raw = tag3Persisted.select("tag3").filter("tag3 is not null")
    val innerTag2DF = applyMapFunctionToDataFrame(innerTag2Raw, "tag2")

    val innerTag2DFSchema = innerTag2DF.schema


    assert(3 === innerTag2DFSchema.size)
    val fieldIndexAsOption = getFieldIndexAsOption(innerTag2DFSchema, "_field")
    val field2IndexAsOption = getFieldIndexAsOption(innerTag2DFSchema, "_field2")
    val field3IndexAsOption = getFieldIndexAsOption(innerTag2DFSchema, "_field3")

    assert(true === fieldIndexAsOption.nonEmpty)
    assert(true === field2IndexAsOption.nonEmpty)
    assert(true === field3IndexAsOption.nonEmpty)

    assert(DataTypes.StringType === innerTag2DFSchema.fields(fieldIndexAsOption.get).dataType)
    assert(DataTypes.StringType === innerTag2DFSchema.fields(field2IndexAsOption.get).dataType)
    assert(DataTypes.LongType === innerTag2DFSchema.fields(field3IndexAsOption.get).dataType)

    val innerTag2Rows = innerTag2DF.collect()
    assert(2 === innerTag2Rows.length)
    for (row <- innerTag2Rows) {
      if ("test".equals(row.get(fieldIndexAsOption.get))) {
        assert("test2" === row.get(field2IndexAsOption.get))
        assert(500 === row.get(field3IndexAsOption.get))
      } else if ("test1".equals(row.get(fieldIndexAsOption.get))) {
        assert("test21" === row.get(field2IndexAsOption.get))
        assert(5001 === row.get(field3IndexAsOption.get))
      } else {
        assert(false, "Failed")
      }
    }
    val innerTag3DF = applyMapFunctionToDataFrame(innerTag3Raw, "tag3")
    val innerTag3DFSchema = innerTag3DF.schema
    assert(3 === innerTag2DFSchema.size)

    val innerFieldIndexAsOption = getFieldIndexAsOption(innerTag3DFSchema, "_innerf")
    val innerField1IndexAsOption = getFieldIndexAsOption(innerTag3DFSchema, "_innerf1")
    val innerField2IndexAsOption = getFieldIndexAsOption(innerTag3DFSchema, "_innerf2")

    assert(true === innerFieldIndexAsOption.nonEmpty)
    assert(true === innerField1IndexAsOption.nonEmpty)
    assert(true === innerField2IndexAsOption.nonEmpty)

    assert(DataTypes.StringType === innerTag3DFSchema.fields(innerFieldIndexAsOption.get).dataType)
    assert(DataTypes.StringType === innerTag3DFSchema.fields(innerField1IndexAsOption.get).dataType)
    assert(DataTypes.LongType === innerTag3DFSchema.fields(innerField2IndexAsOption.get).dataType)
    val innerTag3Persisted = innerTag3DF.persist()
    val innerTag3Rows = innerTag3DF.collect()
    assert(1 === innerTag3Rows.length)
    for (row <- innerTag3Rows) {
      assert("a" === row.get(innerFieldIndexAsOption.get))
      assert("b" === row.get(innerField1IndexAsOption.get))
      assert(2000 === row.get(innerField2IndexAsOption.get))
    }

    val finalTag2Raw = innerTag3Persisted.select("tag2")

    val finalTag2DF = applyMapFunctionToDataFrame(finalTag2Raw, "tag2")

    val finalTag2DFSchema = finalTag2DF.schema

    assert(2 === finalTag2DFSchema.fields.length)
    val col4Index = finalTag2DFSchema.fieldIndex("_col4")
    val col5Index = finalTag2DFSchema.fieldIndex("_col5")

    val finalTag2Rows = finalTag2DF.collect()
    assert(1 === finalTag2Rows.length)

    for (row <- finalTag2Rows) {
      assert("a" === row.get(col4Index))
      assert("b" === row.get(col5Index))
    }

    tag3Persisted.unpersist()
    innerTag3Persisted.unpersist()
  }

  test("keepInnerXmlAsRaw: Xml with max depth 3. Array<tag5> on depth2. Select some tags.") {
    //    <?xml version="1.0" encoding="UTF-8"?>
    //      <tag1 field1="a" field2="1">
    //        <tag2 field3="b" field4="2">
    //          <tag3 field5="c" field6="3"></tag3>
    //          <tag4 field7="d" field8="4"></tag4>
    //          <tag5 field9="e" field10="5">
    //            <tag6 field11="f" field12="6"></tag6>
    //            <tag7 field13="g" field14="7"></tag7>
    //            <tag8 field15="h" field16="8"></tag8>
    //          </tag5>
    //          <tag5 field9="e1" field10="51">
    //            <tag6 field11="f1" field12="61"></tag6>
    //            <tag7 field13="g1" field14="71"></tag7>
    //            <tag8 field15="h1" field16="81"></tag8>
    //          </tag5>
    //        </tag2>
    //        <tag9 field17="i" field18="9" field19="1a"></tag9>
    //        <tag10 field20="99"></tag10>
    //      </tag1>

    val filePath = getTestResourcePath(resDir + keepInnerXmlDir + "/xmlsample2.xml")

    val tag1Raw = spark.read
      .option("rowTag", "tag1")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "true")
      .xml(filePath)

    val xmlTag1 = spark.read
      .option("rowTag", "tag1")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTag1, tag1Raw, List("tag2", "tag9", "tag10"))

    val tag1RawProjection = tag1Raw.select("tag2")


    val tag2Raw = applyMapFunctionToDataFrame(tag1RawProjection, "tag2")

    val tag2RawProjection = tag2Raw.select("_field3", "_field4", "tag4", "tag5")

    val xmlTag2 = spark.read
      .option("rowTag", "tag2")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath).select("_field3", "_field4", "tag4", "tag5")

    compareOriginalAndRawXmlDataFrames(xmlTag2, tag2RawProjection, List("tag4", "tag5"))

    val tag5Raw = tag2RawProjection.select("tag5")
    val tag5FlatMap = applyFlatMapToDataFrame(tag5Raw, "tag5")
    val tag5DF = applyMapFunctionToDataFrame(tag5FlatMap, "tag5")

    val xmlTag5 = spark.read
      .option("rowTag", "tag5")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath).select("_field10", "tag6", "tag8")

    val tag5DFProjection = tag5DF.select("_field10", "tag6", "tag8")

    compareOriginalAndRawXmlDataFrames(xmlTag5, tag5DFProjection, List("tag6", "tag8"))

    val tag8Raw = tag5DFProjection.select("tag8")

    val tag8DF = applyMapFunctionToDataFrame(tag8Raw, "tag8")

    val xmlTag8 = spark.read
      .option("rowTag", "tag8")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .option("keepInnerXmlAsRaw", "false")
      .xml(filePath)

    compareOriginalAndRawXmlDataFrames(xmlTag8, tag8DF, List.empty[String])
  }

  test("keepInnerXmlAsRaw: Test root-level-value xml files") {
    val rowsetRaw = spark.read
      .option("rowTag", "ROWSET")
      .option("keepInnerXmlAsRaw", "true")
      .xml(getTestResourcePath(resDir + "root-level-value.xml"))

    val rows = rowsetRaw.collect()
    var count = 0
    for (elem <- rows) {
      for (innerElem <- elem.get(0).asInstanceOf[mutable.ArraySeq[String]]) {
        if (count == 0) {
          assert(innerElem === "<ROW>value1</ROW>")
        } else if (count == 1) {
          assert(innerElem === "<ROW attr=\"attr1\">value2</ROW>")
        } else if (count == 2) {
          assert(innerElem === "<ROW>\n        value3\n        </ROW>")
        } else {
          assert(false, "Failed")
        }
        count += 1
      }
    }

    val rowXmlDf = spark.read
      .option("rowTag", "ROW")
      .option("keepInnerXmlAsRaw", "false")
      .xml(getTestResourcePath(resDir + "root-level-value.xml"))

    val rowFlatMap = applyFlatMapToDataFrame(rowsetRaw, "ROW")
    val rowDf = applyMapFunctionToDataFrame(rowFlatMap, "ROW")
    compareOriginalAndRawXmlDataFrames(rowXmlDf, rowDf, List.empty[String])


    val rowNoneXmlDf = spark.read
      .option("rowTag", "ROW")
      .option("keepInnerXmlAsRaw", "false")
      .xml(getTestResourcePath(resDir + "root-level-value-none.xml"))

    val rowsetNoneRaw = spark.read
      .option("rowTag", "ROWSET")
      .option("keepInnerXmlAsRaw", "true")
      .xml(getTestResourcePath(resDir + "root-level-value-none.xml"))
    val rowNoneFlatMap = applyFlatMapToDataFrame(rowsetNoneRaw, "ROW")
    val rowNoneDF = applyMapFunctionToDataFrame(rowNoneFlatMap, "ROW")
    compareOriginalAndRawXmlDataFrames(rowNoneXmlDf, rowNoneDF, List("tag"))
    val tagFromXml = rowNoneXmlDf.select("tag").filter("tag is not null")
    var tagDFRaw = applyMapFunctionToDataFrame(rowNoneDF
      .select("tag").filter("tag is not null"), "tag")
    tagDFRaw = tagDFRaw.withColumnRenamed("_VALUE", "tag")
    compareOriginalAndRawXmlDataFrames(tagFromXml, tagDFRaw, List.empty[String])
  }

  private def getSpecificSchemaTupleToCheckCorrectnessOfKeepInnerXmlAsRawOption
  (originalDf: Dataset[Row], xmlAsRawDf: Dataset[Row],
   colNameToBeExcludedList: List[String]): (Dataset[Row], Dataset[Row]) = {
    // originalDf means keepInnerXmlAsRaw option is not active
    val xmlSchemaSf = originalDf.schema.fields
      .filter(sf => !colNameToBeExcludedList.contains(sf.name))
    val rawXmlSchemaSf = xmlAsRawDf.schema.fields
      .filter(sf => !colNameToBeExcludedList.contains(sf.name))
    val xmlCols = xmlSchemaSf.map(sf => col(sf.name))
    val rawXmlCols = rawXmlSchemaSf.map(sf => col(sf.name))
    (originalDf.select(xmlCols.toIndexedSeq: _*), xmlAsRawDf.select(rawXmlCols.toIndexedSeq: _*))
  }

  private def compareOriginalAndRawXmlDataFrames
  (originalDf: Dataset[Row], rawXml: Dataset[Row], colNameToBeExcludedList: List[String]): Unit = {
    val dataFrameTuple =
      getSpecificSchemaTupleToCheckCorrectnessOfKeepInnerXmlAsRawOption(originalDf
        , rawXml, colNameToBeExcludedList)

    // tuple._1 --> originalDF
    // tuple._2 --> xmlAsRawDF
    // Comparing schemas and apply except operator.
    // originalDf means keepInnerXmlAsRaw option is not active
    assert(dataFrameTuple._1.schema === dataFrameTuple._2.schema)
    assert(dataFrameTuple._1.count() !== 0)
    val exceptResult = dataFrameTuple._1.except(dataFrameTuple._2)
    assert(exceptResult.collect().isEmpty)
  }

  private def applyMapFunctionToDataFrame(dataFrame: Dataset[Row], rowTag: String): Dataset[Row] = {
    val map = Map("nullValue" -> "", "inferSchema" -> "true"
      , "rowTag" -> rowTag, "keepInnerXmlAsRaw" -> "true")
    val options = XmlOptions(map)

    val xmlInferSchema = new XmlInferSchema(options
      , spark.sessionState.conf.caseSensitiveAnalysis)
    val inferredSchema = xmlInferSchema.infer(dataFrame.as(Encoders.STRING).rdd)


    val encoder = Encoders.row(inferredSchema)
    // I couldn't find a good solution to convert InternalRow to Row.
    //      expressionEncoder.resolveAndBind().createDeserializer().apply(internalRow)
    //      Above code line didn't work as expected. It threw the following exception:
    //      "MapObjects applied with a null function.
    //      Likely cause is failure to resolve an array expression or encoder."
    //    val expressionEncoder = encoder.asInstanceOf[ExpressionEncoder[Row]]

    val index = dataFrame.schema.fieldIndex(rowTag)
    val staxXmlParser = new StaxXmlParser(inferredSchema, options)

    dataFrame.map(row => {
      val finalSeq: Seq[Any] = staxXmlParser
        .parseColumn(row.getString(index), inferredSchema).toSeq(inferredSchema)
      val arrayBuffer = new ArrayBuffer[Any]
      for (elem <- finalSeq) {

        elem match {
          case data: GenericArrayData =>
            val arrayBufferInner = new ArrayBuffer[Any]
            for (elem <- data.array) {
              if (elem.isInstanceOf[UTF8String]) {
                arrayBufferInner.addOne(elem.toString)
              } else {
                arrayBufferInner.addOne(elem)
              }
            }
            arrayBuffer.addOne(arrayBufferInner)
          case data: UTF8String =>
            arrayBuffer.addOne(data.toString)
          case _ =>
            arrayBuffer.addOne(elem)
        }
      }
      Row.fromSeq(arrayBuffer.toSeq)
    })(encoder)
  }

  private def applyFlatMapToDataFrame(dfAsRaw: Dataset[Row], rowTag: String): Dataset[Row] = {
    var structType = new StructType
    structType = structType.add(StructField(rowTag, DataTypes.StringType, true, Metadata.empty))
    val encoder = Encoders.row(structType)
    val flatMap = dfAsRaw.flatMap(row => row.getSeq[AnyRef](0)
      .map(r => {
        RowFactory.create(r)
      }))(encoder)
    assert(flatMap.count() > 1)
    flatMap
  }

  private def getFieldIndexAsOption(schema: StructType, fieldName: String): Option[Int] = {
    val fieldIndex: Try[Int] = Try(schema.fieldIndex(fieldName))
    val fieldIndexAsOption: Option[Int] = fieldIndex match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
    fieldIndexAsOption
  }


}
