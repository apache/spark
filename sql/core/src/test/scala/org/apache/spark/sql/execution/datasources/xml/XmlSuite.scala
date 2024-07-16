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

import java.io.{EOFException, File}
import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.nio.file.{Files, Path, Paths}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDateTime}
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import javax.xml.stream.XMLStreamException

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.{DebugFilesystem, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoders, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.catalyst.xml.XmlOptions
import org.apache.spark.sql.catalyst.xml.XmlOptions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.execution.datasources.xml.TestUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class XmlSuite
    extends QueryTest
    with SharedSparkSession
    with CommonFileDataSourceSuite
    with TestXmlData {
  import testImplicits._

  private val resDir = "test-data/xml-resources/"

  private var tempDir: Path = _

  override protected def dataSourceFormat: String = "xml"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("XmlSuite")
    tempDir.toFile.deleteOnExit()
  }

  private def getEmptyTempDir(): Path = {
    Files.createTempDirectory(tempDir, "test")
  }

  override def excluded: Seq[String] = Seq(
    s"Propagate Hadoop configs from $dataSourceFormat options to underlying file system")

  private val baseOptions = Map("rowTag" -> "ROW")

  private def readData(
      xmlString: String,
      schemaOpt: Option[StructType],
      options: Map[String, String] = Map.empty): DataFrame = {
    val ds = spark.createDataset(spark.sparkContext.parallelize(Seq(xmlString)))(Encoders.STRING)
    if (schemaOpt.isDefined) {
      spark.read.schema(schemaOpt.get).options(options).xml(ds)
    } else {
      spark.read.options(options).xml(ds)
    }
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
    spark.sql(s"""
         |CREATE TEMPORARY VIEW carsTable1
         |USING org.apache.spark.sql.execution.datasources.xml
         |OPTIONS (rowTag "ROW", path "${getTestResourcePath(resDir + "cars.xml")}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable1").collect().length === 3)
  }

  test("DDL test with alias name") {
    spark.sql(s"""
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
    val inputFile = getTestResourcePath(resDir + "cars-malformed.xml")
    checkError(
      exception = intercept[SparkException] {
        spark.read
          .option("rowTag", "ROW")
          .option("mode", FailFastMode.name)
          .xml(inputFile)
      },
      errorClass = "_LEGACY_ERROR_TEMP_2165",
      parameters = Map("failFastMode" -> "FAILFAST")
    )
    val exceptionInParsing = intercept[SparkException] {
      spark.read
        .schema("year string")
        .option("rowTag", "ROW")
        .option("mode", FailFastMode.name)
        .xml(inputFile)
        .collect()
    }
    checkErrorMatchPVals(
      exception = exceptionInParsing,
      errorClass = "FAILED_READ_FILE.NO_HINT",
      parameters = Map("path" -> s".*$inputFile.*"))
    checkError(
      exception = exceptionInParsing.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[null]",
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("test FAILFAST with unclosed tag") {
    val inputFile = getTestResourcePath(resDir + "unclosed_tag.xml")
    checkError(
      exception = intercept[SparkException] {
        spark.read
          .option("rowTag", "book")
          .option("mode", FailFastMode.name)
          .xml(inputFile)
      },
      errorClass = "_LEGACY_ERROR_TEMP_2165",
      parameters = Map("failFastMode" -> "FAILFAST"))
    val exceptionInParsing = intercept[SparkException] {
      spark.read
        .schema("_id string")
        .option("rowTag", "book")
        .option("mode", FailFastMode.name)
        .xml(inputFile)
        .show()
    }
    checkErrorMatchPVals(
      exception = exceptionInParsing,
      errorClass = "FAILED_READ_FILE.NO_HINT",
      parameters = Map("path" -> s".*$inputFile.*"))
    checkError(
      exception = exceptionInParsing.getCause.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map(
        "badRecord" -> "[null]",
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
    spark.sql(s"""
           |CREATE TEMPORARY VIEW carsTable3
           |(year double, make string, model string, comments string, grp string)
           |USING xml
           |OPTIONS (rowTag "ROW", path "${getTestResourcePath(resDir + "empty.xml")}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT count(*) FROM carsTable3").collect().head(0) === 0)
  }

  test("SQL test insert overwrite") {
    val tempPath = getEmptyTempDir()
    spark.sql(s"""
         |CREATE TEMPORARY VIEW booksTableIO
         |USING xml
         |OPTIONS (path "${getTestResourcePath(resDir + "books.xml")}", rowTag "book")
      """.stripMargin.replaceAll("\n", " "))
    spark.sql(s"""
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
    assert(results(1) === Row("bob", ""))
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
    assert(result(3) === Row(Row(Row("E", ""))))
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
    // TODO: we don't support partial results
    assert(
      invalid.toSeq.toArray.take(schema.length - 1) ===
        Array(null, null, null, null, null,
          null, null, null))

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
    assert(mixedRow.getAs[Row](0) === Row(List("issue", "text ignored"), "lorem"))
    assert(mixedRow.getString(1) === "ipsum")
  }

  test("test mixed text and complex element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(getTestResourcePath(resDir + "mixed_children_2.xml"))
    assert(mixedDF.select("foo.bar").head().getString(0) === "lorem")
    assert(mixedDF.select("foo.baz.bing").head().getLong(0) === 2)
    assert(mixedDF.select("missing").head().getString(0) === "ipsum")
  }

  test("test XSD validation") {
    Seq("basket.xsd", "include-example/first.xsd").foreach { xsdFile =>
      val basketDF = spark.read
        .option("rowTag", "basket")
        .option("inferSchema", true)
        .option("rowValidationXSDPath", getTestResourcePath(resDir + xsdFile)
          .replace("file:/", "/"))
        .xml(getTestResourcePath(resDir + "basket.xml"))
      // Mostly checking it doesn't fail
      assert(basketDF.selectExpr("entry[0].key").head().getLong(0) === 9027)
    }
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

  test("schema_of_xml with DROPMALFORMED parse error test") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(s"""SELECT schema_of_xml('<ROW><a>1<ROW>', map('mode', 'DROPMALFORMED'))""")
          .collect()
      },
      errorClass = "_LEGACY_ERROR_TEMP_1099",
      parameters = Map(
        "funcName" -> "schema_of_xml",
        "mode" -> "DROPMALFORMED",
        "permissiveMode" -> "PERMISSIVE",
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("schema_of_xml with FAILFAST parse error test") {
    checkError(
      exception = intercept[SparkException] {
        spark.sql(s"""SELECT schema_of_xml('<ROW><a>1<ROW>', map('mode', 'FAILFAST'))""")
          .collect()
      },
      errorClass = "_LEGACY_ERROR_TEMP_2165",
      parameters = Map(
        "failFastMode" -> FailFastMode.name)
    )
  }

  test("schema_of_xml with PERMISSIVE check no error test") {
      val s = spark.sql(s"""SELECT schema_of_xml('<ROW><a>1<ROW>', map('mode', 'PERMISSIVE'))""")
        .collect()
      assert(s.head.get(0) == "STRUCT<_corrupt_record: STRING>")
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

  test("to_xml: input must be struct data type") {
    val df = Seq(1, 2).toDF("value")
    checkError(
      exception = intercept[AnalysisException] {
        df.select(to_xml($"value")).collect()
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"to_xml(value)\"",
        "paramIndex" -> ordinalNumber(0),
        "inputSql" -> "\"value\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      context = ExpectedContext(fragment = "to_xml", getCurrentClassCallSitePattern)
    )
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
    assert(whitespaceDF.take(1).head.getAs[String]("_corrupt_record") === null)
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

  test("Write timestamps correctly in ISO8601 format by default") {
    val originalSchema =
      buildSchema(
        field("author"),
        field("time", TimestampType),
        field("time2", TimestampType),
        field("time3", TimestampType),
        field("time4", TimestampType),
        field("time5", TimestampType)
      )

    val df = spark.read
      .option("rowTag", "book")
      .option("timestampFormat", "dd/MM/yyyy HH:mm[XXX]")
      .schema(originalSchema)
      .xml(getTestResourcePath(resDir + "timestamps.xml"))

    withTempDir { dir =>
      // use los angeles as old dates have wierd offsets
      withSQLConf("spark.session.timeZone" -> "America/Los_Angeles") {
        df
          .write
          .option("rowTag", "book")
          .xml(dir.getCanonicalPath + "/xml")
        val schema =
          buildSchema(
            field("author"),
            field("time", StringType),
            field("time2", StringType),
            field("time3", StringType),
            field("time4", StringType),
            field("time5", StringType)
          )
        val df2 = spark.read
          .option("rowTag", "book")
          .schema(schema)
          .xml(dir.getCanonicalPath + "/xml")

        val expectedStringDatesWithoutFormat = Seq(
          Row("John Smith",
            "1800-01-01T10:07:02.000-07:52:58",
            "1885-01-01T10:30:00.000-08:00",
            "2014-10-27T18:30:00.000-07:00",
            "2015-08-26T18:00:00.000-07:00",
            "2016-01-28T20:00:00.000-08:00"))

        checkAnswer(df2, expectedStringDatesWithoutFormat)
      }
    }
  }

  test("Test custom timestampFormat without timezone") {
    val xml = s"""<book>
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
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.xml("/this/file/does/not/exist")
      },
      errorClass = "PATH_NOT_FOUND",
      parameters = Map("path" -> "file:/this/file/does/not/exist")
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.schema(buildSchema(field("dummy"))).xml("/this/file/does/not/exist")
      },
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
      assert(result(2).getAs[String]("_VALUE") == "value3")
    }
  }

  test("SPARK-45488: root-level value tag for not attributes-only object") {
    val ATTRIBUTE_NAME = "_attr"
    val TAG_NAME = "tag"
    val VALUETAG_NAME = "_VALUE"
    val schema = buildSchema(
      field(VALUETAG_NAME),
      field(ATTRIBUTE_NAME),
      field(TAG_NAME, LongType))
    val expectedAns = Seq(
      Row("value1", null, null),
      Row("value2", "attr1", null),
      Row("4", null, 5L),
      Row("7", null, 6L),
      Row(null, "8", null))
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
      checkAnswer(df, expectedAns)
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

  def testWriteReadRoundTrip(df: DataFrame,
                             options: Map[String, String] = Map.empty): Unit = {
    withTempDir { dir =>
      df.write
        .options(options)
        .option("rowTag", "ROW")
        .mode("overwrite")
        .xml(dir.getCanonicalPath)
      val df2 = spark.read
        .options(options)
        .option("rowTag", "ROW")
        .xml(dir.getCanonicalPath)
      checkAnswer(df, df2)
    }
  }

  def primitiveFieldAndType: Dataset[String] =
    spark.createDataset(spark.sparkContext.parallelize("""
      <ROW>
        <string>this is a simple string.</string>
        <integer>10</integer>
        <long>21474836470</long>
        <bigInteger>92233720368547758070</bigInteger>
        <double>1.7976931348623157</double>
        <boolean>true</boolean>
        <null>null</null>
      </ROW>""" :: Nil))(Encoders.STRING)

  test("Primitive field and type inferring") {
    val dfWithNodecimal = spark.read
      .option("nullValue", "null")
      .xml(primitiveFieldAndType)
    assert(dfWithNodecimal.schema("bigInteger").dataType === DoubleType)
    assert(dfWithNodecimal.schema("double").dataType === DoubleType)

    val df = spark.read
      .option("nullValue", "null")
      .option("prefersDecimal", "true")
      .xml(primitiveFieldAndType)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType(20, 0), true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DecimalType(17, 16), true) ::
      StructField("integer", LongType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(df.schema === expectedSchema)

    checkAnswer(
      df,
      Row(
        new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157,
        10,
        21474836470L,
        null,
        "this is a simple string.")
    )
    testWriteReadRoundTrip(df, Map("nullValue" -> "null", "prefersDecimal" -> "true"))
  }

  test("Write and infer TIMESTAMP_NTZ values with a non-default pattern") {
    withTempPath { path =>
      val exp = spark.sql(
        """
      select timestamp_ntz'2020-12-12 12:12:12' as col0 union all
      select timestamp_ntz'2020-12-12 12:12:12.123456' as col0
      """)
      exp.write
        .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .option("rowTag", "ROW")
        .xml(path.getAbsolutePath)

      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString) {
        val res = spark.read
          .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
          .option("rowTag", "ROW")
          .xml(path.getAbsolutePath)

        assert(res.dtypes === exp.dtypes)
        checkAnswer(res, exp)
      }
    }
  }

  test("Write and infer TIMESTAMP_LTZ values with a non-default pattern") {
    withTempPath { path =>
      val exp = spark.sql(
        """
      select timestamp_ltz'2020-12-12 12:12:12' as col0 union all
      select timestamp_ltz'2020-12-12 12:12:12.123456' as col0
      """)
      exp.write
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .option("rowTag", "ROW")
        .xml(path.getAbsolutePath)

      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> SQLConf.TimestampTypes.TIMESTAMP_LTZ.toString) {
        val res = spark.read
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
          .option("rowTag", "ROW")
          .xml(path.getAbsolutePath)

        assert(res.dtypes === exp.dtypes)
        checkAnswer(res, exp)
      }
    }
  }

  test("Roundtrip in reading and writing TIMESTAMP_NTZ values with custom schema") {
    withTempPath { path =>
      val exp = spark.sql(
        """
      select
        timestamp_ntz'2020-12-12 12:12:12' as col1,
        timestamp_ltz'2020-12-12 12:12:12' as col2
      """)

      exp.write.option("rowTag", "ROW").xml(path.getAbsolutePath)

      val res = spark.read
        .schema("col1 TIMESTAMP_NTZ, col2 TIMESTAMP_LTZ")
        .option("rowTag", "ROW")
        .xml(path.getAbsolutePath)

      checkAnswer(res, exp)
    }
  }

  test("Timestamp type inference for a column with TIMESTAMP_NTZ values") {
    withTempPath { path =>
      val exp = spark.sql(
        """
      select timestamp_ntz'2020-12-12 12:12:12' as col0 union all
      select timestamp_ntz'2020-12-12 12:12:12' as col0
      """)

      exp.write.option("rowTag", "ROW").xml(path.getAbsolutePath)

      val timestampTypes = Seq(
        SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString,
        SQLConf.TimestampTypes.TIMESTAMP_LTZ.toString)

      timestampTypes.foreach { timestampType =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> timestampType) {
          val res = spark.read.option("rowTag", "ROW").xml(path.getAbsolutePath)

          if (timestampType == SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString) {
            checkAnswer(res, exp)
          } else {
            checkAnswer(
              res,
              spark.sql(
                """
              select timestamp_ltz'2020-12-12 12:12:12' as col0 union all
              select timestamp_ltz'2020-12-12 12:12:12' as col0
              """)
            )
          }
        }
      }
    }
  }

  test("Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ") {
    withTempPath { path =>
      Seq(
        "<ROW><col0>2020-12-12T12:12:12.000</col0></ROW>",
        "<ROW><col0>2020-12-12T17:12:12.000Z</col0></ROW>",
        "<ROW><col0>2020-12-12T17:12:12.000+05:00</col0></ROW>",
        "<ROW><col0>2020-12-12T12:12:12.000</col0></ROW>"
      ).toDF("data")
        .coalesce(1)
        .write.text(path.getAbsolutePath)

      // TODO: enable "legacy" policy when the backward compatible parsing is implemented.
      for (policy <- Seq("exception", "corrected")) {
        withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
          val res = spark.read.option("rowTag", "ROW").xml(path.getAbsolutePath)

          // NOTE:
          // Every value is tested for all types in XML schema inference so the sequence of
          // ["timestamp_ntz", "timestamp_ltz", "timestamp_ntz"] results in "timestamp_ltz".
          // This is different from CSV where inference starts from the last inferred type.
          //
          // This is why the similar test in CSV has a different result in "legacy" mode.

          val exp = spark.sql("""
            select timestamp_ltz'2020-12-12T12:12:12.000' as col0 union all
            select timestamp_ltz'2020-12-12T17:12:12.000Z' as col0 union all
            select timestamp_ltz'2020-12-12T17:12:12.000+05:00' as col0 union all
            select timestamp_ltz'2020-12-12T12:12:12.000' as col0
            """)
          checkAnswer(res, exp)
        }
      }
    }
  }

  test("Malformed records when reading TIMESTAMP_LTZ as TIMESTAMP_NTZ") {
    withTempPath { path =>
      Seq(
        "<ROW><col0>2020-12-12T12:12:12.000</col0></ROW>",
        "<ROW><col0>2020-12-12T12:12:12.000Z</col0></ROW>",
        "<ROW><col0>2020-12-12T12:12:12.000+05:00</col0></ROW>",
        "<ROW><col0>2020-12-12T12:12:12.000</col0></ROW>"
      ).toDF("data")
        .coalesce(1)
        .write.text(path.getAbsolutePath)

      for (timestampNTZFormat <- Seq(None, Some("yyyy-MM-dd'T'HH:mm:ss[.SSS]"))) {
        val reader = spark.read.schema("col0 TIMESTAMP_NTZ")
        val res = timestampNTZFormat match {
          case Some(format) =>
            reader.option("timestampNTZFormat", format)
              .option("rowTag", "ROW").xml(path.getAbsolutePath)
          case None =>
            reader.option("rowTag", "ROW").xml(path.getAbsolutePath)
        }

        checkAnswer(
          res,
          Seq(
            Row(LocalDateTime.of(2020, 12, 12, 12, 12, 12)),
            Row(null),
            Row(null),
            Row(LocalDateTime.of(2020, 12, 12, 12, 12, 12))
          )
        )
      }
    }
  }

  test("Fail to write TIMESTAMP_NTZ if timestampNTZFormat contains zone offset") {
    val patterns = Seq(
      "yyyy-MM-dd HH:mm:ss XXX",
      "yyyy-MM-dd HH:mm:ss Z",
      "yyyy-MM-dd HH:mm:ss z")

    val exp = spark.sql("select timestamp_ntz'2020-12-12 12:12:12' as col0")
    for (pattern <- patterns) {
      withTempPath { path =>
        val err = intercept[SparkException] {
          exp.write.option("timestampNTZFormat", pattern)
            .option("rowTag", "ROW").xml(path.getAbsolutePath)
        }
        checkErrorMatchPVals(
          exception = err,
          errorClass = "TASK_WRITE_FAILED",
          parameters = Map("path" -> s".*${path.getName}.*"))
        val msg = err.getCause.getMessage
        assert(
          msg.contains("Unsupported field: OffsetSeconds") ||
            msg.contains("Unable to extract value") ||
            msg.contains("Unable to extract ZoneId"))
      }
    }
  }

  test("capture values interspersed between elements - simple") {
    val xmlString =
      s"""
         |<ROW>
         |    value1
         |    <a>
         |        value2
         |        <b>1</b>
         |        value3
         |    </a>
         |    value4
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, Seq(Row(Array("value1", "value4"), Row(Array("value2", "value3"), 1))))
  }

  test("capture values interspersed between elements - array") {
    val xmlString =
      s"""
         |<ROW>
         |    value1
         |    <array>
         |        value2
         |        <b>1</b>
         |        value3
         |    </array>
         |    <array>
         |        value4
         |        <b>2</b>
         |        value5
         |        <c>3</c>
         |        value6
         |    </array>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val expectedAns = Seq(
      Row(
        "value1",
        Array(
          Row(List("value2", "value3"), 1, null),
          Row(List("value4", "value5", "value6"), 2, 3))))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, expectedAns)

  }

  test("capture values interspersed between elements - long and double") {
    val xmlString =
      s"""
         |<ROW>
         |  <a>
         |    1
         |    <b>2</b>
         |    3
         |    <b>4</b>
         |    5.0
         |  </a>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, Seq(Row(Row(Array(1.0, 3.0, 5.0), Array(2, 4)))))
  }

  test("capture values interspersed between elements - comments") {
    val xmlString =
      s"""
         |<ROW>
         |  <a> 1 <!--this is a comment--> 2 </a>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, Seq(Row(Row(Array(1, 2)))))
  }

  test("capture values interspersed between elements - whitespaces with quotes") {
    val xmlString =
      s"""
         |<ROW>
         |  <a>" "</a>
         |  <b>" "<c>1</c></b>
         |  <d><e attr=" "></e></d>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", false)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, Seq(
      Row("\" \"", Row("\" \"", 1), Row(Row(" ")))))
  }

  test("capture values interspersed between elements - nested comments") {
    val xmlString =
      s"""
         |<ROW>
         |    <a> 1
         |        <!--this is a comment--> 2
         |        <b>1</b>
         |        <!--this is a comment--> 3
         |        <b>2</b>
         |    </a>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(df, Seq(Row(Row(Array(1, 2, 3), Array(1, 2)))))
  }

  test("ignore commented and CDATA row tags") {
    val results = spark.read.format("xml")
      .option("rowTag", "ROW")
      .load(getTestResourcePath(resDir + "ignored-rows.xml"))

    val expectedResults = Seq.range(1, 18).map(Row(_))
    checkAnswer(results, expectedResults)

    val results2 = spark.read.format("xml")
      .option("rowTag", "ROW")
      .load(getTestResourcePath(resDir + "cdata-ending-eof.xml"))

    val expectedResults2 = Seq.range(1, 18).map(Row(_))
    checkAnswer(results2, expectedResults2)

    val results3 = spark.read.format("xml")
      .option("rowTag", "ROW")
      .load(getTestResourcePath(resDir + "cdata-no-close.xml"))

    val expectedResults3 = Seq.range(1, 18).map(Row(_))
    checkAnswer(results3, expectedResults3)

    val results4 = spark.read.format("xml")
      .option("rowTag", "ROW")
      .load(getTestResourcePath(resDir + "cdata-no-ignore.xml"))

    val expectedResults4 = Seq(
      Row("<a>1</a>"),
      Row("2"),
      Row("<ROW>3</ROW>"),
      Row("4"),
      Row("<ROW>5</ROW>"))
    checkAnswer(results4, expectedResults4)
  }

  test("capture values interspersed between elements - nested struct") {
    val xmlString =
      s"""
         |<ROW>
         |    <struct1>
         |        <struct2>
         |            <array>1</array>
         |            value1
         |            <array>2</array>
         |            value2
         |            <struct3>3</struct3>
         |        </struct2>
         |        value4
         |    </struct1>
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("rowTag", "ROW")
      .option("ignoreSurroundingSpaces", true)
      .option("multiLine", "true")
      .xml(input)

    checkAnswer(
      df,
      Seq(
        Row(
          Row(
            "value4",
            Row(
              Array("value1", "value2"),
              Array(1, 2),
              3)))))
  }

  test("capture values interspersed between elements - deeply nested") {
    val xmlString =
      s"""
         |<ROW>
         |    value1
         |    <struct1>
         |        value2
         |        <struct2>
         |            value3
         |            <array1>
         |                value4
         |                <struct3>
         |                    value5
         |                    <array2>1<!--First comment--> <!--Second comment--></array2>
         |                    <![CDATA[This is a CDATA section containing <sample1> text.]]>
         |                    <![CDATA[This is a CDATA section containing <sample2> text.]]>
         |                    value6
         |                    <array2>2</array2>
         |                    value7
         |                </struct3>
         |                value8
         |                <string>string</string>
         |                value9
         |            </array1>
         |            value10
         |            <array1>
         |                <struct3><!--First comment--> <!--Second comment-->
         |                    <array2>3</array2>
         |                    value11
         |                    <array2>4</array2><!--First comment--> <!--Second comment-->
         |                </struct3>
         |                <string>string</string>
         |                value12
         |            </array1>
         |            value13
         |            <int>3</int>
         |            value14
         |        </struct2>
         |        value15
         |    </struct1>
         |     <!--First comment-->
         |    value16
         |     <!--Second comment-->
         |</ROW>
         |""".stripMargin
    val input = spark.createDataset(Seq(xmlString))
    val df = spark.read
      .option("ignoreSurroundingSpaces", true)
      .option("rowTag", "ROW")
      .option("multiLine", "true")
      .xml(input)

    val expectedAns = Seq(Row(
      ArraySeq("value1", "value16"),
      Row(
        ArraySeq("value2", "value15"),
        Row(
          ArraySeq("value3", "value10", "value13", "value14"),
          Array(
              Row(
                ArraySeq("value4", "value8", "value9"),
                "string",
                Row(
                  ArraySeq(
                    "value5",
                    "This is a CDATA section containing <sample1> text." +
                      "\n                    This is a CDATA section containing <sample2> text.\n" +
                      "                    value6",
                    "value7"
                  ),
                  ArraySeq(1, 2)
                )
              ),
              Row(ArraySeq("value12"), "string", Row(ArraySeq("value11"), ArraySeq(3, 4)))
            ),
          3))))

    checkAnswer(df, expectedAns)
  }

  test("Find compatible types even if inferred DecimalType is not capable of other IntegralType") {
    val mixedIntegerAndDoubleRecords = Seq(
      """<ROW><a>3</a><b>1.1</b></ROW>""",
      s"""<ROW><a>3.1</a><b>0.${"0" * 38}1</b></ROW>""").toDS()
    val xmlDF = spark.read
      .option("prefersDecimal", "true")
      .option("rowTag", "ROW")
      .xml(mixedIntegerAndDoubleRecords)

    // The values in `a` field will be decimals as they fit in decimal. For `b` field,
    // they will be doubles as `1.0E-39D` does not fit.
    val expectedSchema = StructType(
      StructField("a", DecimalType(21, 1), true) ::
        StructField("b", DoubleType, true) :: Nil)

    assert(xmlDF.schema === expectedSchema)
    checkAnswer(
      xmlDF,
      Row(BigDecimal("3"), 1.1D) ::
        Row(BigDecimal("3.1"), 1.0E-39D) :: Nil
    )
  }

  def bigIntegerRecords: Dataset[String] =
    spark.createDataset(spark.sparkContext.parallelize(
      s"""<ROW><a>1${"0" * 38}</a><b>92233720368547758070</b></ROW>""" :: Nil))(Encoders.STRING)

  test("Infer big integers correctly even when it does not fit in decimal") {
    val df = spark.read
      .option("rowTag", "ROW")
      .option("prefersDecimal", "true")
      .xml(bigIntegerRecords)

    // The value in `a` field will be a double as it does not fit in decimal. For `b` field,
    // it will be a decimal as `92233720368547758070`.
    val expectedSchema = StructType(
      StructField("a", DoubleType, true) ::
        StructField("b", DecimalType(20, 0), true) :: Nil)

    assert(df.schema === expectedSchema)
    checkAnswer(df, Row(1.0E38D, BigDecimal("92233720368547758070")))
  }

  def floatingValueRecords: Dataset[String] =
    spark.createDataset(spark.sparkContext.parallelize(
      s"""<ROW><a>0.${"0" * 38}1</a><b>.01</b></ROW>""" :: Nil))(Encoders.STRING)

  test("Infer floating-point values correctly even when it does not fit in decimal") {
    val df = spark.read
      .option("prefersDecimal", "true")
      .option("rowTag", "ROW")
      .xml(floatingValueRecords)

    // The value in `a` field will be a double as it does not fit in decimal. For `b` field,
    // it will be a decimal as `0.01` by having a precision equal to the scale.
    val expectedSchema = StructType(
      StructField("a", DoubleType, true) ::
        StructField("b", DecimalType(2, 2), true) :: Nil)

    assert(df.schema === expectedSchema)
    checkAnswer(df, Row(1.0E-39D, BigDecimal("0.01")))

    val mergedDF = spark.read
      .option("prefersDecimal", "true")
      .option("rowTag", "ROW")
      .xml(floatingValueRecords.union(bigIntegerRecords))

    val expectedMergedSchema = StructType(
      StructField("a", DoubleType, true) ::
        StructField("b", DecimalType(22, 2), true) :: Nil)

    assert(expectedMergedSchema === mergedDF.schema)
    checkAnswer(
      mergedDF,
      Row(1.0E-39D, BigDecimal("0.01")) ::
        Row(1.0E38D, BigDecimal("92233720368547758070")) :: Nil
    )
  }

  test("SPARK-46248: Enabling/disabling ignoreCorruptFiles/ignoreMissingFiles") {
    withCorruptFile(inputFile => {
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
        val e = intercept[SparkException] {
          spark.read
            .option("rowTag", "ROW")
            .option("multiLine", false)
            .xml(inputFile.toURI.toString)
            .collect()
        }
        assert(ExceptionUtils.getRootCause(e).isInstanceOf[EOFException])
        assert(ExceptionUtils.getRootCause(e).getMessage === "Unexpected end of input stream")
        val e2 = intercept[SparkException] {
          spark.read
            .option("rowTag", "ROW")
            .option("multiLine", true)
            .xml(inputFile.toURI.toString)
            .collect()
        }
        assert(ExceptionUtils.getRootCause(e2).isInstanceOf[EOFException])
        assert(ExceptionUtils.getRootCause(e2).getMessage === "Unexpected end of input stream")
      }
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
        val result = spark.read
           .option("rowTag", "ROW")
           .option("multiLine", false)
           .xml(inputFile.toURI.toString)
           .collect()
        assert(result.isEmpty)
      }
    })
    withTempPath { dir =>
      import org.apache.hadoop.fs.Path
      val xmlPath = new Path(dir.getCanonicalPath, "xml")
      val fs = xmlPath.getFileSystem(spark.sessionState.newHadoopConf())

      sampledTestData.write.option("rowTag", "ROW").xml(xmlPath.toString)
      val df = spark.read.option("rowTag", "ROW").option("multiLine", true).xml(xmlPath.toString)
      fs.delete(xmlPath, true)
      withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "false") {
        checkErrorMatchPVals(
          exception = intercept[SparkException] {
            df.collect()
          },
          errorClass = "FAILED_READ_FILE.FILE_NOT_EXIST",
          parameters = Map("path" -> s".*$dir.*")
        )
      }

      sampledTestData.write.option("rowTag", "ROW").xml(xmlPath.toString)
      val df2 = spark.read.option("rowTag", "ROW").option("multiLine", true).xml(xmlPath.toString)
      fs.delete(xmlPath, true)
      withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "true") {
        assert(df2.collect().isEmpty)
      }
    }
  }

  test("SPARK-46248: Read from a corrupted compressed file") {
    withTempDir { dir =>
      val format = "xml"
      val numRecords = 10000
      // create data
      val data =
        spark.sparkContext.parallelize(
          (0 until numRecords).map(i => Row(i.toLong, (i * 2).toLong)))
      val schema = buildSchema(field("a1", LongType), field("a2", LongType))
      val df = spark.createDataFrame(data, schema)

      df.coalesce(4)
        .write
        .mode(SaveMode.Overwrite)
        .format(format)
        .option("compression", "gZiP")
        .option("rowTag", "row")
        .save(dir.getCanonicalPath)

      withCorruptedFile(dir) { corruptedDir =>
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
          val dfCorrupted = spark.read
            .format(format)
            .option("multiline", "true")
            .option("compression", "gzip")
            .option("rowTag", "row")
            .load(corruptedDir.getCanonicalPath)
          val results = dfCorrupted.collect()
          assert(results(1) === Row(1, 2))
          assert(results.length > 100)
          val dfCorruptedWSchema = spark.read
            .format(format)
            .schema(schema)
            .option("multiline", "true")
            .option("compression", "gzip")
            .option("rowTag", "row")
            .load(corruptedDir.getCanonicalPath)
          assert(dfCorrupted.dtypes === dfCorruptedWSchema.dtypes)
          checkAnswer(dfCorrupted, dfCorruptedWSchema)
        }
      }
    }
  }

  test("XML Validate Name") {
    val data = Seq(Row("Random String"))

    def checkValidation(fieldName: String,
                        errorMsg: String,
                        validateName: Boolean = true): Unit = {
      val schema = StructType(Seq(StructField(fieldName, StringType)))
      val df = spark.createDataFrame(data.asJava, schema)

      withTempDir { dir =>
        val path = dir.getCanonicalPath
        validateName match {
          case false =>
            df.write
              .option("rowTag", "ROW")
              .option("validateName", false)
              .option("declaration", "")
              .option("indent", "")
              .mode(SaveMode.Overwrite)
              .xml(path)
            // read file back and check its content
            val xmlFile = new File(path).listFiles()
              .filter(_.isFile)
              .filter(_.getName.endsWith("xml")).head
            val actualContent = Files.readString(xmlFile.toPath).replaceAll("\\n", "")
            assert(actualContent ===
              s"<${XmlOptions.DEFAULT_ROOT_TAG}><ROW>" +
                s"<$fieldName>${data.head.getString(0)}</$fieldName>" +
                s"</ROW></${XmlOptions.DEFAULT_ROOT_TAG}>")

          case true =>
            val e = intercept[SparkException] {
              df.write
                .option("rowTag", "ROW")
                .mode(SaveMode.Overwrite)
                .xml(path)
            }
            checkErrorMatchPVals(
              exception = e,
              errorClass = "TASK_WRITE_FAILED",
              parameters = Map("path" -> s".*${dir.getName}.*"))
            assert(e.getCause.isInstanceOf[XMLStreamException])
            assert(e.getCause.getMessage.contains(errorMsg))
        }
      }
    }

    checkValidation("", "Illegal to pass empty name")
    checkValidation(" ", "Illegal first name character ' '")
    checkValidation("1field", "Illegal first name character '1'")
    checkValidation("field name with space", "Illegal name character ' '")
    checkValidation("field", "", false)
  }

  test("SPARK-46355: Check Number of open files") {
    withSQLConf("fs.file.impl" -> classOf[XmlSuiteDebugFileSystem].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val numFiles = 10
        val numRecords = 10000
        val data =
          spark.sparkContext.parallelize(
            (0 until numRecords).map(i => Row(i.toLong, (i * 2).toLong)))
        val schema = buildSchema(field("a1", LongType), field("a2", LongType))
        val df = spark.createDataFrame(data, schema)

        // Write numFiles files
        df.repartition(numFiles)
          .write
          .mode(SaveMode.Overwrite)
          .option("rowTag", "row")
          .xml(path)

        // Serialized file read for Schema inference
        val dfRead = spark.read
          .option("rowTag", "row")
          .xml(path)
        assert(XmlSuiteDebugFileSystem.totalFiles() === numFiles)
        assert(XmlSuiteDebugFileSystem.maxFiles() > 1)

        XmlSuiteDebugFileSystem.reset()
        // Serialized file read for parsing across multiple executors
        assert(dfRead.count() === numRecords)
        assert(XmlSuiteDebugFileSystem.totalFiles() === numFiles)
        assert(XmlSuiteDebugFileSystem.maxFiles() > 1)
      }
    }
  }

  /////////////////////////////////////
  // Projection, sorting, filtering  //
  /////////////////////////////////////
  test("select with string xml object") {
    val xmlString =
      s"""
         |<ROW>
         |    <name>John</name>
         |    <metadata><id>3</id></metadata>
         |</ROW>
         |""".stripMargin
    val schema = new StructType()
      .add("name", StringType)
      .add("metadata", StringType)
    val df = readData(xmlString, Some(schema), baseOptions)
    checkAnswer(df.select("name"), Seq(Row("John")))
  }

  test("select with duplicate field name in string xml object") {
    val xmlString =
      s"""
         |<ROW>
         |    <a><b>c</b></a>
         |    <b>d</b>
         |</ROW>
         |""".stripMargin
    val schema = new StructType()
      .add("a", StringType)
      .add("b", StringType)
    val df = readData(xmlString, Some(schema), baseOptions)
    val dfWithSchemaInference = readData(xmlString, None, baseOptions)
    Seq(df, dfWithSchemaInference).foreach { df =>
      checkAnswer(df.select("b"), Seq(Row("d")))
    }
  }

  test("select nested struct objects") {
    val xmlString =
      s"""
         |<ROW>
         |    <struct>
         |        <innerStruct>
         |            <field1>1</field1>
         |            <field2>2</field2>
         |        </innerStruct>
         |    </struct>
         |</ROW>
         |""".stripMargin
    val schema = new StructType()
      .add(
        "struct",
        new StructType()
          .add("innerStruct", new StructType().add("field1", LongType).add("field2", LongType))
      )
    val df = readData(xmlString, Some(schema), baseOptions)
    val dfWithSchemaInference = readData(xmlString, None, baseOptions)
    Seq(df, dfWithSchemaInference).foreach { df =>
      checkAnswer(df.select("struct"), Seq(Row(Row(Row(1, 2)))))
      checkAnswer(df.select("struct.innerStruct"), Seq(Row(Row(1, 2))))
    }
  }

  test("select a struct of lists") {
    val xmlString =
      s"""
         |<ROW>
         |    <struct>
         |        <array><field>1</field></array>
         |        <array><field>2</field></array>
         |        <array><field>3</field></array>
         |    </struct>
         |</ROW>
         |""".stripMargin
    val schema = new StructType()
      .add(
        "struct",
        new StructType()
          .add("array", ArrayType(StructType(StructField("field", LongType) :: Nil))))

    val df = readData(xmlString, Some(schema), baseOptions)
    val dfWithSchemaInference = readData(xmlString, None, baseOptions)
    Seq(df, dfWithSchemaInference).foreach { df =>
      checkAnswer(df.select("struct"), Seq(Row(Row(Array(Row(1), Row(2), Row(3))))))
      checkAnswer(df.select("struct.array"), Seq(Row(Array(Row(1), Row(2), Row(3)))))
    }
  }

  test("select complex objects") {
    val xmlString =
      s"""
         |<ROW>
         |    1
         |    <struct1>
         |        value2
         |        <struct2>
         |            3
         |            <array1>
         |                value4
         |                <struct3>
         |                    5
         |                    <array2>1<!--First comment--> <!--Second comment--></array2>
         |                    value6
         |                    <array2>2</array2>
         |                    7
         |                </struct3>
         |                value8
         |                <string>string</string>
         |                9
         |            </array1>
         |            value10
         |            <array1>
         |                <struct3><!--First comment--> <!--Second comment-->
         |                    <array2>3</array2>
         |                    11
         |                    <array2>4</array2><!--First comment--> <!--Second comment-->
         |                </struct3>
         |                <string>string</string>
         |                value12
         |            </array1>
         |            13
         |            <int>3</int>
         |            value14
         |        </struct2>
         |        15
         |    </struct1>
         |     <!--First comment-->
         |    value16
         |     <!--Second comment-->
         |</ROW>
         |""".stripMargin
    val df = readData(xmlString, None, baseOptions ++ Map("valueTag" -> "VALUE"))
    checkAnswer(df.select("struct1.VALUE"), Seq(Row(Seq("value2", "15"))))
    checkAnswer(df.select("struct1.struct2.array1"), Seq(Row(Seq(
      Row(Seq("value4", "value8", "9"), "string", Row(Seq("5", "value6", "7"), Seq(1, 2))),
      Row(Seq("value12"), "string", Row(Seq("11"), Seq(3, 4)))
    ))))
    checkAnswer(df.select("struct1.struct2.array1.struct3"), Seq(Row(Seq(
      Row(Seq("5", "value6", "7"), Seq(1, 2)),
      Row(Seq("11"), Seq(3, 4))
    ))))
    checkAnswer(df.select("struct1.struct2.array1.string"), Seq(Row(Seq("string", "string"))))
  }
}

// Mock file system that checks the number of open files
class XmlSuiteDebugFileSystem extends DebugFilesystem {

  override def open(f: org.apache.hadoop.fs.Path, bufferSize: Int): FSDataInputStream = {
    val wrapped: FSDataInputStream = super.open(f, bufferSize)
    // All files should be closed before reading next one
    XmlSuiteDebugFileSystem.open()
    new FSDataInputStream(wrapped.getWrappedStream) {
      override def close(): Unit = {
        try {
          wrapped.close()
        } finally {
          XmlSuiteDebugFileSystem.close()
        }
      }
    }
  }
}

object XmlSuiteDebugFileSystem {
  private val openCounterPerThread = new ConcurrentHashMap[Long, Int]()
  private val maxFilesPerThread = new ConcurrentHashMap[Long, Int]()

  def reset() : Unit = {
    maxFilesPerThread.clear()
  }

  def open() : Unit = {
    val threadId = Thread.currentThread().getId
    // assert that there are no open files for this executor
    assert(openCounterPerThread.getOrDefault(threadId, 0) == 0)
    openCounterPerThread.put(threadId, 1)
    maxFilesPerThread.put(threadId, maxFilesPerThread.getOrDefault(threadId, 0) + 1)
  }

  def close(): Unit = {
    val threadId = Thread.currentThread().getId
    if (openCounterPerThread.get(threadId) == 1) {
      openCounterPerThread.put(threadId, 0)
    }
    assert(openCounterPerThread.get(threadId) == 0)
  }

  def maxFiles() : Int = {
    maxFilesPerThread.values().asScala.max
  }

  def totalFiles() : Int = {
    maxFilesPerThread.values().asScala.sum
  }
}
