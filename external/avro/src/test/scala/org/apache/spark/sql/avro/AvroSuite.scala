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

package org.apache.spark.sql.avro

import java.io._
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.util.{TimeZone, UUID}

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.avro.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.types._

class AvroSuite extends SparkFunSuite {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("AvroSuite")
      .config("spark.sql.files.maxPartitionBytes", 1024)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("reading from multiple paths") {
    val df = spark.read.avro(episodesFile, episodesFile)
    assert(df.count == 16)
  }

  test("reading and writing partitioned data") {
    val df = spark.read.avro(episodesFile)
    val fields = List("title", "air_date", "doctor")
    for (field <- fields) {
      TestUtils.withTempDir { dir =>
        val outputDir = s"$dir/${UUID.randomUUID}"
        df.write.partitionBy(field).avro(outputDir)
        val input = spark.read.avro(outputDir)
        // makes sure that no fields got dropped.
        // We convert Rows to Seqs in order to work around SPARK-10325
        assert(input.select(field).collect().map(_.toSeq).toSet ===
          df.select(field).collect().map(_.toSeq).toSet)
      }
    }
  }

  test("request no fields") {
    val df = spark.read.avro(episodesFile)
    df.registerTempTable("avro_table")
    assert(spark.sql("select count(*) from avro_table").collect().head === Row(8))
  }

  test("convert formats") {
    TestUtils.withTempDir { dir =>
      val df = spark.read.avro(episodesFile)
      df.write.parquet(dir.getCanonicalPath)
      assert(spark.read.parquet(dir.getCanonicalPath).count() === df.count)
    }
  }

  test("rearrange internal schema") {
    TestUtils.withTempDir { dir =>
      val df = spark.read.avro(episodesFile)
      df.select("doctor", "title").write.avro(dir.getCanonicalPath)
    }
  }

  test("test NULL avro type") {
    TestUtils.withTempDir { dir =>
      val fields = Seq(new Field("null", Schema.create(Type.NULL), "doc", null)).asJava
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)
      avroRec.put("null", null)
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      intercept[IncompatibleSchemaException] {
        spark.read.avro(s"$dir.avro")
      }
    }
  }

  test("union(int, long) is read as long") {
    TestUtils.withTempDir { dir =>
      val avroSchema: Schema = {
        val union =
          Schema.createUnion(List(Schema.create(Type.INT), Schema.create(Type.LONG)).asJava)
        val fields = Seq(new Field("field1", union, "doc", null)).asJava
        val schema = Schema.createRecord("name", "docs", "namespace", false)
        schema.setFields(fields)
        schema
      }

      val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(avroSchema, new File(s"$dir.avro"))
      val rec1 = new GenericData.Record(avroSchema)
      rec1.put("field1", 1.toLong)
      dataFileWriter.append(rec1)
      val rec2 = new GenericData.Record(avroSchema)
      rec2.put("field1", 2)
      dataFileWriter.append(rec2)
      dataFileWriter.flush()
      dataFileWriter.close()
      val df = spark.read.avro(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", LongType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1L), Row(2L)))
    }
  }

  test("union(float, double) is read as double") {
    TestUtils.withTempDir { dir =>
      val avroSchema: Schema = {
        val union =
          Schema.createUnion(List(Schema.create(Type.FLOAT), Schema.create(Type.DOUBLE)).asJava)
        val fields = Seq(new Field("field1", union, "doc", null)).asJava
        val schema = Schema.createRecord("name", "docs", "namespace", false)
        schema.setFields(fields)
        schema
      }

      val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(avroSchema, new File(s"$dir.avro"))
      val rec1 = new GenericData.Record(avroSchema)
      rec1.put("field1", 1.toFloat)
      dataFileWriter.append(rec1)
      val rec2 = new GenericData.Record(avroSchema)
      rec2.put("field1", 2.toDouble)
      dataFileWriter.append(rec2)
      dataFileWriter.flush()
      dataFileWriter.close()
      val df = spark.read.avro(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", DoubleType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1.toDouble), Row(2.toDouble)))
    }
  }

  test("union(float, double, null) is read as nullable double") {
    TestUtils.withTempDir { dir =>
      val avroSchema: Schema = {
        val union = Schema.createUnion(
          List(Schema.create(Type.FLOAT),
            Schema.create(Type.DOUBLE),
            Schema.create(Type.NULL)
          ).asJava
        )
        val fields = Seq(new Field("field1", union, "doc", null)).asJava
        val schema = Schema.createRecord("name", "docs", "namespace", false)
        schema.setFields(fields)
        schema
      }

      val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(avroSchema, new File(s"$dir.avro"))
      val rec1 = new GenericData.Record(avroSchema)
      rec1.put("field1", 1.toFloat)
      dataFileWriter.append(rec1)
      val rec2 = new GenericData.Record(avroSchema)
      rec2.put("field1", null)
      dataFileWriter.append(rec2)
      dataFileWriter.flush()
      dataFileWriter.close()
      val df = spark.read.avro(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", DoubleType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1.toDouble), Row(null)))
    }
  }

  test("Union of a single type") {
    TestUtils.withTempDir { dir =>
      val UnionOfOne = Schema.createUnion(List(Schema.create(Type.INT)).asJava)
      val fields = Seq(new Field("field1", UnionOfOne, "doc", null)).asJava
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)

      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)

      avroRec.put("field1", 8)

      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      val df = spark.read.avro(s"$dir.avro")
      assert(df.first() == Row(8))
    }
  }

  test("Complex Union Type") {
    TestUtils.withTempDir { dir =>
      val fixedSchema = Schema.createFixed("fixed_name", "doc", "namespace", 4)
      val enumSchema = Schema.createEnum("enum_name", "doc", "namespace", List("e1", "e2").asJava)
      val complexUnionType = Schema.createUnion(
        List(Schema.create(Type.INT), Schema.create(Type.STRING), fixedSchema, enumSchema).asJava)
      val fields = Seq(
        new Field("field1", complexUnionType, "doc", null),
        new Field("field2", complexUnionType, "doc", null),
        new Field("field3", complexUnionType, "doc", null),
        new Field("field4", complexUnionType, "doc", null)
      ).asJava
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)
      val field1 = 1234
      val field2 = "Hope that was not load bearing"
      val field3 = Array[Byte](1, 2, 3, 4)
      val field4 = "e2"
      avroRec.put("field1", field1)
      avroRec.put("field2", field2)
      avroRec.put("field3", new Fixed(fixedSchema, field3))
      avroRec.put("field4", new EnumSymbol(enumSchema, field4))
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      val df = spark.sqlContext.read.avro(s"$dir.avro")
      assertResult(field1)(df.selectExpr("field1.member0").first().get(0))
      assertResult(field2)(df.selectExpr("field2.member1").first().get(0))
      assertResult(field3)(df.selectExpr("field3.member2").first().get(0))
      assertResult(field4)(df.selectExpr("field4.member3").first().get(0))
    }
  }

  test("Lots of nulls") {
    TestUtils.withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("binary", BinaryType, true),
        StructField("timestamp", TimestampType, true),
        StructField("array", ArrayType(ShortType), true),
        StructField("map", MapType(StringType, StringType), true),
        StructField("struct", StructType(Seq(StructField("int", IntegerType, true))))))
      val rdd = spark.sparkContext.parallelize(Seq[Row](
        Row(null, new Timestamp(1), Array[Short](1, 2, 3), null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null)))
      val df = spark.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("Struct field type") {
    TestUtils.withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("float", FloatType, true),
        StructField("short", ShortType, true),
        StructField("byte", ByteType, true),
        StructField("boolean", BooleanType, true)
      ))
      val rdd = spark.sparkContext.parallelize(Seq(
        Row(1f, 1.toShort, 1.toByte, true),
        Row(2f, 2.toShort, 2.toByte, true),
        Row(3f, 3.toShort, 3.toByte, true)
      ))
      val df = spark.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("Date field type") {
    TestUtils.withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("float", FloatType, true),
        StructField("date", DateType, true)
      ))
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
      val rdd = spark.sparkContext.parallelize(Seq(
        Row(1f, null),
        Row(2f, new Date(1451948400000L)),
        Row(3f, new Date(1460066400500L))
      ))
      val df = spark.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
      assert(spark.read.avro(dir.toString).select("date").collect().map(_(0)).toSet ==
        Array(null, 1451865600000L, 1459987200000L).toSet)
    }
  }

  test("Array data types") {
    TestUtils.withTempDir { dir =>
      val testSchema = StructType(Seq(
        StructField("byte_array", ArrayType(ByteType), true),
        StructField("short_array", ArrayType(ShortType), true),
        StructField("float_array", ArrayType(FloatType), true),
        StructField("bool_array", ArrayType(BooleanType), true),
        StructField("long_array", ArrayType(LongType), true),
        StructField("double_array", ArrayType(DoubleType), true),
        StructField("decimal_array", ArrayType(DecimalType(10, 0)), true),
        StructField("bin_array", ArrayType(BinaryType), true),
        StructField("timestamp_array", ArrayType(TimestampType), true),
        StructField("array_array", ArrayType(ArrayType(StringType), true), true),
        StructField("struct_array", ArrayType(
          StructType(Seq(StructField("name", StringType, true)))))))

      val arrayOfByte = new Array[Byte](4)
      for (i <- arrayOfByte.indices) {
        arrayOfByte(i) = i.toByte
      }

      val rdd = spark.sparkContext.parallelize(Seq(
        Row(arrayOfByte, Array[Short](1, 2, 3, 4), Array[Float](1f, 2f, 3f, 4f),
          Array[Boolean](true, false, true, false), Array[Long](1L, 2L), Array[Double](1.0, 2.0),
          Array[BigDecimal](BigDecimal.valueOf(3)), Array[Array[Byte]](arrayOfByte, arrayOfByte),
          Array[Timestamp](new Timestamp(0)),
          Array[Array[String]](Array[String]("CSH, tearing down the walls that divide us", "-jd")),
          Array[Row](Row("Bobby G. can't swim")))))
      val df = spark.createDataFrame(rdd, testSchema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("write with compression") {
    TestUtils.withTempDir { dir =>
      val AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
      val AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level"
      val uncompressDir = s"$dir/uncompress"
      val deflateDir = s"$dir/deflate"
      val snappyDir = s"$dir/snappy"
      val fakeDir = s"$dir/fake"

      val df = spark.read.avro(testFile)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "uncompressed")
      df.write.avro(uncompressDir)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "deflate")
      spark.conf.set(AVRO_DEFLATE_LEVEL, "9")
      df.write.avro(deflateDir)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "snappy")
      df.write.avro(snappyDir)

      val uncompressSize = FileUtils.sizeOfDirectory(new File(uncompressDir))
      val deflateSize = FileUtils.sizeOfDirectory(new File(deflateDir))
      val snappySize = FileUtils.sizeOfDirectory(new File(snappyDir))

      assert(uncompressSize > deflateSize)
      assert(snappySize > deflateSize)
    }
  }

  test("dsl test") {
    val results = spark.read.avro(episodesFile).select("title").collect()
    assert(results.length === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = spark.read.avro(testFile).collect()
    assert(all.length == 3)

    val str = spark.read.avro(testFile).select("string").collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = spark.read.avro(testFile).select("simple_map").collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = spark.read.avro(testFile).select("union_string_null").collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = spark.read.avro(testFile).select("union_int_long_null").collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = spark.read.avro(testFile).select("union_float_double").collect()
    assert(
      union2
        .map(x => new java.lang.Double(x(0).toString))
        .exists(p => Math.abs(p - Math.PI) < 0.001))

    val fixed = spark.read.avro(testFile).select("fixed3").collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = spark.read.avro(testFile).select("enum").collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = spark.read.avro(testFile).select("record").collect()
    assert(record(0)(0).getClass.toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = spark.read.avro(testFile).select("array_of_boolean").collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = spark.read.avro(testFile).select("bytes").collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].length).toSet == Set(3, 1, 0))
  }

  test("sql test") {
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE avroTable
         |USING avro
         |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT * FROM avroTable").collect().length === 8)
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    TestUtils.withTempDir { dir =>
      val avroDir = s"$dir/avro"
      spark.read.avro(testFile).write.avro(avroDir)
      TestUtils.checkReloadMatchesSaved(spark, testFile, avroDir)
    }
  }

  test("conversion to avro and back with namespace") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    TestUtils.withTempDir { tempDir =>
      val name = "AvroTest"
      val namespace = "com.databricks.spark.avro"
      val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

      val avroDir = tempDir + "/namedAvro"
      spark.read.avro(testFile).write.options(parameters).avro(avroDir)
      TestUtils.checkReloadMatchesSaved(spark, testFile, avroDir)

      // Look at raw file and make sure has namespace info
      val rawSaved = spark.sparkContext.textFile(avroDir)
      val schema = rawSaved.collect().mkString("")
      assert(schema.contains(name))
      assert(schema.contains(namespace))
    }
  }

  test("converting some specific sparkSQL types to avro") {
    TestUtils.withTempDir { tempDir =>
      val testSchema = StructType(Seq(
        StructField("Name", StringType, false),
        StructField("Length", IntegerType, true),
        StructField("Time", TimestampType, false),
        StructField("Decimal", DecimalType(10, 2), true),
        StructField("Binary", BinaryType, false)))

      val arrayOfByte = new Array[Byte](4)
      for (i <- arrayOfByte.indices) {
        arrayOfByte(i) = i.toByte
      }
      val cityRDD = spark.sparkContext.parallelize(Seq(
        Row("San Francisco", 12, new Timestamp(666), null, arrayOfByte),
        Row("Palo Alto", null, new Timestamp(777), null, arrayOfByte),
        Row("Munich", 8, new Timestamp(42), Decimal(3.14), arrayOfByte)))
      val cityDataFrame = spark.createDataFrame(cityRDD, testSchema)

      val avroDir = tempDir + "/avro"
      cityDataFrame.write.avro(avroDir)
      assert(spark.read.avro(avroDir).collect().length == 3)

      // TimesStamps are converted to longs
      val times = spark.read.avro(avroDir).select("Time").collect()
      assert(times.map(_(0)).toSet == Set(666, 777, 42))

      // DecimalType should be converted to string
      val decimals = spark.read.avro(avroDir).select("Decimal").collect()
      assert(decimals.map(_(0)).contains("3.14"))

      // There should be a null entry
      val length = spark.read.avro(avroDir).select("Length").collect()
      assert(length.map(_(0)).contains(null))

      val binary = spark.read.avro(avroDir).select("Binary").collect()
      for (i <- arrayOfByte.indices) {
        assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
      }
    }
  }

  test("correctly read long as date/timestamp type") {
    TestUtils.withTempDir { tempDir =>
      val sparkSession = spark
      import sparkSession.implicits._

      val currentTime = new Timestamp(System.currentTimeMillis())
      val currentDate = new Date(System.currentTimeMillis())
      val schema = StructType(Seq(
        StructField("_1", DateType, false), StructField("_2", TimestampType, false)))
      val writeDs = Seq((currentDate, currentTime)).toDS

      val avroDir = tempDir + "/avro"
      writeDs.write.avro(avroDir)
      assert(spark.read.avro(avroDir).collect().length == 1)

      val readDs = spark.read.schema(schema).avro(avroDir).as[(Date, Timestamp)]

      assert(readDs.collect().sameElements(writeDs.collect()))
    }
  }

  test("support of globbed paths") {
    val e1 = spark.read.avro("*/test/resources/episodes.avro").collect()
    assert(e1.length == 8)

    val e2 = spark.read.avro("src/*/*/episodes.avro").collect()
    assert(e2.length == 8)
  }

  test("does not coerce null date/timestamp value to 0 epoch.") {
    TestUtils.withTempDir { tempDir =>
      val sparkSession = spark
      import sparkSession.implicits._

      val nullTime: Timestamp = null
      val nullDate: Date = null
      val schema = StructType(Seq(
        StructField("_1", DateType, nullable = true),
        StructField("_2", TimestampType, nullable = true))
      )
      val writeDs = Seq((nullDate, nullTime)).toDS

      val avroDir = tempDir + "/avro"
      writeDs.write.avro(avroDir)
      val readValues = spark.read.schema(schema).avro(avroDir).as[(Date, Timestamp)].collect

      assert(readValues.size == 1)
      assert(readValues.head == ((nullDate, nullTime)))
    }
  }

  test("support user provided avro schema") {
    val avroSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "test_schema",
        |  "fields" : [{
        |    "name" : "string",
        |    "type" : "string",
        |    "doc"  : "Meaningless string of characters"
        |  }]
        |}
      """.stripMargin
    val result = spark.read.option(AvroFileFormat.AvroSchema, avroSchema).avro(testFile).collect()
    val expected = spark.read.avro(testFile).select("string").collect()
    assert(result.sameElements(expected))
  }

  test("support user provided avro schema with defaults for missing fields") {
    val avroSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "test_schema",
        |  "fields" : [{
        |    "name"    : "missingField",
        |    "type"    : "string",
        |    "default" : "foo"
        |  }]
        |}
      """.stripMargin
    val result = spark.read.option(AvroFileFormat.AvroSchema, avroSchema)
      .avro(testFile).select("missingField").first
    assert(result === Row("foo"))
  }

  test("reading from invalid path throws exception") {

    // Directory given has no avro files
    intercept[AnalysisException] {
      TestUtils.withTempDir(dir => spark.read.avro(dir.getCanonicalPath))
    }

    intercept[AnalysisException] {
      spark.read.avro("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[AnalysisException] {
      spark.read.avro("*/*/*/*/*/*/*/something.avro")
    }

    intercept[FileNotFoundException] {
      TestUtils.withTempDir { dir =>
        FileUtils.touch(new File(dir, "test"))
        spark.read.avro(dir.toString)
      }
    }

  }

  test("SQL test insert overwrite") {
    TestUtils.withTempDir { tempDir =>
      val tempEmptyDir = s"$tempDir/sqlOverwrite"
      // Create a temp directory for table that will be overwritten
      new File(tempEmptyDir).mkdirs()
      spark.sql(
        s"""
           |CREATE TEMPORARY TABLE episodes
           |USING avro
           |OPTIONS (path "$episodesFile")
         """.stripMargin.replaceAll("\n", " "))
      spark.sql(
        s"""
           |CREATE TEMPORARY TABLE episodesEmpty
           |(name string, air_date string, doctor int)
           |USING avro
           |OPTIONS (path "$tempEmptyDir")
         """.stripMargin.replaceAll("\n", " "))

      assert(spark.sql("SELECT * FROM episodes").collect().length === 8)
      assert(spark.sql("SELECT * FROM episodesEmpty").collect().isEmpty)

      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE episodesEmpty
           |SELECT * FROM episodes
         """.stripMargin.replaceAll("\n", " "))
      assert(spark.sql("SELECT * FROM episodesEmpty").collect().length == 8)
    }
  }

  test("test save and load") {
    // Test if load works as expected
    TestUtils.withTempDir { tempDir =>
      val df = spark.read.avro(episodesFile)
      assert(df.count == 8)

      val tempSaveDir = s"$tempDir/save/"

      df.write.avro(tempSaveDir)
      val newDf = spark.read.avro(tempSaveDir)
      assert(newDf.count == 8)
    }
  }

  test("test load with non-Avro file") {
    // Test if load works as expected
    TestUtils.withTempDir { tempDir =>
      val df = spark.read.avro(episodesFile)
      assert(df.count == 8)

      val tempSaveDir = s"$tempDir/save/"
      df.write.avro(tempSaveDir)

      Files.createFile(new File(tempSaveDir, "non-avro").toPath)

      val newDf = spark
        .read
        .option(AvroFileFormat.IgnoreFilesWithoutExtensionProperty, "true")
        .avro(tempSaveDir)

      assert(newDf.count == 8)
    }
  }

  test("read avro with user defined schema: read partial columns") {
    val partialColumns = StructType(Seq(
      StructField("string", StringType, false),
      StructField("simple_map", MapType(StringType, IntegerType), false),
      StructField("complex_map", MapType(StringType, MapType(StringType, StringType)), false),
      StructField("union_string_null", StringType, true),
      StructField("union_int_long_null", LongType, true),
      StructField("fixed3", BinaryType, true),
      StructField("fixed2", BinaryType, true),
      StructField("enum", StringType, false),
      StructField("record", StructType(Seq(StructField("value_field", StringType, false))), false),
      StructField("array_of_boolean", ArrayType(BooleanType), false),
      StructField("bytes", BinaryType, true)))
    val withSchema = spark.read.schema(partialColumns).avro(testFile).collect()
    val withOutSchema = spark
      .read
      .avro(testFile)
      .select("string", "simple_map", "complex_map", "union_string_null", "union_int_long_null",
        "fixed3", "fixed2", "enum", "record", "array_of_boolean", "bytes")
      .collect()
    assert(withSchema.sameElements(withOutSchema))
  }

  test("read avro with user defined schema: read non-exist columns") {
    val schema =
      StructType(
        Seq(
          StructField("non_exist_string", StringType, true),
          StructField(
            "record",
            StructType(Seq(
              StructField("non_exist_field", StringType, false),
              StructField("non_exist_field2", StringType, false))),
            false)))
    val withEmptyColumn = spark.read.schema(schema).avro(testFile).collect()

    assert(withEmptyColumn.forall(_ == Row(null: String, Row(null: String, null: String))))
  }

  test("read avro file partitioned") {
    TestUtils.withTempDir { dir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val df = (0 to 1024 * 3).toDS.map(i => s"record${i}").toDF("records")
      val outputDir = s"$dir/${UUID.randomUUID}"
      df.write.avro(outputDir)
      val input = spark.read.avro(outputDir)
      assert(input.collect.toSet.size === 1024 * 3 + 1)
      assert(input.rdd.partitions.size > 2)
    }
  }

  case class NestedBottom(id: Int, data: String)

  case class NestedMiddle(id: Int, data: NestedBottom)

  case class NestedTop(id: Int, data: NestedMiddle)

  test("saving avro that has nested records with the same name") {
    TestUtils.withTempDir { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(List(NestedTop(1, NestedMiddle(2, NestedBottom(3, "1")))))
      val outputFolder = s"$tempDir/duplicate_names/"
      writeDf.write.avro(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.avro(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }

  case class NestedMiddleArray(id: Int, data: Array[NestedBottom])

  case class NestedTopArray(id: Int, data: NestedMiddleArray)

  test("saving avro that has nested records with the same name inside an array") {
    TestUtils.withTempDir { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(
        List(NestedTopArray(1, NestedMiddleArray(2, Array(
          NestedBottom(3, "1"), NestedBottom(4, "2")
        ))))
      )
      val outputFolder = s"$tempDir/duplicate_names_array/"
      writeDf.write.avro(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.avro(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }

  case class NestedMiddleMap(id: Int, data: Map[String, NestedBottom])

  case class NestedTopMap(id: Int, data: NestedMiddleMap)

  test("saving avro that has nested records with the same name inside a map") {
    TestUtils.withTempDir { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(
        List(NestedTopMap(1, NestedMiddleMap(2, Map(
          "1" -> NestedBottom(3, "1"), "2" -> NestedBottom(4, "2")
        ))))
      )
      val outputFolder = s"$tempDir/duplicate_names_map/"
      writeDf.write.avro(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.avro(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }
}
