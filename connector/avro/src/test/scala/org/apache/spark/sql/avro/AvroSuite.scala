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
import java.net.URI
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.{Date, Timestamp}
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.avro.{AvroTypeException, Schema, SchemaBuilder}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.Schema.Type._
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.commons.io.FileUtils

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf, SparkException, SparkUpgradeException}
import org.apache.spark.TestUtils.assertExceptionMsg
import org.apache.spark.sql._
import org.apache.spark.sql.TestingUDT.IntervalData
import org.apache.spark.sql.avro.AvroCompressionCodec._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA, UTC}
import org.apache.spark.sql.execution.{FormattedMode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{CommonFileDataSourceSuite, DataSource, FilePartition}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.internal.LegacyBehaviorPolicy._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.Utils

abstract class AvroSuite
  extends QueryTest
  with SharedSparkSession
  with CommonFileDataSourceSuite
  with NestedDataSourceSuiteBase {

  import testImplicits._

  override protected def dataSourceFormat = "avro"
  override val nestedDataSources = Seq("avro")
  val episodesAvro = testFile("episodes.avro")
  val testAvro = testFile("test.avro")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // initialize SessionCatalog here so it has a clean hadoopConf
    spark.sessionState.catalog
    spark.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, 1024)
  }

  def checkReloadMatchesSaved(originalFile: String, newFile: String): Unit = {
    val originalEntries = spark.read.format("avro").load(testAvro).collect()
    val newEntries = spark.read.format("avro").load(newFile)
    checkAnswer(newEntries, originalEntries)
  }

  def checkAvroSchemaEquals(avroSchema: String, expectedAvroSchema: String): Unit = {
    assert(new Schema.Parser().parse(avroSchema) ==
      new Schema.Parser().parse(expectedAvroSchema))
  }

  def getAvroSchemaStringFromFiles(filePath: String): String = {
    new DataFileReader({
      val file = new File(filePath)
      if (file.isFile) {
        file
      } else {
        file.listFiles()
          .filter(_.isFile)
          .filter(_.getName.endsWith("avro"))
          .head
      }
    }, new GenericDatumReader[Any]()).getSchema.toString(false)
  }

  // Check whether an Avro schema of union type is converted to SQL in an expected way, when the
  // stable ID option is on.
  //
  // @param types             Avro types that contain in an Avro union type
  // @param expectedSchema    Expected SQL schema, provided in DDL string form
  // @param fieldsAndRow      A list of rows to be appended to the Avro file and the expected
  //                          converted SQL rows
  // @param stableIdPrefixOpt Stable id prefix to use for Union type
  private def checkUnionStableId(
      types: List[Schema],
      expectedSchema: String,
      fieldsAndRow: Seq[(Any, Row)],
      stableIdPrefixOpt: Option[String] = None): Unit = {
    withTempDir { dir =>
      val unionType = Schema.createUnion(
        types.asJava
      )
      val fields =
        Seq(new Field("field1", unionType, "doc", null.asInstanceOf[AnyVal])).asJava
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))

      fieldsAndRow.map(_._1).foreach { f =>
        val avroRec = new GenericData.Record(schema)
        f match {
          case a : Array[Byte] =>
            val fixedSchema = SchemaBuilder.fixed("fixed_name").size(4)
            avroRec.put("field1", new Fixed(fixedSchema, a));
          case other =>
            avroRec.put("field1", other)
        }
        dataFileWriter.append(avroRec)
      }
      dataFileWriter.flush()
      dataFileWriter.close()

      var dfReader = spark
        .read
        .format("avro")
        .option(AvroOptions.STABLE_ID_FOR_UNION_TYPE, "true")

      stableIdPrefixOpt.foreach { prefix =>
        dfReader = dfReader.option(AvroOptions.STABLE_ID_PREFIX_FOR_UNION_TYPE, prefix)
      }

      val df = dfReader.load(s"$dir.avro")
      assert(df.schema === StructType.fromDDL("field1 " + expectedSchema))
      assert(df.collect().toSet == fieldsAndRow.map(fr => Row(fr._2)).toSet)
    }
  }

  private def getResourceAvroFilePath(name: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(name).toString
  }

  test("resolve avro data source") {
    val databricksAvro = "com.databricks.spark.avro"
    // By default the backward compatibility for com.databricks.spark.avro is enabled.
    Seq("org.apache.spark.sql.avro.AvroFileFormat", databricksAvro).foreach { provider =>
      assert(DataSource.lookupDataSource(provider, spark.sessionState.conf) ===
        classOf[org.apache.spark.sql.avro.AvroFileFormat])
    }

    withSQLConf(SQLConf.LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED.key -> "false") {
      val message = intercept[AnalysisException] {
        DataSource.lookupDataSource(databricksAvro, spark.sessionState.conf)
      }.getMessage
      assert(message.contains(s"Failed to find data source: $databricksAvro"))
    }
  }

  test("reading from multiple paths") {
    val df = spark.read.format("avro").load(episodesAvro, episodesAvro)
    assert(df.count() == 16)
  }

  test("reading and writing partitioned data") {
    val df = spark.read.format("avro").load(episodesAvro)
    val fields = List("title", "air_date", "doctor")
    for (field <- fields) {
      withTempPath { dir =>
        val outputDir = s"$dir/${UUID.randomUUID}"
        df.write.partitionBy(field).format("avro").save(outputDir)
        val input = spark.read.format("avro").load(outputDir)
        // makes sure that no fields got dropped.
        // We convert Rows to Seqs in order to work around SPARK-10325
        assert(input.select(field).collect().map(_.toSeq).toSet ===
          df.select(field).collect().map(_.toSeq).toSet)
      }
    }
  }

  test("request no fields") {
    val df = spark.read.format("avro").load(episodesAvro)
    df.createOrReplaceTempView("avro_table")
    assert(spark.sql("select count(*) from avro_table").collect().head === Row(8))
  }

  test("convert formats") {
    withTempPath { dir =>
      val df = spark.read.format("avro").load(episodesAvro)
      df.write.parquet(dir.getCanonicalPath)
      assert(spark.read.parquet(dir.getCanonicalPath).count() === df.count())
    }
  }

  test("rearrange internal schema") {
    withTempPath { dir =>
      val df = spark.read.format("avro").load(episodesAvro)
      df.select("doctor", "title").write.format("avro").save(dir.getCanonicalPath)
    }
  }

  test("union(int, long) is read as long") {
    withTempPath { dir =>
      val avroSchema: Schema = {
        val union =
          Schema.createUnion(List(Schema.create(Type.INT), Schema.create(Type.LONG)).asJava)
        val fields = Seq(new Field("field1", union, "doc", null.asInstanceOf[AnyVal])).asJava
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
      val df = spark.read.format("avro").load(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", LongType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1L), Row(2L)))
    }
  }

  test("union(float, double) is read as double") {
    withTempPath { dir =>
      val avroSchema: Schema = {
        val union =
          Schema.createUnion(List(Schema.create(Type.FLOAT), Schema.create(Type.DOUBLE)).asJava)
        val fields = Seq(new Field("field1", union, "doc", null.asInstanceOf[AnyVal])).asJava
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
      val df = spark.read.format("avro").load(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", DoubleType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1.toDouble), Row(2.toDouble)))
    }
  }

  test("union(float, double, null) is read as nullable double") {
    withTempPath { dir =>
      val avroSchema: Schema = {
        val union = Schema.createUnion(
          List(Schema.create(Type.FLOAT),
            Schema.create(Type.DOUBLE),
            Schema.create(Type.NULL)
          ).asJava
        )
        val fields = Seq(new Field("field1", union, "doc", null.asInstanceOf[AnyVal])).asJava
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
      val df = spark.read.format("avro").load(s"$dir.avro")
      assert(df.schema.fields === Seq(StructField("field1", DoubleType, nullable = true)))
      assert(df.collect().toSet == Set(Row(1.toDouble), Row(null)))
    }
  }

  test("Union of a single type") {
    withTempPath { dir =>
      val UnionOfOne = Schema.createUnion(List(Schema.create(Type.INT)).asJava)
      val fields = Seq(new Field("field1", UnionOfOne, "doc", null.asInstanceOf[AnyVal])).asJava
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

      val df = spark.read.format("avro").load(s"$dir.avro")
      assert(df.first() == Row(8))
    }
  }

  // The test verifies Avro option "enableStableIdentifiersForUnionType". It adds all types into
  // union and validate they are converted to expected SQL field names. The test also creates
  // different cases that might cause field name conflicts and see they are handled properly.
  test("SPARK-43333: Stable field names when converting Union type") {
    checkUnionStableId(
      List(Type.INT, Type.NULL, Type.STRING).map(Schema.create(_)),
      "struct<member_int: int, member_string: string>",
      Seq(
        (42, Row(42, null)),
        ("Alice", Row(null, "Alice"))))

    checkUnionStableId(
      List( Type.FLOAT, Type.BOOLEAN, Type.BYTES, Type.DOUBLE, Type.LONG).map(Schema.create(_)),
      "struct<member_float: float, member_boolean: boolean, " +
        "member_bytes: binary, member_double: double, member_long: long>",
      Seq(
        (true, Row(null, true, null, null, null)),
        (42L, Row(null, null, null, null, 42L)),
        (42F, Row(42.0, null, null, null, null)),
       (42D, Row(null, null, null, 42D, null))))

    checkUnionStableId(
      List(
        Schema.createArray(Schema.create(Type.FLOAT)),
        Schema.createMap(Schema.create(Schema.Type.INT))),
      "struct<member_array: array<float>, member_map: map<string, int>>",
      Seq())

    checkUnionStableId(
      List(
        Schema.createEnum("myenum", "", null, List[String]("e1", "e2").asJava),
        Schema.createRecord("myrecord", "", null, false,
          List[Schema.Field](new Schema.Field("f", Schema.createFixed("myfield", "", null, 6)))
            .asJava),
        Schema.createRecord("myrecord2", "", null, false,
          List[Schema.Field](new Schema.Field("f", Schema.create(Type.FLOAT)))
            .asJava)),
      "struct<member_myenum: string, member_myrecord: struct<f: binary>, " +
                    "member_myrecord2: struct<f: float>>",
      Seq())

    {
      val e = intercept[Exception] {
        checkUnionStableId(
          List(
            Schema.createFixed("MYFIELD2", "", null, 6),
            Schema.createFixed("myfield1", "", null, 6),
            Schema.createFixed("myfield2", "", null, 9)),
          "",
          Seq())
      }
      assert(e.getMessage.contains("Cannot generate stable identifier"))
    }
    {
      val e = intercept[Exception] {
        checkUnionStableId(
          List(
            Schema.createFixed("ARRAY", "", null, 6),
            Schema.createArray(Schema.create(Type.STRING))),
          "",
          Seq())
      }
      assert(e.getMessage.contains("Cannot generate stable identifier"))
    }
    // Two array types or two map types are not allowed in union.
    {
      val e = intercept[Exception] {
        Schema.createUnion(
          List(
           Schema.createArray(Schema.create(Type.FLOAT)),
           Schema.createArray(Schema.create(Type.STRING))).asJava)
      }
      assert(e.getMessage.contains("Duplicate in union"))
    }
    {
      val e = intercept[Exception] {
        Schema.createUnion(
          List(
            Schema.createMap(Schema.create(Type.FLOAT)),
            Schema.createMap(Schema.create(Type.STRING))).asJava)
      }
      assert(e.getMessage.contains("Duplicate in union"))
    }

    // Somehow Avro allows named type "array", but doesn't allow an array type in the same union.
    {
      val e = intercept[Exception] {
        Schema.createUnion(
          List(
            Schema.createArray(Schema.create(Type.FLOAT)),
            Schema.createFixed("array", "", null, 6)
          ).asJava
        )
      }
      assert(e.getMessage.contains("Duplicate in union"))
    }
    {
      val e = intercept[Exception] {
        Schema.createUnion(
          List(Schema.createFixed("long", "", null, 6)).asJava
        )
      }
      assert(e.getMessage.contains("Schemas may not be named after primitives"))
    }

    {
      val e = intercept[Exception] {
        Schema.createUnion(
          List(Schema.createFixed("bytes", "", null, 6)).asJava
        )
      }
      assert(e.getMessage.contains("Schemas may not be named after primitives"))
    }
  }

  test("SPARK-47904: Test that field name case is preserved") {
    checkUnionStableId(
      List(
        Schema.createEnum("myENUM", "", null, List[String]("E1", "e2").asJava),
        Schema.createRecord("myRecord", "", null, false,
          List[Schema.Field](new Schema.Field("f", Schema.createFixed("myField", "", null, 6)))
            .asJava),
        Schema.createRecord("myRecord2", "", null, false,
          List[Schema.Field](new Schema.Field("F", Schema.create(Type.FLOAT)))
            .asJava)),
      "struct<member_myENUM: string, member_myRecord: struct<f: binary>, " +
                    "member_myRecord2: struct<F: float>>",
      Seq())

    {
      val e = intercept[Exception] {
        checkUnionStableId(
          List(
            Schema.createRecord("myRecord", "", null, false, List[Schema.Field]().asJava),
            Schema.createRecord("myrecord", "", null, false, List[Schema.Field]().asJava)),
          "",
          Seq())
      }
      assert(e.getMessage.contains("Cannot generate stable identifier"))
    }
  }

  test("SPARK-46930: Use custom prefix for stable ids when converting Union type") {
    // Test default "member_" prefix.
    checkUnionStableId(
      List(Type.INT, Type.NULL, Type.STRING).map(Schema.create(_)),
      "struct<member_int: int, member_string: string>",
      Seq(
        (42, Row(42, null)),
        ("Alice", Row(null, "Alice"))))

    // Test user-configured prefixes.
    for (prefix <- Seq("tmp_", "tmp", "member", "MEMBER_", "__", "")) {
      checkUnionStableId(
        List(Type.INT, Type.NULL, Type.STRING).map(Schema.create(_)),
        s"struct<${prefix}int: int, ${prefix}string: string>",
        Seq(
          (42, Row(42, null)),
          ("Alice", Row(null, "Alice"))),
        Some(prefix)
      )
    }
  }

  test("SPARK-27858 Union type: More than one non-null type") {
    Seq(true, false).foreach { isStableUnionMember =>
      withTempDir { dir =>
        val complexNullUnionType = Schema.createUnion(
          List(Schema.create(Type.INT), Schema.create(Type.NULL), Schema.create(Type.STRING))
            .asJava
        )
        val fields =
          Seq(new Field("field1", complexNullUnionType, "doc", null.asInstanceOf[AnyVal])).asJava
        val schema = Schema.createRecord("name", "docs", "namespace", false)
        schema.setFields(fields)
        val datumWriter = new GenericDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        dataFileWriter.create(schema, new File(s"$dir.avro"))
        val avroRec = new GenericData.Record(schema)
        avroRec.put("field1", 42)
        dataFileWriter.append(avroRec)
        val avroRec2 = new GenericData.Record(schema)
        avroRec2.put("field1", "Alice")
        dataFileWriter.append(avroRec2)
        dataFileWriter.flush()
        dataFileWriter.close()

        val df = spark
          .read
          .format("avro")
          .option(AvroOptions.STABLE_ID_FOR_UNION_TYPE, isStableUnionMember)
          .load(s"$dir.avro")
        if (isStableUnionMember) {
          assert(df.schema === StructType.fromDDL(
            "field1 struct<member_int: int, member_string: string>"))
        } else {
          assert(df.schema === StructType.fromDDL("field1 struct<member0: int, member1: string>"))
        }
        assert(df.collect().toSet == Set(Row(Row(42, null)), Row(Row(null, "Alice"))))
      }
    }
  }

  test("Complex Union Type") {
    withTempPath { dir =>
      val nativeWriterPath = s"$dir.avro"
      val sparkWriterPath = s"$dir/spark"
      val fixedSchema = SchemaBuilder.fixed("fixed_name").size(4)
      val enumSchema = SchemaBuilder.enumeration("enum_name").symbols("e1", "e2")
      val complexUnionType = SchemaBuilder.unionOf()
          .intType().and()
          .stringType().and()
          .`type`(fixedSchema).and()
          .`type`(enumSchema).and()
          .nullType()
        .endUnion()
      val schema = SchemaBuilder.record("name").fields()
          .name("field1").`type`(complexUnionType).noDefault()
          .name("field2").`type`(complexUnionType).noDefault()
          .name("field3").`type`(complexUnionType).noDefault()
          .name("field4").`type`(complexUnionType).noDefault()
          .name("field5").`type`(complexUnionType).noDefault()
        .endRecord()
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(nativeWriterPath))
      val avroRec = new GenericData.Record(schema)
      val field1 = 1234
      val field2 = "Hope that was not load bearing"
      val field3 = Array[Byte](1, 2, 3, 4)
      val field4 = "e2"
      avroRec.put("field1", field1)
      avroRec.put("field2", field2)
      avroRec.put("field3", new Fixed(fixedSchema, field3))
      avroRec.put("field4", new EnumSymbol(enumSchema, field4))
      avroRec.put("field5", null)
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      val df = spark.read.format("avro").load(nativeWriterPath)
      assertResult(Row(field1, null, null, null))(df.selectExpr("field1.*").first())
      assertResult(Row(null, field2, null, null))(df.selectExpr("field2.*").first())
      assertResult(Row(null, null, field3, null))(df.selectExpr("field3.*").first())
      assertResult(Row(null, null, null, field4))(df.selectExpr("field4.*").first())
      assertResult(Row(null, null, null, null))(df.selectExpr("field5.*").first())

      df.write.format("avro").option("avroSchema", schema.toString).save(sparkWriterPath)

      val df2 = spark.read.format("avro").load(nativeWriterPath)
      assertResult(Row(field1, null, null, null))(df2.selectExpr("field1.*").first())
      assertResult(Row(null, field2, null, null))(df2.selectExpr("field2.*").first())
      assertResult(Row(null, null, field3, null))(df2.selectExpr("field3.*").first())
      assertResult(Row(null, null, null, field4))(df2.selectExpr("field4.*").first())
      assertResult(Row(null, null, null, null))(df2.selectExpr("field5.*").first())

      val reader = openDatumReader(new File(sparkWriterPath))
      assert(reader.hasNext)
      assertResult(avroRec)(reader.next())
      assert(!reader.hasNext)
      reader.close()
    }
  }

  test("Lots of nulls") {
    withTempPath { dir =>
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
      df.write.format("avro").save(dir.toString)
      assert(spark.read.format("avro").load(dir.toString).count() == rdd.count())
    }
  }

  test("Struct field type") {
    withTempPath { dir =>
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
      df.write.format("avro").save(dir.toString)
      assert(spark.read.format("avro").load(dir.toString).count() == rdd.count())
    }
  }

  private def createDummyCorruptFile(dir: File): Unit = {
    Utils.tryWithResource {
      FileUtils.forceMkdir(dir)
      val corruptFile = new File(dir, "corrupt.avro")
      new BufferedWriter(new FileWriter(corruptFile))
    } { writer =>
      writer.write("corrupt")
    }
  }

  test("Ignore corrupt Avro file if flag IGNORE_CORRUPT_FILES enabled") {
    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
      withTempPath { dir =>
        createDummyCorruptFile(dir)
        val message = intercept[FileNotFoundException] {
          spark.read.format("avro").load(dir.getAbsolutePath).schema
        }.getMessage
        assert(message.contains("No Avro files found."))

        Files.copy(
          Paths.get(new URI(episodesAvro)),
          Paths.get(dir.getCanonicalPath, "episodes.avro"))

        val result = spark.read.format("avro").load(episodesAvro).collect()
        checkAnswer(spark.read.format("avro").load(dir.getAbsolutePath), result)
      }
    }
  }

  test("Throws IOException on reading corrupt Avro file if flag IGNORE_CORRUPT_FILES disabled") {
    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
      withTempPath { dir =>
        createDummyCorruptFile(dir)
        val message = intercept[org.apache.spark.SparkException] {
          spark.read.format("avro").load(dir.getAbsolutePath)
        }.getMessage

        assert(message.contains("Could not read file"))
      }
    }
  }

  test("Date field type") {
    withTempPath { dir =>
      val schema = StructType(Seq(
        StructField("float", FloatType, true),
        StructField("date", DateType, true)
      ))
      withDefaultTimeZone(UTC) {
        val rdd = spark.sparkContext.parallelize(Seq(
          Row(1f, null),
          Row(2f, new Date(1451948400000L)),
          Row(3f, new Date(1460066400500L))
        ))
        val df = spark.createDataFrame(rdd, schema)
        df.write.format("avro").save(dir.toString)
        assert(spark.read.format("avro").load(dir.toString).count() == rdd.count())
        checkAnswer(
          spark.read.format("avro").load(dir.toString).select("date"),
          Seq(Row(null), Row(new Date(1451865600000L)), Row(new Date(1459987200000L))))
      }
    }
  }

  test("Array data types") {
    withTempPath { dir =>
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
      df.write.format("avro").save(dir.toString)
      assert(spark.read.format("avro").load(dir.toString).count() == rdd.count())
    }
  }

  test("write with compression - sql configs") {
    withTempPath { dir =>
      val uncompressDir = s"$dir/uncompress"
      val bzip2Dir = s"$dir/bzip2"
      val xzDir = s"$dir/xz"
      val deflateDir = s"$dir/deflate"
      val snappyDir = s"$dir/snappy"
      val zstandardDir = s"$dir/zstandard"

      val df = spark.read.format("avro").load(testAvro)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, UNCOMPRESSED.lowerCaseName())
      df.write.format("avro").save(uncompressDir)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, BZIP2.lowerCaseName())
      df.write.format("avro").save(bzip2Dir)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, XZ.lowerCaseName())
      df.write.format("avro").save(xzDir)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, DEFLATE.lowerCaseName())
      spark.conf.set(SQLConf.AVRO_DEFLATE_LEVEL.key, "9")
      df.write.format("avro").save(deflateDir)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, SNAPPY.lowerCaseName())
      df.write.format("avro").save(snappyDir)
      spark.conf.set(SQLConf.AVRO_COMPRESSION_CODEC.key, ZSTANDARD.lowerCaseName())
      df.write.format("avro").save(zstandardDir)

      val uncompressSize = FileUtils.sizeOfDirectory(new File(uncompressDir))
      val bzip2Size = FileUtils.sizeOfDirectory(new File(bzip2Dir))
      val xzSize = FileUtils.sizeOfDirectory(new File(xzDir))
      val deflateSize = FileUtils.sizeOfDirectory(new File(deflateDir))
      val snappySize = FileUtils.sizeOfDirectory(new File(snappyDir))
      val zstandardSize = FileUtils.sizeOfDirectory(new File(zstandardDir))

      assert(uncompressSize > deflateSize)
      assert(snappySize > deflateSize)
      assert(snappySize > bzip2Size)
      assert(xzSize > bzip2Size)
      assert(uncompressSize > zstandardSize)
    }
  }

  test("dsl test") {
    val results = spark.read.format("avro").load(episodesAvro).select("title").collect()
    assert(results.length === 8)
  }

  test("old avro data source name works") {
    val results =
      spark.read.format("com.databricks.spark.avro")
        .load(episodesAvro).select("title").collect()
    assert(results.length === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = spark.read.format("avro").load(testAvro).collect()
    assert(all.length == 3)

    val str = spark.read.format("avro").load(testAvro).select("string").collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = spark.read.format("avro").load(testAvro).select("simple_map").collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = spark.read.format("avro").load(testAvro).select("union_string_null").collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = spark.read.format("avro").load(testAvro).select("union_int_long_null").collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = spark.read.format("avro").load(testAvro).select("union_float_double").collect()
    assert(
      union2
        .map(x => java.lang.Double.valueOf(x(0).toString))
        .exists(p => Math.abs(p - Math.PI) < 0.001))

    val fixed = spark.read.format("avro").load(testAvro).select("fixed3").collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enums = spark.read.format("avro").load(testAvro).select("enum").collect()
    assert(enums.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = spark.read.format("avro").load(testAvro).select("record").collect()
    assert(record(0)(0).getClass.toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean =
      spark.read.format("avro").load(testAvro).select("array_of_boolean").collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[scala.collection.Seq[Boolean]].size).toSet ==
      Set(3, 1, 0))

    val bytes = spark.read.format("avro").load(testAvro).select("bytes").collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].length).toSet == Set(3, 1, 0))
  }

  test("sql test") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW avroTable
         |USING avro
         |OPTIONS (path "${episodesAvro}")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT * FROM avroTable").collect().length === 8)
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    withTempPath { dir =>
      val avroDir = s"$dir/avro"
      spark.read.format("avro").load(testAvro).write.format("avro").save(avroDir)
      checkReloadMatchesSaved(testAvro, avroDir)
    }
  }

  test("conversion to avro and back with namespace") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    withTempPath { tempDir =>
      val name = "AvroTest"
      val namespace = "org.apache.spark.avro"
      val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

      val avroDir = s"$tempDir/namedAvro"
      spark.read.format("avro").load(testAvro)
        .write.options(parameters).format("avro").save(avroDir)
      checkReloadMatchesSaved(testAvro, avroDir)

      // Look at raw file and make sure has namespace info
      val rawSaved = spark.sparkContext.textFile(avroDir)
      val schema = rawSaved.collect().mkString("")
      assert(schema.contains(name))
      assert(schema.contains(namespace))
    }
  }

  test("SPARK-34229: Avro should read decimal values with the file schema") {
    withTempPath { path =>
      sql("SELECT 3.14 a").write.format("avro").save(path.toString)
      val data = spark.read.schema("a DECIMAL(4, 3)").format("avro").load(path.toString).collect()
      assert(data.map(_ (0)).contains(new java.math.BigDecimal("3.140")))
    }
  }

  test("SPARK-45638: Avro should read decimal values with the file schema, same scale") {
    // write schema has precision and scale as 3
    // read schema has precision as 4 and scale as 3
    withTempPath { path =>
      sql("SELECT 0.314 a").write.format("avro").save(path.toString)
      val data = spark.read.schema("a DECIMAL(4, 3)").format("avro").load(path.toString).collect()
      assert(data.map(_(0)).contains(new java.math.BigDecimal("0.314")))
    }
  }

  test("SPARK-43380: Fix Avro data type conversion" +
    " of decimal type to avoid producing incorrect results") {
    withTempPath { path =>
      val confKey = SQLConf.LEGACY_AVRO_ALLOW_INCOMPATIBLE_SCHEMA.key
      sql("SELECT 13.1234567890 a").write.format("avro").save(path.toString)
      // With the flag disabled, we will throw an exception if there is a mismatch
      withSQLConf(confKey -> "false") {
        val ex = intercept[SparkException] {
          spark.read.schema("a DECIMAL(4, 3)").format("avro").load(path.toString).collect()
        }
        assert(ex.getCondition.startsWith("FAILED_READ_FILE"))
        checkError(
          exception = ex.getCause.asInstanceOf[AnalysisException],
          condition = "AVRO_INCOMPATIBLE_READ_TYPE",
          parameters = Map("avroPath" -> "field 'a'",
            "sqlPath" -> "field 'a'",
            "avroType" -> "decimal\\(12,10\\)",
            "sqlType" -> "\"DECIMAL\\(4,3\\)\""),
          matchPVals = true
        )
      }
      // The following used to work, so it should still work with the flag enabled
      checkAnswer(
        spark.read.schema("a DECIMAL(5, 3)").format("avro").load(path.toString),
        Row(new java.math.BigDecimal("13.123"))
      )
      withSQLConf(confKey -> "true") {
        // With the flag enabled, we return a null silently, which isn't great
        checkAnswer(
          spark.read.schema("a DECIMAL(4, 3)").format("avro").load(path.toString),
          Row(null)
        )
        checkAnswer(
          spark.read.schema("a DECIMAL(5, 3)").format("avro").load(path.toString),
          Row(new java.math.BigDecimal("13.123"))
        )
      }
    }
  }

  test("SPARK-49082: Widening type promotions in AvroDeserializer") {
    withTempPath { tempPath =>
      // Int -> Long
      val intPath = s"$tempPath/int_data"
      val intDf = Seq(1, Int.MinValue, Int.MaxValue).toDF("col")
      intDf.write.format("avro").save(intPath)
      checkAnswer(
        spark.read.schema("col Long").format("avro").load(intPath),
        Seq(Row(1L), Row(-2147483648L), Row(2147483647L))
      )

      // Int -> Double
      checkAnswer(
        spark.read.schema("col Double").format("avro").load(intPath),
        Seq(Row(1D), Row(-2147483648D), Row(2147483647D))
      )

      // Float -> Double
      val floatPath = s"$tempPath/float_data"
      val floatDf = Seq(1F,
        Float.MinValue, Float.MinPositiveValue, Float.MaxValue,
        Float.NaN, Float.NegativeInfinity, Float.PositiveInfinity
      ).toDF("col")
      floatDf.write.format("avro").save(floatPath)
      checkAnswer(
        spark.read.schema("col Double").format("avro").load(floatPath),
        Seq(Row(1D),
          Row(-3.4028234663852886E38D), Row(1.401298464324817E-45D), Row(3.4028234663852886E38D),
          Row(Double.NaN), Row(Double.NegativeInfinity), Row(Double.PositiveInfinity))
      )
    }
  }

  test("SPARK-43380: Fix Avro data type conversion" +
    " of DayTimeIntervalType to avoid producing incorrect results") {
    withTempPath { path =>
      val confKey = SQLConf.LEGACY_AVRO_ALLOW_INCOMPATIBLE_SCHEMA.key
      val schema = StructType(Array(StructField("a", DayTimeIntervalType(), false)))
      val data = Seq(Row(java.time.Duration.ofDays(1).plusSeconds(1)))

      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
      df.write.format("avro").save(path.getCanonicalPath)

      withSQLConf(confKey -> "false") {
        Seq("DATE", "TIMESTAMP", "TIMESTAMP_NTZ").foreach { sqlType =>
          val ex = intercept[SparkException] {
            spark.read.schema(s"a $sqlType").format("avro").load(path.toString).collect()
          }
          assert(ex.getCondition.startsWith("FAILED_READ_FILE"))
          checkError(
            exception = ex.getCause.asInstanceOf[AnalysisException],
            condition = "AVRO_INCOMPATIBLE_READ_TYPE",
            parameters = Map("avroPath" -> "field 'a'",
              "sqlPath" -> "field 'a'",
              "avroType" -> "interval day to second",
              "sqlType" -> s""""$sqlType""""),
            matchPVals = true
          )
        }
      }

      withSQLConf(confKey -> "true") {
        // Allow conversion and do not need to check result
        spark.read.schema("a Date").format("avro").load(path.toString)
        spark.read.schema("a timestamp").format("avro").load(path.toString)
        spark.read.schema("a timestamp_ntz").format("avro").load(path.toString)
      }
    }
  }

  test("SPARK-43380: Fix Avro data type conversion" +
    " of YearMonthIntervalType to avoid producing incorrect results") {
    withTempPath { path =>
      val confKey = SQLConf.LEGACY_AVRO_ALLOW_INCOMPATIBLE_SCHEMA.key
      val schema = StructType(Array(StructField("a", YearMonthIntervalType(), false)))
      val data = Seq(Row(java.time.Period.of(1, 1, 0)))

      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
      df.write.format("avro").save(path.getCanonicalPath)

      withSQLConf(confKey -> "false") {
        Seq("DATE", "TIMESTAMP", "TIMESTAMP_NTZ").foreach { sqlType =>
          val ex = intercept[SparkException] {
            spark.read.schema(s"a $sqlType").format("avro").load(path.toString).collect()
          }
          assert(ex.getCondition.startsWith("FAILED_READ_FILE"))
          checkError(
            exception = ex.getCause.asInstanceOf[AnalysisException],
            condition = "AVRO_INCOMPATIBLE_READ_TYPE",
            parameters = Map("avroPath" -> "field 'a'",
              "sqlPath" -> "field 'a'",
              "avroType" -> "interval year to month",
              "sqlType" -> s""""$sqlType""""),
            matchPVals = true
          )
        }
      }

      withSQLConf(confKey -> "true") {
        // Allow conversion and do not need to check result
        spark.read.schema("a Date").format("avro").load(path.toString)
        spark.read.schema("a timestamp").format("avro").load(path.toString)
        spark.read.schema("a timestamp_ntz").format("avro").load(path.toString)
      }
    }
  }

  Seq(
    "time-millis",
    "time-micros",
    "timestamp-micros",
    "timestamp-millis",
    "local-timestamp-millis",
    "local-timestamp-micros"
  ).foreach { timeLogicalType =>
    test(s"converting $timeLogicalType type to long in avro") {
      withTempPath { path =>
        val df = Seq(100L)
          .toDF("dt")
        val avroSchema =
          s"""
             |{
             |  "type" : "record",
             |  "name" : "test_schema",
             |  "fields" : [
             |    {"name": "dt", "type": {"type": "long", "logicalType": "$timeLogicalType"}}
             |  ]
             |}""".stripMargin
        df.write.format("avro").option("avroSchema", avroSchema).save(path.getCanonicalPath)
        checkAnswer(
          spark.read.schema(s"dt long").format("avro").load(path.toString),
          Row(100L))
      }
    }
  }

  test("converting some specific sparkSQL types to avro") {
    withTempPath { tempDir =>
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

      val avroDir = s"$tempDir/avro"
      cityDataFrame.write.format("avro").save(avroDir)
      assert(spark.read.format("avro").load(avroDir).collect().length == 3)

      // TimesStamps are converted to longs
      val times = spark.read.format("avro").load(avroDir).select("Time").collect()
      assert(times.map(_(0)).toSet ==
        Set(new Timestamp(666), new Timestamp(777), new Timestamp(42)))

      // DecimalType should be converted to string
      val decimals = spark.read.format("avro").load(avroDir).select("Decimal").collect()
      assert(decimals.map(_(0)).contains(new java.math.BigDecimal("3.14")))

      // There should be a null entry
      val length = spark.read.format("avro").load(avroDir).select("Length").collect()
      assert(length.map(_(0)).contains(null))

      val binary = spark.read.format("avro").load(avroDir).select("Binary").collect()
      for (i <- arrayOfByte.indices) {
        assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
      }
    }
  }

  test("correctly read long as date/timestamp type") {
    withTempPath { tempDir =>
      val currentTime = new Timestamp(System.currentTimeMillis())
      val currentDate = new Date(System.currentTimeMillis())
      val schema = StructType(Seq(
        StructField("_1", DateType, false), StructField("_2", TimestampType, false)))
      val writeDs = Seq((currentDate, currentTime)).toDS()

      val avroDir = s"$tempDir/avro"
      writeDs.write.format("avro").save(avroDir)
      assert(spark.read.format("avro").load(avroDir).collect().length == 1)

      val readDs = spark.read.schema(schema).format("avro").load(avroDir).as[(Date, Timestamp)]

      assert(readDs.collect().sameElements(writeDs.collect()))
    }
  }

  test("support of globbed paths") {
    val resourceDir = testFile(".")
    val e1 = spark.read.format("avro").load(resourceDir + "../*/episodes.avro").collect()
    assert(e1.length == 8)

    val e2 = spark.read.format("avro").load(resourceDir + "../../*/*/episodes.avro").collect()
    assert(e2.length == 8)
  }

  test("does not coerce null date/timestamp value to 0 epoch.") {
    withTempPath { tempDir =>
      val nullTime: Timestamp = null
      val nullDate: Date = null
      val schema = StructType(Seq(
        StructField("_1", DateType, nullable = true),
        StructField("_2", TimestampType, nullable = true))
      )
      val writeDs = Seq((nullDate, nullTime)).toDS()

      val avroDir = s"$tempDir/avro"
      writeDs.write.format("avro").save(avroDir)
      val readValues =
        spark.read.schema(schema).format("avro").load(avroDir).as[(Date, Timestamp)].collect()

      assert(readValues.length == 1)
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
    val result = spark
      .read
      .option("avroSchema", avroSchema)
      .format("avro")
      .load(testAvro)
      .collect()
    val expected = spark.read.format("avro").load(testAvro).select("string").collect()
    assert(result.sameElements(expected))
  }

  test("SPARK-34416: support user provided avro schema url") {
    val avroSchemaUrl = testFile("test_sub.avsc")
    val result = spark.read.option("avroSchemaUrl", avroSchemaUrl)
      .format("avro")
      .load(testAvro)
      .collect()
    val expected = spark.read.format("avro").load(testAvro).select("string").collect()
    assert(result.sameElements(expected))
  }

  test("SPARK-34416: support user provided both avro schema and avro schema url") {
    val avroSchemaUrl = testFile("test_sub.avsc")
    val avroSchema =
      """
        |{
        |  "type" : "record",
        |  "name" : "test_schema",
        |  "fields" : [{
        |    "name" : "union_int_long_null",
        |    "type" : ["int", "long", "null"]
        |  }]
        |}
      """.stripMargin

    val result = spark.read
      .option("avroSchema", avroSchema)
      .option("avroSchemaUrl", avroSchemaUrl)
      .format("avro")
      .load(testAvro)
      .collect()
    val expected = spark.read.format("avro").load(testAvro).select("union_int_long_null").collect()
    assert(result.sameElements(expected))
  }

  test("SPARK-34416: support user provided wrong avro schema url") {
    val e = intercept[FileNotFoundException] {
      spark.read
        .option("avroSchemaUrl", "not_exists.avsc")
        .format("avro")
        .load(testAvro)
        .collect()
    }

    assertExceptionMsg[FileNotFoundException](e, "File not_exists.avsc does not exist")
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
    val result = spark
      .read
      .option("avroSchema", avroSchema)
      .format("avro").load(testAvro).select("missingField").first()
    assert(result === Row("foo"))
  }

  test("support user provided avro schema for writing nullable enum type") {
    withTempPath { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [{
          |    "name": "Suit",
          |    "type": [{ "type": "enum",
          |              "name": "SuitEnumType",
          |              "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
          |            }, "null"]
          |  }]
          |}
        """.stripMargin

      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row("SPADES"), Row(null), Row("HEARTS"), Row("DIAMONDS"),
        Row(null), Row("CLUBS"), Row("HEARTS"), Row("SPADES"))),
        StructType(Seq(StructField("Suit", StringType, true))))

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").option("avroSchema", avroSchema).save(tempSaveDir)

      checkAnswer(df, spark.read.format("avro").load(tempSaveDir))
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempSaveDir))

      // Writing df containing data not in the enum will throw an exception
      val e = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row("SPADES"), Row("NOT-IN-ENUM"), Row("HEARTS"), Row("DIAMONDS"))),
          StructType(Seq(StructField("Suit", StringType, true))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e,
        """"NOT-IN-ENUM" cannot be written since it's not defined in enum""")
    }
  }

  test("support user provided avro schema for writing non-nullable enum type") {
    withTempPath { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [{
          |    "name": "Suit",
          |    "type": { "type": "enum",
          |              "name": "SuitEnumType",
          |              "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
          |            }
          |  }]
          |}
        """.stripMargin

      val dfWithNull = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row("SPADES"), Row(null), Row("HEARTS"), Row("DIAMONDS"),
        Row(null), Row("CLUBS"), Row("HEARTS"), Row("SPADES"))),
        StructType(Seq(StructField("Suit", StringType, true))))

      val df = spark.createDataFrame(dfWithNull.na.drop().rdd,
        StructType(Seq(StructField("Suit", StringType, false))))

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").option("avroSchema", avroSchema).save(tempSaveDir)

      checkAnswer(df, spark.read.format("avro").load(tempSaveDir))
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempSaveDir))

      // Writing df containing nulls without using avro union type will
      // throw an exception as avro uses union type to handle null.
      val e1 = intercept[SparkException] {
        dfWithNull.write.format("avro")
          .option("avroSchema", avroSchema).save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[AvroTypeException](e1, "value null is not a SuitEnumType")

      // Writing df containing data not in the enum will throw an exception
      val e2 = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row("SPADES"), Row("NOT-IN-ENUM"), Row("HEARTS"), Row("DIAMONDS"))),
          StructType(Seq(StructField("Suit", StringType, false))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e2,
        """"NOT-IN-ENUM" cannot be written since it's not defined in enum""")
    }
  }

  test("support user provided avro schema for writing nullable fixed type") {
    withTempPath { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [{
          |    "name": "fixed2",
          |    "type": [{ "type": "fixed",
          |               "size": 2,
          |               "name": "fixed2"
          |            }, "null"]
          |  }]
          |}
        """.stripMargin

      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(Array(192, 168).map(_.toByte)), Row(null))),
        StructType(Seq(StructField("fixed2", BinaryType, true))))

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").option("avroSchema", avroSchema).save(tempSaveDir)

      checkAnswer(df, spark.read.format("avro").load(tempSaveDir))
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempSaveDir))

      // Writing df containing binary data that doesn't fit FIXED size will throw an exception
      val e1 = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(Array(192, 168, 1).map(_.toByte)))),
          StructType(Seq(StructField("fixed2", BinaryType, true))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e1,
        "3 bytes of binary data cannot be written into FIXED type with size of 2 bytes")

      // Writing df containing binary data that doesn't fit FIXED size will throw an exception
      val e2 = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(Array(192).map(_.toByte)))),
          StructType(Seq(StructField("fixed2", BinaryType, true))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e2,
        "1 byte of binary data cannot be written into FIXED type with size of 2 bytes")
    }
  }

  test("support user provided avro schema for writing non-nullable fixed type") {
    withTempPath { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [{
          |    "name": "fixed2",
          |    "type": { "type": "fixed",
          |               "size": 2,
          |               "name": "fixed2"
          |            }
          |  }]
          |}
        """.stripMargin

      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(Array(192, 168).map(_.toByte)), Row(Array(1, 1).map(_.toByte)))),
        StructType(Seq(StructField("fixed2", BinaryType, false))))

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").option("avroSchema", avroSchema).save(tempSaveDir)

      checkAnswer(df, spark.read.format("avro").load(tempSaveDir))
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempSaveDir))

      // Writing df containing binary data that doesn't fit FIXED size will throw an exception
      val e1 = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(Array(192, 168, 1).map(_.toByte)))),
          StructType(Seq(StructField("fixed2", BinaryType, false))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e1,
        "3 bytes of binary data cannot be written into FIXED type with size of 2 bytes")

      // Writing df containing binary data that doesn't fit FIXED size will throw an exception
      val e2 = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(Array(192).map(_.toByte)))),
          StructType(Seq(StructField("fixed2", BinaryType, false))))
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assertExceptionMsg[IncompatibleSchemaException](e2,
        "1 byte of binary data cannot be written into FIXED type with size of 2 bytes")
    }
  }

  test("support user provided avro schema for writing / reading fields with different ordering") {
    withTempPath { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [
          |    {"name": "Age", "type": "int"},
          |    {"name": "Name", "type": "string"}
          |  ]
          |}
        """.stripMargin

      val avroSchemaReversed =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [
          |    {"name": "Name", "type": "string"},
          |    {"name": "Age", "type": "int"}
          |  ]
          |}
        """.stripMargin

      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(2, "Aurora"))),
        StructType(Seq(
          StructField("Age", IntegerType, false),
          StructField("Name", StringType, false))))

      val tempSaveDir = s"$tempDir/save/"

      // Writing avro file with reversed field ordering
      df.write.format("avro").option("avroSchema", avroSchemaReversed).save(tempSaveDir)

      // Reading reversed avro file
      checkAnswer(df.select("Name", "Age"), spark.read.format("avro").load(tempSaveDir))
      checkAvroSchemaEquals(avroSchemaReversed, getAvroSchemaStringFromFiles(tempSaveDir))

      // Reading reversed avro file with provided original schema
      val avroDf = spark.read.format("avro").option("avroSchema", avroSchema).load(tempSaveDir)
      checkAnswer(df, avroDf)
      assert(avroDf.schema.fieldNames.sameElements(Array("Age", "Name")))
    }
  }

  test("support user provided non-nullable avro schema " +
    "for nullable catalyst schema without any null record") {
    withTempPath { tempDir =>
      val catalystSchema =
        StructType(Seq(
          StructField("Age", IntegerType, true),
          StructField("Name", StringType, true)))

      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [
          |    {"name": "Age", "type": "int"},
          |    {"name": "Name", "type": "string"}
          |  ]
          |}
        """.stripMargin

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(2, "Aurora"))), catalystSchema)

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").option("avroSchema", avroSchema).save(tempSaveDir)
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempSaveDir))

      val ex = intercept[SparkException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(2, null))), catalystSchema)
          .write.format("avro").option("avroSchema", avroSchema)
          .save(s"$tempDir/${UUID.randomUUID()}")
      }
      assert(ex.getCondition == "TASK_WRITE_FAILED")
      assert(ex.getCause.isInstanceOf[java.lang.NullPointerException])
      assert(ex.getCause.getMessage.contains(
        "null value for (non-nullable) string at test_schema.Name"))
    }
  }

  test("support user provided nullable avro schema " +
    "for non-nullable catalyst schema without any null record") {
    val catalystSchema =
      StructType(Seq(
        StructField("Age", IntegerType, nullable = false),
        StructField("Name", StringType, nullable = false)))

    val avroSchema = """
      |{
      |  "type" : "record",
      |  "name" : "test_schema",
      |  "fields" : [
      |    {"name": "Age", "type": ["null", "int"]},
      |    {"name": "Name", "type": ["null", "string"]}
      |  ]
      |}
    """.stripMargin

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(2, "Aurora"))), catalystSchema)

    withTempPath { tempDir =>
      df.write.format("avro").option("avroSchema", avroSchema).save(tempDir.getPath)
      checkAvroSchemaEquals(avroSchema, getAvroSchemaStringFromFiles(tempDir.getPath))
    }
  }

  test("SPARK-34365: support reading renamed schema using positionalFieldMatching") {
    val renamedSchema = new StructType()
      .add("foo", StringType)
      .add("foo_map", MapType(StringType, IntegerType))
    val dfLoaded = spark
      .read
      .option("positionalFieldMatching", true.toString)
      .schema(renamedSchema)
      .format("avro")
      .load(testAvro)
    assert(dfLoaded.schema === renamedSchema)
    val expectedDf = spark.read.format("avro").load(testAvro).select("string", "simple_map")
    assert(dfLoaded.select($"foo".as("string"), $"foo_map".as("simple_map")).collect().toSet ===
      expectedDf.collect().toSet)
  }

  test("SPARK-34365: support writing with renamed schema using positionalFieldMatching") {
    withTempDir { tempDir =>
      val avroSchema = SchemaBuilder.record("renamed").fields()
        .requiredString("foo")
        .name("foo_map").`type`(Schema.createMap(Schema.create(Schema.Type.INT))).noDefault()
        .endRecord()
      val expectedDf = spark.read.format("avro").load(testAvro).select("string", "simple_map")
      val savePath = s"$tempDir/save"
      expectedDf.write
        .option("avroSchema", avroSchema.toString)
        .option("positionalFieldMatching", true.toString)
        .format("avro")
        .save(savePath)
      val reloadedDf = spark.read.format("avro").load(savePath)
      assert(reloadedDf.schema ===
        new StructType().add("foo", StringType).add("foo_map", MapType(StringType, IntegerType)))
      assert(reloadedDf.select($"foo".as("string"), $"foo_map".as("simple_map")).collect().toSet ===
        expectedDf.collect().toSet)
    }
  }

  test("int/long double/float conversion") {
    val catalystSchema =
      StructType(Seq(
        StructField("Age", LongType),
        StructField("Length", DoubleType),
        StructField("Name", StringType)))

    for (optionalNull <- Seq(""""null",""", "")) {
      val avroSchema = s"""
        |{
        |  "type" : "record",
        |  "name" : "test_schema",
        |  "fields" : [
        |    {"name": "Age", "type": [$optionalNull "int", "long"]},
        |    {"name": "Length", "type": [$optionalNull "float", "double"]},
        |    {"name": "Name", "type": ["null", "string"]}
        |  ]
        |}
      """.stripMargin

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(2L, 1.8D, "Aurora"), Row(1L, 0.9D, null))),
        catalystSchema)

      withTempPath { tempDir =>
        df.write.format("avro").option("avroSchema", avroSchema).save(tempDir.getPath)
        checkAnswer(
          spark.read
            .format("avro")
            .option("avroSchema", avroSchema)
            .load(tempDir.getPath),
          df)
      }
    }
  }

  test("non-matching complex union types") {
    val catalystSchema = new StructType().add("Union", new StructType()
      .add("member0", IntegerType)
      .add("member1", new StructType().add("f1", StringType, nullable = false))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Row(1, null)))), catalystSchema)

    val recordS = SchemaBuilder.record("r").fields().requiredString("f1").endRecord()
    val intS = Schema.create(Schema.Type.INT)
    val nullS = Schema.create(Schema.Type.NULL)
    for ((unionTypes, compatible) <- Seq(
      (Seq(nullS, intS, recordS), true),
      (Seq(intS, nullS, recordS), true),
      (Seq(intS, recordS, nullS), true),
      (Seq(intS, recordS), true),
      (Seq(nullS, recordS, intS), false),
      (Seq(nullS, recordS), false),
      (Seq(nullS, SchemaBuilder.record("r").fields().requiredString("f2").endRecord()), false)
    )) {
      val avroSchema = SchemaBuilder.record("test_schema").fields()
        .name("union").`type`(Schema.createUnion(unionTypes: _*)).noDefault()
        .endRecord().toString()

      withTempPath { tempDir =>
        if (!compatible) {
          intercept[SparkException] {
            df.write.format("avro").option("avroSchema", avroSchema).save(tempDir.getPath)
          }
        } else {
          df.write.format("avro").option("avroSchema", avroSchema).save(tempDir.getPath)
          checkAnswer(
            spark.read
              .format("avro")
              .option("avroSchema", avroSchema)
              .load(tempDir.getPath),
            df)
        }
      }
    }
  }

  test("error handling for unsupported Interval data types") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      withTempDir { dir =>
        val tempDir = new File(dir, "files").getCanonicalPath
        checkError(
          exception = intercept[AnalysisException] {
            sql("select interval 1 days").write.format("avro").mode("overwrite").save(tempDir)
          },
          condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
          parameters = Map(
            "format" -> "Avro",
            "columnName" -> "`INTERVAL '1 days'`",
            "columnType" -> "\"INTERVAL\""
        )
        )
        checkError(
          exception = intercept[AnalysisException] {
            spark.udf.register("testType", () => new IntervalData())
            sql("select testType()").write.format("avro").mode("overwrite").save(tempDir)
          },
          condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
          parameters = Map(
            "columnName" -> "`testType()`",
            "columnType" -> "UDT(\"INTERVAL\")",
            "format" -> "Avro")
        )
      }
    }
  }

  test("support Null data types") {
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      val df = sql("select null")
      df.write.format("avro").mode("overwrite").save(tempDir)
      checkAnswer(spark.read.format("avro").load(tempDir), df)
    }
  }

  test("throw exception if unable to write with user provided Avro schema") {
    val input: Seq[(DataType, Schema.Type)] = Seq(
      (NullType, NULL),
      (BooleanType, BOOLEAN),
      (ByteType, INT),
      (ShortType, INT),
      (IntegerType, INT),
      (LongType, LONG),
      (FloatType, FLOAT),
      (DoubleType, DOUBLE),
      (BinaryType, BYTES),
      (DateType, INT),
      (TimestampType, LONG),
      (DecimalType(4, 2), BYTES)
    )
    def assertException(f: () => AvroSerializer): Unit = {
      val message = intercept[org.apache.spark.sql.avro.IncompatibleSchemaException] {
        f()
      }.getMessage
      assert(message.contains("Cannot convert SQL type"))
    }

    def resolveNullable(schema: Schema, nullable: Boolean): Schema = {
      if (nullable && schema.getType != NULL) {
        Schema.createUnion(schema, Schema.create(NULL))
      } else {
        schema
      }
    }
    for {
      i <- input
      j <- input
      nullable <- Seq(true, false)
    } if (i._2 != j._2) {
      val avroType = resolveNullable(Schema.create(j._2), nullable)
      val avroArrayType = resolveNullable(Schema.createArray(avroType), nullable)
      val avroMapType = resolveNullable(Schema.createMap(avroType), nullable)
      val name = "foo"
      val avroField = new Field(name, avroType, "", null.asInstanceOf[AnyVal])
      val recordSchema = Schema.createRecord("name", "doc", "space", true, Seq(avroField).asJava)
      val avroRecordType = resolveNullable(recordSchema, nullable)

      val catalystType = i._1
      val catalystArrayType = ArrayType(catalystType, nullable)
      val catalystMapType = MapType(StringType, catalystType, nullable)
      val catalystStructType = StructType(Seq(StructField(name, catalystType, nullable)))

      for {
        avro <- Seq(avroType, avroArrayType, avroMapType, avroRecordType)
        catalyst <- Seq(catalystType, catalystArrayType, catalystMapType, catalystStructType)
      } {
        assertException(() => new AvroSerializer(catalyst, avro, nullable))
      }
    }
  }

  test("reading from invalid path throws exception") {

    // Directory given has no avro files
    intercept[AnalysisException] {
      withTempPath(dir => spark.read.format("avro").load(dir.getCanonicalPath))
    }

    intercept[AnalysisException] {
      spark.read.format("avro").load("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[AnalysisException] {
      spark.read.format("avro").load("*/*/*/*/*/*/*/something.avro")
    }

    intercept[FileNotFoundException] {
      withTempDir { dir =>
        FileUtils.touch(new File(dir, "test"))
        withSQLConf(AvroFileFormat.IgnoreFilesWithoutExtensionProperty -> "true") {
          spark.read.format("avro").load(dir.toString)
        }
      }
    }

    intercept[FileNotFoundException] {
      withTempDir { dir =>
        FileUtils.touch(new File(dir, "test"))

        spark
          .read
          .option("ignoreExtension", false)
          .format("avro")
          .load(dir.toString)
      }
    }
  }

  test("SQL test insert overwrite") {
    withTempPath { tempDir =>
      val tempEmptyDir = s"$tempDir/sqlOverwrite"
      // Create a temp directory for table that will be overwritten
      new File(tempEmptyDir).mkdirs()
      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW episodes
           |USING avro
           |OPTIONS (path "${episodesAvro}")
         """.stripMargin.replaceAll("\n", " "))
      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW episodesEmpty
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
    withTempPath { tempDir =>
      val df = spark.read.format("avro").load(episodesAvro)
      assert(df.count() == 8)

      val tempSaveDir = s"$tempDir/save/"

      df.write.format("avro").save(tempSaveDir)
      val newDf = spark.read.format("avro").load(tempSaveDir)
      assert(newDf.count() == 8)
    }
  }

  test("test load with non-Avro file") {
    // Test if load works as expected
    withTempPath { tempDir =>
      val df = spark.read.format("avro").load(episodesAvro)
      assert(df.count() == 8)

      val tempSaveDir = s"$tempDir/save/"
      df.write.format("avro").save(tempSaveDir)

      Files.createFile(new File(tempSaveDir, "non-avro").toPath)

      withSQLConf(AvroFileFormat.IgnoreFilesWithoutExtensionProperty -> "true") {
        val newDf = spark.read.format("avro").load(tempSaveDir)
        assert(newDf.count() == 8)
      }
    }
  }

  test("SPARK-34378: support writing user provided avro schema with missing optional fields") {
    withTempDir { tempDir =>
      val avroSchema = SchemaBuilder.builder().record("test").fields()
        .requiredString("f1").optionalString("f2").endRecord().toString()

      val data = Seq("foo", "bar")

      // Fail if required field f1 is missing
      val e = intercept[SparkException] {
        data.toDF("f2").write.option("avroSchema", avroSchema).format("avro").save(s"$tempDir/fail")
      }
      assertExceptionMsg[IncompatibleSchemaException](e,
        "Found field 'f1' in Avro schema but there is no match in the SQL schema")

      val tempSaveDir = s"$tempDir/save/"
      // Succeed if optional field f2 is missing
      data.toDF("f1").write.option("avroSchema", avroSchema).format("avro").save(tempSaveDir)

      val newDf = spark.read.format("avro").load(tempSaveDir)
      assert(newDf.schema === new StructType().add("f1", StringType).add("f2", StringType))
      val rows = newDf.collect()
      assert(rows.map(_.getAs[String]("f1")).sorted === data.sorted)
      rows.foreach(row => assert(row.isNullAt(1)))
    }
  }

  test("SPARK-34133: Reading user provided schema respects case sensitivity for field matching") {
    val wrongCaseSchema = new StructType()
        .add("STRING", StringType, nullable = false)
        .add("UNION_STRING_NULL", StringType, nullable = true)
    val withSchema = spark.read
        .schema(wrongCaseSchema)
        .format("avro").load(testAvro).collect()

    val withOutSchema = spark.read.format("avro").load(testAvro)
        .select("STRING", "UNION_STRING_NULL")
        .collect()
    assert(withSchema.sameElements(withOutSchema))

    withSQLConf((SQLConf.CASE_SENSITIVE.key, "true")) {
      val  out = spark.read.format("avro").schema(wrongCaseSchema).load(testAvro).collect()
      assert(out.forall(_.isNullAt(0)))
      assert(out.forall(_.isNullAt(1)))
    }
  }

  test("SPARK-34133: Writing user provided schema respects case sensitivity for field matching") {
    withTempDir { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [
          |    {"name": "foo", "type": "int"},
          |    {"name": "BAR", "type": "int"}
          |  ]
          |}
      """.stripMargin
      val df = Seq((1, 3), (2, 4)).toDF("FOO", "bar")

      val savePath = s"$tempDir/save"
      df.write.option("avroSchema", avroSchema).format("avro").save(savePath)

      val loaded = spark.read.format("avro").load(savePath)
      assert(loaded.schema === new StructType().add("foo", IntegerType).add("BAR", IntegerType))
      assert(loaded.collect().map(_.getInt(0)).toSet === Set(1, 2))
      assert(loaded.collect().map(_.getInt(1)).toSet === Set(3, 4))

      withSQLConf((SQLConf.CASE_SENSITIVE.key, "true")) {
        val e = intercept[SparkException] {
          df.write.option("avroSchema", avroSchema).format("avro").save(s"$tempDir/save2")
        }
        assertExceptionMsg[IncompatibleSchemaException](e, "Cannot find field 'FOO' in Avro schema")
      }
    }
  }

  test("SPARK-34133: Writing user provided schema with multiple matching Avro fields fails") {
    withTempDir { tempDir =>
      val avroSchema =
        """
          |{
          |  "type" : "record",
          |  "name" : "test_schema",
          |  "fields" : [
          |    {"name": "foo", "type": "int"},
          |    {"name": "FOO", "type": "string"}
          |  ]
          |}
      """.stripMargin

      val errorMsg = "Searching for 'foo' in Avro schema at top-level record gave 2 matches. " +
          "Candidates: [foo, FOO]"
      assertExceptionMsg(intercept[SparkException] {
        val fooBarDf = Seq((1, "3"), (2, "4")).toDF("foo", "bar")
        fooBarDf.write.option("avroSchema", avroSchema).format("avro").save(s"$tempDir/save-fail")
      }, errorMsg)

      val savePath = s"$tempDir/save"
      withSQLConf((SQLConf.CASE_SENSITIVE.key, "true")) {
        val fooFooDf = Seq((1, "3"), (2, "4")).toDF("foo", "FOO")
        fooFooDf.write.option("avroSchema", avroSchema).format("avro").save(savePath)

        val loadedDf = spark.read.format("avro").schema(fooFooDf.schema).load(savePath)
        assert(loadedDf.collect().toSet === fooFooDf.collect().toSet)
      }

      assertExceptionMsg(intercept[SparkException] {
        val fooSchema = new StructType().add("foo", IntegerType)
        spark.read.format("avro").schema(fooSchema).load(savePath).collect()
      }, errorMsg)
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
    val withSchema = spark.read.schema(partialColumns).format("avro").load(testAvro).collect()
    val withOutSchema = spark
      .read
      .format("avro")
      .load(testAvro)
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
    val withEmptyColumn = spark.read.schema(schema).format("avro").load(testAvro).collect()

    assert(withEmptyColumn.forall(_ == Row(null: String, Row(null: String, null: String))))
  }

  test("read avro file partitioned") {
    withTempPath { dir =>
      val df = (0 to 1024 * 3).toDS().map(i => s"record${i}").toDF("records")
      val outputDir = s"$dir/${UUID.randomUUID}"
      df.write.format("avro").save(outputDir)
      val input = spark.read.format("avro").load(outputDir)
      assert(input.collect().toSet.size === 1024 * 3 + 1)
      assert(input.rdd.partitions.length > 2)
    }
  }

  case class NestedBottom(id: Int, data: String)

  case class NestedMiddle(id: Int, data: NestedBottom)

  case class NestedTop(id: Int, data: NestedMiddle)

  test("Validate namespace in avro file that has nested records with the same name") {
    withTempPath { dir =>
      val writeDf = spark.createDataFrame(List(NestedTop(1, NestedMiddle(2, NestedBottom(3, "1")))))
      writeDf.write.format("avro").save(dir.toString)
      val schema = getAvroSchemaStringFromFiles(dir.toString)
      assert(schema.contains("\"namespace\":\"topLevelRecord\""))
      assert(schema.contains("\"namespace\":\"topLevelRecord.data\""))
    }
  }

  test("saving avro that has nested records with the same name") {
    withTempPath { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(List(NestedTop(1, NestedMiddle(2, NestedBottom(3, "1")))))
      val outputFolder = s"$tempDir/duplicate_names/"
      writeDf.write.format("avro").save(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.format("avro").load(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }

  test("check namespace - toAvroType") {
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("address", StructType(Seq(
        StructField("city", StringType, nullable = false),
        StructField("state", StringType, nullable = false))),
        nullable = false)))
    val employeeType = SchemaConverters.toAvroType(sparkSchema,
      recordName = "employee",
      nameSpace = "foo.bar")

    assert(employeeType.getFullName == "foo.bar.employee")
    assert(employeeType.getName == "employee")
    assert(employeeType.getNamespace == "foo.bar")

    val addressType = employeeType.getField("address").schema()
    assert(addressType.getFullName == "foo.bar.employee.address")
    assert(addressType.getName == "address")
    assert(addressType.getNamespace == "foo.bar.employee")
  }

  test("check empty namespace - toAvroType") {
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("address", StructType(Seq(
        StructField("city", StringType, nullable = false),
        StructField("state", StringType, nullable = false))),
        nullable = false)))
    val employeeType = SchemaConverters.toAvroType(sparkSchema,
      recordName = "employee")

    assert(employeeType.getFullName == "employee")
    assert(employeeType.getName == "employee")
    assert(employeeType.getNamespace == null)

    val addressType = employeeType.getField("address").schema()
    assert(addressType.getFullName == "employee.address")
    assert(addressType.getName == "address")
    assert(addressType.getNamespace == "employee")
  }

  case class NestedMiddleArray(id: Int, data: Array[NestedBottom])

  case class NestedTopArray(id: Int, data: NestedMiddleArray)

  test("saving avro that has nested records with the same name inside an array") {
    withTempPath { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(
        List(NestedTopArray(1, NestedMiddleArray(2, Array(
          NestedBottom(3, "1"), NestedBottom(4, "2")
        ))))
      )
      val outputFolder = s"$tempDir/duplicate_names_array/"
      writeDf.write.format("avro").save(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.format("avro").load(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }

  case class NestedMiddleMap(id: Int, data: Map[String, NestedBottom])

  case class NestedTopMap(id: Int, data: NestedMiddleMap)

  test("saving avro that has nested records with the same name inside a map") {
    withTempPath { tempDir =>
      // Save avro file on output folder path
      val writeDf = spark.createDataFrame(
        List(NestedTopMap(1, NestedMiddleMap(2, Map(
          "1" -> NestedBottom(3, "1"), "2" -> NestedBottom(4, "2")
        ))))
      )
      val outputFolder = s"$tempDir/duplicate_names_map/"
      writeDf.write.format("avro").save(outputFolder)
      // Read avro file saved on the last step
      val readDf = spark.read.format("avro").load(outputFolder)
      // Check if the written DataFrame is equals than read DataFrame
      assert(readDf.collect().sameElements(writeDf.collect()))
    }
  }

  test("SPARK-24805: do not ignore files without .avro extension by default") {
    withTempDir { dir =>
      Files.copy(
        Paths.get(new URI(episodesAvro)),
        Paths.get(dir.getCanonicalPath, "episodes"))

      val fileWithoutExtension = s"${dir.getCanonicalPath}/episodes"
      val df1 = spark.read.format("avro").load(fileWithoutExtension)
      assert(df1.count() == 8)

      val schema = new StructType()
        .add("title", StringType)
        .add("air_date", StringType)
        .add("doctor", IntegerType)
      val df2 = spark.read.schema(schema).format("avro").load(fileWithoutExtension)
      assert(df2.count() == 8)
    }
  }

  test("SPARK-24836: checking the ignoreExtension option") {
    withTempPath { tempDir =>
      val df = spark.read.format("avro").load(episodesAvro)
      assert(df.count() == 8)

      val tempSaveDir = s"$tempDir/save/"
      df.write.format("avro").save(tempSaveDir)

      Files.createFile(new File(tempSaveDir, "non-avro").toPath)

      val newDf = spark
        .read
        .option("ignoreExtension", false)
        .format("avro")
        .load(tempSaveDir)

      assert(newDf.count() == 8)
    }
  }

  test("SPARK-24836: ignoreExtension must override hadoop's config") {
    withTempDir { dir =>
      Files.copy(
        Paths.get(new URI(episodesAvro)),
        Paths.get(dir.getCanonicalPath, "episodes"))

      val hadoopConf = spark.sessionState.newHadoopConf()
      withSQLConf(AvroFileFormat.IgnoreFilesWithoutExtensionProperty -> "true") {
        val newDf = spark
          .read
          .option("ignoreExtension", "true")
          .format("avro")
          .load(s"${dir.getCanonicalPath}/episodes")
        assert(newDf.count() == 8)
      }
    }
  }

  test("SPARK-24881: write with compression - avro options") {
    def getCodec(dir: String): Option[String] = {
      val files = new File(dir)
        .listFiles()
        .filter(_.isFile)
        .filter(_.getName.endsWith("avro"))
      files.map { file =>
        val reader = new DataFileReader(file, new GenericDatumReader[Any]())
        val r = reader.getMetaString("avro.codec")
        r
      }.map(v => if (v == "null") UNCOMPRESSED.lowerCaseName() else v).headOption
    }
    def checkCodec(df: DataFrame, dir: String, codec: String): Unit = {
      val subdir = s"$dir/$codec"
      df.write.option("compression", codec).format("avro").save(subdir)
      assert(getCodec(subdir) == Some(codec))
    }
    withTempPath { dir =>
      val path = dir.toString
      val df = spark.read.format("avro").load(testAvro)

      AvroCompressionCodec.values().foreach { codec =>
        checkCodec(df, path, codec.lowerCaseName())
      }
    }
  }

  private def checkSchemaWithRecursiveLoop(avroSchema: String, recursiveFieldMaxDepth: Int):
    Unit = {
    val message = intercept[IncompatibleSchemaException] {
      SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema), false, "")
    }.getMessage

    assert(message.contains("Found recursive reference in Avro schema"))
  }

  test("Detect recursive loop") {
    for (recursiveFieldMaxDepth <- Seq(-1, 0)) {
      checkSchemaWithRecursiveLoop(
        """
          |{
          |  "type": "record",
          |  "name": "LongList",
          |  "fields" : [
          |    {"name": "value", "type": "long"},             // each element has a long
          |    {"name": "next", "type": ["null", "LongList"]} // optional next element
          |  ]
          |}
    """.stripMargin, recursiveFieldMaxDepth)

      checkSchemaWithRecursiveLoop(
        """
          |{
          |  "type": "record",
          |  "name": "LongList",
          |  "fields": [
          |    {
          |      "name": "value",
          |      "type": {
          |        "type": "record",
          |        "name": "foo",
          |        "fields": [
          |          {
          |            "name": "parent",
          |            "type": "LongList"
          |          }
          |        ]
          |      }
          |    }
          |  ]
          |}
    """.stripMargin, recursiveFieldMaxDepth)

      checkSchemaWithRecursiveLoop(
        """
          |{
          |  "type": "record",
          |  "name": "LongList",
          |  "fields" : [
          |    {"name": "value", "type": "long"},
          |    {"name": "array", "type": {"type": "array", "items": "LongList"}}
          |  ]
          |}
    """.stripMargin, recursiveFieldMaxDepth)

      checkSchemaWithRecursiveLoop(
        """
          |{
          |  "type": "record",
          |  "name": "LongList",
          |  "fields" : [
          |    {"name": "value", "type": "long"},
          |    {"name": "map", "type": {"type": "map", "values": "LongList"}}
          |  ]
          |}
    """.stripMargin, recursiveFieldMaxDepth)
    }
  }

  private def checkSparkSchemaEquals(
      avroSchema: String, expectedSchema: StructType, recursiveFieldMaxDepth: Int): Unit = {
    val sparkSchema =
      SchemaConverters.toSqlType(
        new Schema.Parser().parse(avroSchema), false, "", recursiveFieldMaxDepth).dataType

    assert(sparkSchema === expectedSchema)
  }

  test("Translate recursive schema - union") {
    val avroSchema = """
      |{
      |  "type": "record",
      |  "name": "LongList",
      |  "fields" : [
      |    {"name": "value", "type": "long"},             // each element has a long
      |    {"name": "next", "type": ["null", "LongList"]} // optional next element
      |  ]
      |}
    """.stripMargin
    val nonRecursiveFields = new StructType().add("value", LongType, nullable = false)
    var expectedSchema = nonRecursiveFields
    for (i <- 1 to 5) {
      checkSparkSchemaEquals(avroSchema, expectedSchema, i)
      expectedSchema = nonRecursiveFields.add("next", expectedSchema)
    }
  }

  test("Translate recursive schema - union - 2 non-null fields") {
    val avroSchema = """
     |{
     |  "type": "record",
     |  "name": "TreeNode",
     |  "fields": [
     |    {
     |      "name": "name",
     |      "type": "string"
     |    },
     |    {
     |      "name": "value",
     |      "type": [
     |        "long"
     |      ]
     |    },
     |    {
     |      "name": "children",
     |      "type": [
     |        "null",
     |        {
     |          "type": "array",
     |          "items": "TreeNode"
     |        }
     |      ],
     |      "default": null
     |    }
     |  ]
     |}
    """.stripMargin
    val nonRecursiveFields = new StructType().add("name", StringType, nullable = false)
      .add("value", LongType, nullable = false)
    var expectedSchema = nonRecursiveFields
    for (i <- 1 to 5) {
      checkSparkSchemaEquals(avroSchema, expectedSchema, i)
      expectedSchema = nonRecursiveFields.add("children",
        new ArrayType(expectedSchema, false), nullable = true)
    }
  }

  test("Translate recursive schema - record") {
    val avroSchema = """
      |{
      |  "type": "record",
      |  "name": "LongList",
      |  "fields": [
      |    {
      |      "name": "value",
      |      "type": {
      |        "type": "record",
      |        "name": "foo",
      |        "fields": [
      |          {
      |            "name": "parent",
      |            "type": "LongList"
      |          }
      |        ]
      |      }
      |    }
      |  ]
      |}
    """.stripMargin
    val nonRecursiveFields = new StructType().add("value", StructType(Seq()), nullable = false)
    var expectedSchema = nonRecursiveFields
    for (i <- 1 to 5) {
      checkSparkSchemaEquals(avroSchema, expectedSchema, i)
      expectedSchema = new StructType().add("value",
          new StructType().add("parent", expectedSchema, nullable = false), nullable = false)
    }
  }

  test("Translate recursive schema - array") {
    val avroSchema = """
      |{
      |  "type": "record",
      |  "name": "LongList",
      |  "fields" : [
      |    {"name": "value", "type": "long"},
      |    {"name": "array", "type": {"type": "array", "items": "LongList"}}
      |  ]
      |}
    """.stripMargin
    val nonRecursiveFields = new StructType().add("value", LongType, nullable = false)
    var expectedSchema = nonRecursiveFields
    for (i <- 1 to 5) {
      checkSparkSchemaEquals(avroSchema, expectedSchema, i)
      expectedSchema =
        nonRecursiveFields.add("array", new ArrayType(expectedSchema, false), nullable = false)
    }
  }

  test("Translate recursive schema - map") {
    val avroSchema = """
      |{
      |  "type": "record",
      |  "name": "LongList",
      |  "fields" : [
      |    {"name": "value", "type": "long"},
      |    {"name": "map", "type": {"type": "map", "values": "LongList"}}
      |  ]
      |}
    """.stripMargin
    val nonRecursiveFields = new StructType().add("value", LongType, nullable = false)
    var expectedSchema = nonRecursiveFields
    for (i <- 1 to 5) {
      checkSparkSchemaEquals(avroSchema, expectedSchema, i)
      expectedSchema =
        nonRecursiveFields.add("map",
          new MapType(StringType, expectedSchema, false), nullable = false)
    }
  }

  test("recursive schema integration test") {
    val catalystSchema =
      StructType(Seq(
        StructField("Id", IntegerType),
        StructField("Name", StructType(Seq(
          StructField("Id", IntegerType),
          StructField("Name", StructType(Seq(
            StructField("Id", IntegerType),
            StructField("Name", NullType)))))))))

    val avroSchema = s"""
      |{
      |  "type" : "record",
      |  "name" : "test_schema",
      |  "fields" : [
      |    {"name": "Id", "type": "int"},
      |    {"name": "Name", "type": ["null", "test_schema"]}
      |  ]
      |}
    """.stripMargin

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(2, Row(3, Row(4, null))), Row(1, null))),
      catalystSchema)

    withTempPath { tempDir =>
      df.write.format("avro").save(tempDir.getPath)

      val exc = intercept[AnalysisException] {
        spark.read
          .format("avro")
          .option("avroSchema", avroSchema)
          .option("recursiveFieldMaxDepth", 16)
          .load(tempDir.getPath)
      }
      assert(exc.getMessage.contains("Should not be greater than 15."))

      checkAnswer(
        spark.read
          .format("avro")
          .option("avroSchema", avroSchema)
          .option("recursiveFieldMaxDepth", 10)
          .load(tempDir.getPath),
        df)

      checkAnswer(
        spark.read
          .format("avro")
          .option("avroSchema", avroSchema)
          .option("recursiveFieldMaxDepth", 1)
          .load(tempDir.getPath),
        df.select("Id"))
    }
  }

  test("log a warning of ignoreExtension deprecation") {
    val logAppender = new LogAppender("deprecated Avro option 'ignoreExtension'")
    withTempPath { dir =>
      Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1), ("d", 2, 1))
        .toDF("value", "p1", "p2")
        .repartition(2)
        .write
        .format("avro")
        .save(dir.getCanonicalPath)
      withLogAppender(logAppender) {
        spark
          .read
          .format("avro")
          .option(AvroOptions.IGNORE_EXTENSION, false)
          .load(dir.getCanonicalPath)
          .count()
      }
      val deprecatedEvents = logAppender.loggingEvents
        .filter(_.getMessage.getFormattedMessage.contains(
          s"Option ${AvroOptions.IGNORE_EXTENSION} is deprecated"))
      assert(deprecatedEvents.size === 1)
    }
  }

  // It generates input files for the test below:
  // "SPARK-31183, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps"
  ignore("SPARK-31855: generate test files for checking compatibility with Spark 2.4/3.2") {
    val resourceDir = "connector/avro/src/test/resources"
    val version = SPARK_VERSION_SHORT.replaceAll("\\.", "_")
    def save(
      in: Seq[String],
      t: String,
      dstFile: String,
      options: Map[String, String] = Map.empty): Unit = {
      withTempDir { dir =>
        in.toDF("dt")
          .select($"dt".cast(t))
          .repartition(1)
          .write
          .mode("overwrite")
          .options(options)
          .format("avro")
          .save(dir.getCanonicalPath)
        Files.copy(
          dir.listFiles().filter(_.getName.endsWith(".avro")).head.toPath,
          Paths.get(resourceDir, dstFile),
          StandardCopyOption.REPLACE_EXISTING)
      }
    }
    withDefaultTimeZone(LA) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
        save(
          Seq("1001-01-01"),
          "date",
          s"before_1582_date_v$version.avro")
        save(
          Seq("1001-01-01 01:02:03.123"),
          "timestamp",
          s"before_1582_timestamp_millis_v$version.avro",
          // scalastyle:off line.size.limit
          Map("avroSchema" ->
            s"""
               |  {
               |    "namespace": "logical",
               |    "type": "record",
               |    "name": "test",
               |    "fields": [
               |      {"name": "dt", "type": ["null", {"type": "long","logicalType": "timestamp-millis"}], "default": null}
               |    ]
               |  }
               |""".stripMargin))
        // scalastyle:on line.size.limit
        save(
          Seq("1001-01-01 01:02:03.123456"),
          "timestamp",
          s"before_1582_timestamp_micros_v$version.avro")
      }
    }
  }

  private def runInMode(
      modes: Seq[LegacyBehaviorPolicy.Value])(f: Map[String, String] => Unit): Unit = {
    modes.foreach { mode =>
      withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_READ.key -> mode.toString) {
        f(Map.empty)
      }
    }
    withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
      modes.foreach { mode =>
        f(Map(AvroOptions.DATETIME_REBASE_MODE -> mode.toString))
      }
    }
  }

  test("SPARK-31183, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps") {
    // test reading the existing 2.4/3.2 files and new 3.x files (with rebase on/off) together.
    def checkReadMixedFiles(
        fileName: String,
        dt: String,
        dataStr: String,
        checkDefaultLegacyRead: String => Unit): Unit = {
      withTempPaths(2) { paths =>
        paths.foreach(_.delete())
        val oldPath = getResourceAvroFilePath(fileName)
        val path3_x = paths(0).getCanonicalPath
        val path3_x_rebase = paths(1).getCanonicalPath
        if (dt == "date") {
          val df = Seq(dataStr).toDF("str").select($"str".cast("date").as("dt"))

          checkDefaultLegacyRead(oldPath)

          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> CORRECTED.toString) {
            df.write.format("avro").mode("overwrite").save(path3_x)
          }
          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
            df.write.format("avro").save(path3_x_rebase)
          }

          // For Avro files written by Spark 3.0, we know the writer info and don't need the config
          // to guide the rebase behavior.
          runInMode(Seq(LEGACY)) { options =>
            checkAnswer(
              spark.read.options(options).format("avro").load(oldPath, path3_x, path3_x_rebase),
              1.to(3).map(_ => Row(java.sql.Date.valueOf(dataStr))))
          }
        } else {
          val df = Seq(dataStr).toDF("str").select($"str".cast("timestamp").as("dt"))
          val avroSchema =
            s"""
              |{
              |  "type" : "record",
              |  "name" : "test_schema",
              |  "fields" : [
              |    {"name": "dt", "type": {"type": "long", "logicalType": "$dt"}}
              |  ]
              |}""".stripMargin

          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
            val e = intercept[SparkException] {
              df.write.format("avro").option("avroSchema", avroSchema).save(path3_x)
            }
            assert(e.getCondition == "TASK_WRITE_FAILED")
            assert(e.getCause.isInstanceOf[SparkUpgradeException])
          }
          checkDefaultLegacyRead(oldPath)

          // By default we should not fail to write ancient datetime values.
          df.write.format("avro").option("avroSchema", avroSchema).mode("overwrite").save(path3_x)

          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> CORRECTED.toString) {
            df.write.format("avro").option("avroSchema", avroSchema).mode("overwrite").save(path3_x)
          }
          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
            df.write.format("avro").option("avroSchema", avroSchema).save(path3_x_rebase)
          }

          // For Avro files written by Spark 3.0, we know the writer info and don't need the config
          // to guide the rebase behavior.
          runInMode(Seq(LEGACY)) { options =>
            checkAnswer(
              spark.read.options(options).format("avro").load(oldPath, path3_x, path3_x_rebase),
              1.to(3).map(_ => Row(java.sql.Timestamp.valueOf(dataStr))))
          }
        }
      }
    }

    def successInRead(path: String): Unit = spark.read.format("avro").load(path).collect()
    Seq(
      // By default we should not fail to read ancient datetime values when parquet files don't
      // contain Spark version.
      "2_4_5" -> successInRead _,
      "2_4_6" -> successInRead _,
      "3_2_0" -> successInRead _
    ).foreach { case (version, checkDefaultRead) =>
      checkReadMixedFiles(
        s"before_1582_date_v$version.avro",
        "date",
        "1001-01-01",
        checkDefaultRead)
      checkReadMixedFiles(
        s"before_1582_timestamp_micros_v$version.avro",
        "timestamp-micros",
        "1001-01-01 01:02:03.123456",
        checkDefaultRead)
      checkReadMixedFiles(
        s"before_1582_timestamp_millis_v$version.avro",
        "timestamp-millis",
        "1001-01-01 01:02:03.123",
        checkDefaultRead)
    }
  }

  test("SPARK-31183, SPARK-37705: rebasing microseconds timestamps in write") {
    // Ignore the default JVM time zone and use the session time zone instead of it in rebasing.
    DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.JST) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> DateTimeTestUtils.LA.getId) {
        val tsStr = "1001-01-01 01:02:03.123456"
        val nonRebased = "1001-01-07 01:09:05.123456"
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
            Seq(tsStr).toDF("tsS")
              .select($"tsS".cast("timestamp").as("ts"))
              .write.format("avro")
              .save(path)
          }

          // The file metadata indicates if it needs rebase or not, so we can always get the correct
          // result regardless of the "rebase mode" config.
          runInMode(Seq(LEGACY, CORRECTED, EXCEPTION)) { options =>
            checkAnswer(
              spark.read.options(options).format("avro").load(path).select($"ts".cast("string")),
              Row(tsStr))
          }

          // Force to not rebase to prove the written datetime values are rebased and we will get
          // wrong result if we don't rebase while reading.
          withSQLConf("spark.test.forceNoRebase" -> "true") {
            checkAnswer(
              spark.read.format("avro").load(path).select($"ts".cast("string")),
              Row(nonRebased))
          }
        }
      }
    }
  }

  test("SPARK-31183, SPARK-37705: rebasing milliseconds timestamps in write") {
    // Ignore the default JVM time zone and use the session time zone instead of it in rebasing.
    DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.JST) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> DateTimeTestUtils.LA.getId) {
        val tsStr = "1001-01-01 01:02:03.123456"
        val rebased = "1001-01-01 01:02:03.123"
        val nonRebased = "1001-01-07 01:09:05.123"
        Seq(
          """{"type": "long","logicalType": "timestamp-millis"}""",
          """"long"""").foreach { tsType =>
          val timestampSchema = s"""
            |{
            |  "namespace": "logical",
            |  "type": "record",
            |  "name": "test",
            |  "fields": [
            |    {"name": "ts", "type": $tsType}
            |  ]
            |}""".stripMargin
          withTempPath { dir =>
            val path = dir.getAbsolutePath
            withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
              Seq(tsStr).toDF("tsS")
                .select($"tsS".cast("timestamp").as("ts"))
                .write
                .option("avroSchema", timestampSchema)
                .format("avro")
                .save(path)
            }

            // The file metadata indicates if it needs rebase or not, so we can always get
            // the correct result regardless of the "rebase mode" config.
            runInMode(Seq(LEGACY, CORRECTED, EXCEPTION)) { options =>
              checkAnswer(
                spark.read
                  .options(options)
                  .schema("ts timestamp")
                  .format("avro").load(path)
                  .select($"ts".cast("string")),
                Row(rebased))
            }

            // Force to not rebase to prove the written datetime values are rebased and we will get
            // wrong result if we don't rebase while reading.
            withSQLConf("spark.test.forceNoRebase" -> "true") {
              checkAnswer(
                spark.read
                  .schema("ts timestamp")
                  .format("avro").load(path)
                  .select($"ts".cast("string")),
                Row(nonRebased))
            }
          }
        }
      }
    }
  }

  test("SPARK-31183: rebasing dates in write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
        Seq("1001-01-01").toDF("dateS")
          .select($"dateS".cast("date").as("date"))
          .write.format("avro")
          .save(path)
      }

      // The file metadata indicates if it needs rebase or not, so we can always get the correct
      // result regardless of the "rebase mode" config.
      runInMode(Seq(LEGACY, CORRECTED, EXCEPTION)) { options =>
        checkAnswer(
          spark.read.options(options).format("avro").load(path),
          Row(Date.valueOf("1001-01-01")))
      }

      // Force to not rebase to prove the written datetime values are rebased and we will get
      // wrong result if we don't rebase while reading.
      withSQLConf("spark.test.forceNoRebase" -> "true") {
        checkAnswer(spark.read.format("avro").load(path), Row(Date.valueOf("1001-01-07")))
      }
    }
  }

  private def checkMetaData(path: java.io.File, key: String, expectedValue: String): Unit = {
    val value = openDatumReader(path).asInstanceOf[DataFileReader[_]].getMetaString(key)
    assert(value === expectedValue)
  }

  private def openDatumReader(path: File): org.apache.avro.file.FileReader[GenericRecord] = {
    val avroFiles = path.listFiles()
      .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
    assert(avroFiles.length === 1)
    DataFileReader.openReader(avroFiles(0), new GenericDatumReader[GenericRecord]())
  }

  test("SPARK-31327: Write Spark version into Avro file metadata") {
    withTempPath { path =>
      spark.range(1).repartition(1).write.format("avro").save(path.getCanonicalPath)
      checkMetaData(path, SPARK_VERSION_METADATA_KEY, SPARK_VERSION_SHORT)
    }
  }

  test("SPARK-33163, SPARK-37705: write the metadata key 'org.apache.spark.legacyDateTime' " +
    "and 'org.apache.spark.timeZone'") {
    def saveTs(dir: java.io.File): Unit = {
      Seq(Timestamp.valueOf("2020-10-15 01:02:03")).toDF()
        .repartition(1)
        .write
        .format("avro")
        .save(dir.getAbsolutePath)
    }
    withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
      withTempPath { dir =>
        saveTs(dir)
        checkMetaData(dir, SPARK_LEGACY_DATETIME_METADATA_KEY, "")
        checkMetaData(dir, SPARK_TIMEZONE_METADATA_KEY, SQLConf.get.sessionLocalTimeZone)
      }
    }
    Seq(CORRECTED, EXCEPTION).foreach { mode =>
      withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> mode.toString) {
        withTempPath { dir =>
          saveTs(dir)
          checkMetaData(dir, SPARK_LEGACY_DATETIME_METADATA_KEY, null)
          checkMetaData(dir, SPARK_TIMEZONE_METADATA_KEY, null)
        }
      }
    }
  }

  test("SPARK-33314: RowReader doesn't over-consume when hasNextRow called twice") {
    withTempPath { dir =>
      Seq((1), (2), (3))
        .toDF("index")
        .write
        .format("avro")
        .save(dir.getCanonicalPath)
      val df = spark
        .read
        .format("avro")
        .load(dir.getCanonicalPath)
        .orderBy("index")

      checkAnswer(df,
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("SPARK-35427: datetime rebasing in the EXCEPTION mode") {
    withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
      Seq("timestamp-millis", "timestamp-micros").foreach { dt =>
        withTempPath { dir =>
          val df = Seq("1001-01-01 01:02:03.123456")
            .toDF("str")
            .select($"str".cast("timestamp").as("dt"))
          val avroSchema =
            s"""
              |{
              |  "type" : "record",
              |  "name" : "test_schema",
              |  "fields" : [
              |    {"name": "dt", "type": {"type": "long", "logicalType": "$dt"}}
              |  ]
              |}""".stripMargin

          val e = intercept[SparkException] {
            df.write.format("avro").option("avroSchema", avroSchema).save(dir.getCanonicalPath)
          }
          assert(e.getCondition == "TASK_WRITE_FAILED")
          val errMsg = e.getCause.asInstanceOf[SparkUpgradeException].getMessage
          assert(errMsg.contains("You may get a different result due to the upgrading"))
        }
      }

      withTempPath { dir =>
        val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
        val e = intercept[SparkException] {
          df.write.format("avro").save(dir.getCanonicalPath)
        }
        assert(e.getCondition == "TASK_WRITE_FAILED")
        val errMsg = e.getCause.asInstanceOf[SparkUpgradeException].getMessage
        assert(errMsg.contains("You may get a different result due to the upgrading"))
      }
    }

    withSQLConf(SQLConf.AVRO_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
      Seq(
        "before_1582_date_v2_4_5.avro",
        "before_1582_timestamp_micros_v2_4_5.avro",
        "before_1582_timestamp_millis_v2_4_5.avro"
      ).foreach { fileName =>
        val e = intercept[SparkUpgradeException] {
          spark.read.format("avro").load(getResourceAvroFilePath(fileName)).collect()
        }
        assert(e.getMessage.contains("You may get a different result due to the upgrading"))
      }
    }
  }

  test("SPARK-33865: CREATE TABLE DDL with avro should check col name") {
    withTable("test_ddl") {
      withView("v") {
        spark.range(1).createTempView("v")
        withTempDir { dir =>
          checkError(
            exception = intercept[AnalysisException] {
              sql(
                s"""
                   |CREATE TABLE test_ddl USING AVRO
                   |LOCATION '${dir}'
                   |AS SELECT ID, IF(ID=1,1,0) FROM v""".stripMargin)
            },
            condition = "INVALID_COLUMN_NAME_AS_PATH",
            parameters = Map(
              "datasource" -> "AvroFileFormat", "columnName" -> "`(IF((ID = 1), 1, 0))`")
          )
        }

        withTempDir { dir =>
          spark.sql(
            s"""
               |CREATE TABLE test_ddl USING AVRO
               |LOCATION '${dir}'
               |AS SELECT ID, IF(ID=1,ID,0) AS A, ABS(ID) AS B
               |FROM v""".stripMargin)
          val expectedSchema = StructType(Seq(StructField("ID", LongType, true),
            StructField("A", LongType, true), StructField("B", LongType, true)))
          assert(spark.table("test_ddl").schema == expectedSchema)
        }
      }
    }
  }

  test("SPARK-37225: Support reading and writing ANSI intervals") {
    Seq(
      YearMonthIntervalType() -> ((i: Int) => java.time.Period.of(i, i, 0)),
      DayTimeIntervalType() -> ((i: Int) => java.time.Duration.ofDays(i).plusSeconds(i))
    ).foreach { case (it, f) =>
      val data = (1 to 10).map(i => Row(i, f(i)))
      val schema = StructType(Array(StructField("d", IntegerType, false),
        StructField("i", it, false)))
      withTempPath { file =>
        val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
        df.write.format("avro").save(file.getCanonicalPath)
        val df2 = spark.read.format("avro").load(file.getCanonicalPath)
        checkAnswer(df2, df.collect().toSeq)
      }
    }

    // Tests for ANSI intervals in complex types.
    withTempPath { file =>
      val df = spark.sql(
        """SELECT
          |  named_struct('interval', interval '1-2' year to month) a,
          |  array(interval '1 2:3' day to minute) b,
          |  map('key', interval '10' year) c""".stripMargin)
      df.write.format("avro").save(file.getCanonicalPath)
      val df2 = spark.read.format("avro").load(file.getCanonicalPath)
      checkAnswer(df2, df.collect().toSeq)
    }
  }

  test("SPARK-40667: validate Avro Options") {
    assert(AvroOptions.getAllOptions.size == 12)
    // Please add validation on any new Avro options here
    assert(AvroOptions.isValidOption("ignoreExtension"))
    assert(AvroOptions.isValidOption("mode"))
    assert(AvroOptions.isValidOption("recordName"))
    assert(AvroOptions.isValidOption("compression"))
    assert(AvroOptions.isValidOption("avroSchema"))
    assert(AvroOptions.isValidOption("avroSchemaUrl"))
    assert(AvroOptions.isValidOption("recordNamespace"))
    assert(AvroOptions.isValidOption("positionalFieldMatching"))
    assert(AvroOptions.isValidOption("datetimeRebaseMode"))
    assert(AvroOptions.isValidOption("enableStableIdentifiersForUnionType"))
    assert(AvroOptions.isValidOption("stableIdentifierPrefixForUnionType"))
    assert(AvroOptions.isValidOption("recursiveFieldMaxDepth"))
  }

  test("SPARK-46633: read file with empty blocks") {
    for (maxPartitionBytes <- Seq(100, 100000, 100000000)) {
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> s"$maxPartitionBytes") {
        val file = getResourceAvroFilePath("empty_blocks.avro")
        val df = spark.read.format("avro").load(file)
        val count = df.count()
        val records = df.collect()
        assert(count == 58)
        assert(count == records.length)
      }
    }
  }

  test("SPARK-46990: read an empty file where pastSync returns false at EOF") {
    for (maxPartitionBytes <- Seq(100, 100000, 100000000)) {
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> s"$maxPartitionBytes") {
        val file = getResourceAvroFilePath("empty_file.avro")
        val df = spark.read.format("avro").load(file)
        assert(df.count() == 0)
      }
    }
  }
}

class AvroV1Suite extends AvroSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "avro")

  test("SPARK-36271: V1 insert should check schema field name too") {
    withView("v") {
      spark.range(1).createTempView("v")
      withTempDir { dir =>
        checkError(
          exception = intercept[AnalysisException] {
            sql("SELECT ID, IF(ID=1,1,0) FROM v").write.mode(SaveMode.Overwrite)
              .format("avro").save(dir.getCanonicalPath)
          },
          condition = "INVALID_COLUMN_NAME_AS_PATH",
          parameters = Map(
            "datasource" -> "AvroFileFormat", "columnName" -> "`(IF((ID = 1), 1, 0))`")
        )
      }

      withTempDir { dir =>
        checkError(
          exception = intercept[AnalysisException] {
            sql("SELECT NAMED_STRUCT('(IF((ID = 1), 1, 0))', IF(ID=1,ID,0)) AS col1 FROM v")
              .write.mode(SaveMode.Overwrite)
              .format("avro").save(dir.getCanonicalPath)
          },
          condition = "INVALID_COLUMN_NAME_AS_PATH",
          parameters = Map(
            "datasource" -> "AvroFileFormat", "columnName" -> "`(IF((ID = 1), 1, 0))`")
        )
      }
    }
  }
}

class AvroV2Suite extends AvroSuite with ExplainSuiteHelper {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("Avro source v2: support partition pruning") {
    withTempPath { dir =>
      Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
        .toDF("value", "p1", "p2")
        .write
        .format("avro")
        .partitionBy("p1", "p2")
        .save(dir.getCanonicalPath)
      val df = spark
        .read
        .format("avro")
        .load(dir.getCanonicalPath)
        .where("p1 = 1 and p2 = 2 and value != \"a\"")

       val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
         case f: Filter => f.condition
       }
      assert(filterCondition.isDefined)
      // The partitions filters should be pushed down and no need to be reevaluated.
      assert(!filterCondition.get.exists {
        case a: AttributeReference => a.name == "p1" || a.name == "p2"
        case _ => false
      })

      val fileScan = df.queryExecution.executedPlan collectFirst {
        case BatchScanExec(_, f: AvroScan, _, _, _, _) => f
      }
      assert(fileScan.nonEmpty)
      assert(fileScan.get.partitionFilters.nonEmpty)
      assert(fileScan.get.dataFilters.nonEmpty)
      assert(fileScan.get.planInputPartitions().forall { partition =>
        partition.asInstanceOf[FilePartition].files.forall { file =>
          file.urlEncodedPath.contains("p1=1") &&
            file.urlEncodedPath.contains("p2=2")
        }
      })
      checkAnswer(df, Row("b", 1, 2))
    }
  }

  test("Avro source v2: support passing data filters to FileScan without partitionFilters") {
    withTempPath { dir =>
      Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
        .toDF("value", "p1", "p2")
        .write
        .format("avro")
        .save(dir.getCanonicalPath)
      val df = spark
        .read
        .format("avro")
        .load(dir.getCanonicalPath)
        .where("value = 'a'")

      val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
        case f: Filter => f.condition
      }
      assert(filterCondition.isDefined)

      val fileScan = df.queryExecution.executedPlan collectFirst {
        case BatchScanExec(_, f: AvroScan, _, _, _, _) => f
      }
      assert(fileScan.nonEmpty)
      assert(fileScan.get.partitionFilters.isEmpty)
      assert(fileScan.get.dataFilters.nonEmpty)
      checkAnswer(df, Row("a", 1, 2))
    }
  }

  private def getBatchScanExec(plan: SparkPlan): BatchScanExec = {
    plan.find(_.isInstanceOf[BatchScanExec]).get.asInstanceOf[BatchScanExec]
  }

  test("Avro source v2: same result with different orders of data filters and partition filters") {
    withTempPath { path =>
      val tmpDir = path.getCanonicalPath
      spark
        .range(10)
        .selectExpr("id as a", "id + 1 as b", "id + 2 as c", "id + 3 as d")
        .write
        .partitionBy("a", "b")
        .format("avro")
        .save(tmpDir)
      val df = spark.read.format("avro").load(tmpDir)
      // partition filters: a > 1 AND b < 9
      // data filters: c > 1 AND d < 9
      val plan1 = df.where("a > 1 AND b < 9 AND c > 1 AND d < 9").queryExecution.sparkPlan
      val plan2 = df.where("b < 9 AND a > 1 AND d < 9 AND c > 1").queryExecution.sparkPlan
      assert(plan1.sameResult(plan2))
      val scan1 = getBatchScanExec(plan1)
      val scan2 = getBatchScanExec(plan2)
      assert(scan1.sameResult(scan2))
    }
  }

  test("explain formatted on an avro data source v2") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath + "/avro"
      val expected_plan_fragment =
        s"""
           |\\(1\\) BatchScan avro file:$basePath
           |Output \\[2\\]: \\[value#xL, id#x\\]
           |DataFilters: \\[isnotnull\\(value#xL\\), \\(value#xL > 2\\)\\]
           |Format: avro
           |Location: InMemoryFileIndex\\([0-9]+ paths\\)\\[.*\\]
           |PartitionFilters: \\[isnotnull\\(id#x\\), \\(id#x > 1\\)\\]
           |PushedFilters: \\[IsNotNull\\(value\\), GreaterThan\\(value,2\\)\\]
           |ReadSchema: struct\\<value:bigint\\>
           |""".stripMargin.trim
      spark.range(10)
        .select(col("id"), col("id").as("value"))
        .write.option("header", true)
        .partitionBy("id")
        .format("avro")
        .save(basePath)
      val df = spark
        .read
        .format("avro")
        .load(basePath).where($"id" > 1 && $"value" > 2)
      val normalizedOutput = getNormalizedExplain(df, FormattedMode)
      assert(expected_plan_fragment.r.findAllMatchIn(normalizedOutput).length == 1,
        normalizedOutput)
    }
  }

  test("SPARK-32346: filters pushdown to Avro datasource v2") {
    Seq(true, false).foreach { filtersPushdown =>
      withSQLConf(SQLConf.AVRO_FILTER_PUSHDOWN_ENABLED.key -> filtersPushdown.toString) {
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format("avro")
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format("avro")
            .load(dir.getCanonicalPath)
            .where("value = 'a'")

          val fileScan = df.queryExecution.executedPlan collectFirst {
            case BatchScanExec(_, f: AvroScan, _, _, _, _) => f
          }
          assert(fileScan.nonEmpty)
          if (filtersPushdown) {
            assert(fileScan.get.pushedFilters.nonEmpty)
          } else {
            assert(fileScan.get.pushedFilters.isEmpty)
          }
          checkAnswer(df, Row("a", 1, 2))
        }
      }
    }
  }
}
