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

import java.io.ByteArrayOutputStream
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.avro.{functions => Fns}
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CreateMap, ElementAt, Expression, ExprId, FromAvro, LeafExpression, Literal, RuntimeReplaceable, SchemaOfAvro => SchemaOfAvroExpression, ToAvro}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{col, lit, map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  BinaryType, CharType, DataType, IntegerType, MapType, StringType, StructField, StructType,
  VarcharType
}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

/** A test string literal that runs onEval before returning, to pause or fail evaluation. */
private case class StringLiteralWithEvalHook(
    value: String,
    onEval: () => Unit)
  extends LeafExpression with CodegenFallback {

  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    onEval()
    UTF8String.fromString(value)
  }
}

class AvroFunctionsSuite extends SharedSparkSession {
  import testImplicits._

  test("roundtrip in to_avro and from_avro - int and string") {
    val df = spark.range(10).select($"id", $"id".cast("string").as("str"))

    val avroDF = df.select(
      functions.to_avro($"id").as("a"),
      functions.to_avro($"str").as("b"))
    val avroTypeLong = s"""
      |{
      |  "type": "int",
      |  "name": "id"
      |}
    """.stripMargin
    val avroTypeStr = s"""
      |{
      |  "type": "string",
      |  "name": "str"
      |}
    """.stripMargin
    checkAnswer(avroDF.select(
      functions.from_avro($"a", avroTypeLong),
      functions.from_avro($"b", avroTypeStr)), df)
  }

  test("roundtrip in to_avro and from_avro - struct") {
    val df = spark.range(10).select(struct($"id", $"id".cast("string").as("str")).as("struct"))
    val avroStructDF = df.select(functions.to_avro($"struct").as("avro"))
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "col1", "type": "long"},
      |    {"name": "col2", "type": "string"}
      |  ]
      |}
    """.stripMargin
    checkAnswer(avroStructDF.select(
      functions.from_avro($"avro", avroTypeStruct)), df)
  }

  test("handle invalid input in from_avro") {
    val count = 10
    val df = spark.range(count).select(struct($"id", $"id".as("id2")).as("struct"))
    val avroStructDF = df.select(functions.to_avro($"struct").as("avro"))
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "col1", "type": "long"},
      |    {"name": "col2", "type": "double"}
      |  ]
      |}
    """.stripMargin

    intercept[SparkException] {
      avroStructDF.select(
        functions.from_avro(
          $"avro", avroTypeStruct, Map("mode" -> "FAILFAST").asJava)).collect()
    }

    // For PERMISSIVE mode, the result should be row of null columns.
    val expected = (0 until count).map(_ => Row(Row(null, null)))
    checkAnswer(
      avroStructDF.select(
       functions.from_avro(
          $"avro", avroTypeStruct, Map("mode" -> "PERMISSIVE").asJava)),
      expected)

    checkError(
      exception = intercept[AnalysisException] {
        avroStructDF.select(
          functions.from_avro(
            $"avro", avroTypeStruct, Map("mode" -> "DROPMALFORMED").asJava)).collect()
      },
      condition = "PARSE_MODE_UNSUPPORTED",
      parameters = Map(
        "funcName" -> "`from_avro`",
        "mode" -> "DROPMALFORMED"))
  }

  test("roundtrip in to_avro and from_avro - array with null") {
    val dfOne = Seq(Tuple1(Tuple1(1) :: Nil), Tuple1(null :: Nil)).toDF("array")
    val avroTypeArrStruct = s"""
      |[ {
      |  "type" : "array",
      |  "items" : [ {
      |    "type" : "record",
      |    "name" : "x",
      |    "fields" : [ {
      |      "name" : "y",
      |      "type" : "int"
      |    } ]
      |  }, "null" ]
      |}, "null" ]
    """.stripMargin
    val readBackOne = dfOne.select(functions.to_avro($"array").as("avro"))
      .select(functions.from_avro($"avro", avroTypeArrStruct).as("array"))
    checkAnswer(dfOne, readBackOne)
  }

  test("SPARK-27798: from_avro produces same value when converted to local relation") {
    val simpleSchema =
      """
        |{
        |  "type": "record",
        |  "name" : "Payload",
        |  "fields" : [ {"name" : "message", "type" : "string" } ]
        |}
      """.stripMargin

    def generateBinary(message: String, avroSchema: String): Array[Byte] = {
      val schema = new Schema.Parser().parse(avroSchema)
      val out = new ByteArrayOutputStream()
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val rootRecord = new GenericRecordBuilder(schema).set("message", message).build()
      writer.write(rootRecord, encoder)
      encoder.flush()
      out.toByteArray
    }

    // This bug is hit when the rule `ConvertToLocalRelation` is run. But the rule was excluded
    // in `SharedSparkSession`.
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val df = Seq("one", "two", "three", "four").map(generateBinary(_, simpleSchema))
        .toDF()
        .withColumn("value",
          functions.from_avro(col("value"), simpleSchema))

      assert(df.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
      assert(df.collect().map(_.get(0)) === Seq(Row("one"), Row("two"), Row("three"), Row("four")))
    }
  }

  test("SPARK-27506: roundtrip in to_avro and from_avro with different compatible schemas") {
    val df = spark.range(10).select(
      struct($"id".as("col1"), $"id".cast("string").as("col2")).as("struct")
    )
    val avroStructDF = df.select(functions.to_avro($"struct").as("avro"))
    val actualAvroSchema =
      s"""
         |{
         |  "type": "record",
         |  "name": "struct",
         |  "fields": [
         |    {"name": "col1", "type": "int"},
         |    {"name": "col2", "type": "string"}
         |  ]
         |}
         |""".stripMargin

    val evolvedAvroSchema =
      s"""
         |{
         |  "type": "record",
         |  "name": "struct",
         |  "fields": [
         |    {"name": "col1", "type": "int"},
         |    {"name": "col2", "type": "string"},
         |    {"name": "col3", "type": "string", "default": ""}
         |  ]
         |}
         |""".stripMargin

    val expected = spark.range(10).select(
      struct($"id".as("col1"), $"id".cast("string").as("col2"), lit("").as("col3")).as("struct")
    )

    checkAnswer(
      avroStructDF.select(
        functions.from_avro(
          $"avro",
          actualAvroSchema,
          Map("avroSchema" -> evolvedAvroSchema).asJava)),
      expected)
  }

  test("roundtrip in to_avro and from_avro - struct with nullable Avro schema") {
    val df = spark.range(10).select(struct($"id", $"id".cast("string").as("str")).as("struct"))
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "id", "type": "long"},
      |    {"name": "str", "type": ["null", "string"]}
      |  ]
      |}
    """.stripMargin
    val avroStructDF = df.select(functions.to_avro($"struct", avroTypeStruct).as("avro"))
    checkAnswer(avroStructDF.select(
      functions.from_avro($"avro", avroTypeStruct)), df)
  }

  test("to_avro optional union Avro schema") {
    val df = spark.range(10).select(struct($"id", $"id".cast("string").as("str")).as("struct"))
    for (supportedAvroType <- Seq("""["null", "int", "long"]""", """["int", "long"]""")) {
      val avroTypeStruct = s"""
        |{
        |  "type": "record",
        |  "name": "struct",
        |  "fields": [
        |    {"name": "id", "type": $supportedAvroType},
        |    {"name": "str", "type": ["null", "string"]}
        |  ]
        |}
      """.stripMargin
      val avroStructDF = df.select(functions.to_avro($"struct", avroTypeStruct).as("avro"))
      checkAnswer(avroStructDF.select(
        functions.from_avro($"avro", avroTypeStruct)), df)
    }
  }

  test("to_avro complex union Avro schema") {
    val df = Seq((Some(1), None), (None, Some("a"))).toDF()
      .select(struct(struct($"_1".as("member0"), $"_2".as("member1")).as("u")).as("struct"))
    val avroTypeStruct = SchemaBuilder.record("struct").fields()
      .name("u").`type`().unionOf().intType().and().stringType().endUnion().noDefault()
      .endRecord().toString
    val avroStructDF = df.select(functions.to_avro($"struct", avroTypeStruct).as("avro"))
    checkAnswer(avroStructDF.select(
      functions.from_avro($"avro", avroTypeStruct)), df)
  }

  test("SPARK-39775: Disable validate default values when parsing Avro schemas") {
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "id", "type": "long", "default": null}
      |  ]
      |}
    """.stripMargin
    val options = Map("avroSchema" -> avroTypeStruct)
    val avroOptions = AvroOptions(options)
    val avroSchema = avroOptions.schema.get
    val sparkSchema = SchemaConverters
      .toSqlType(avroSchema, avroOptions.useStableIdForUnionType,
        avroOptions.stableIdPrefixForUnionType)
      .dataType
      .asInstanceOf[StructType]

    val df = spark.range(5).select($"id")
    val structDf = df.select(struct($"id").as("struct"))
    val avroStructDF = structDf.select(functions.to_avro($"struct", avroTypeStruct).as("avro"))
    checkAnswer(avroStructDF.select(functions.from_avro($"avro", avroTypeStruct)), structDf)

    withTempPath { dir =>
      df.write.format("avro").save(dir.getCanonicalPath)
      checkAnswer(spark.read.schema(sparkSchema).format("avro").load(dir.getCanonicalPath), df)

      val msg = intercept[SparkException] {
        spark.read.option("avroSchema", avroTypeStruct).format("avro")
          .load(dir.getCanonicalPath)
          .collect()
      }.getCause.getMessage
      assert(msg.contains("Invalid default for field id: null not a \"long\""))
    }
  }

  test("SPARK-48545: from_avro and to_avro SQL functions") {
    withTable("t") {
      sql(
        """
          |create table t as
          |  select named_struct('u', named_struct('member0', member0, 'member1', member1)) as s
          |  from values (1, null), (null,  'a') tab(member0, member1)
          |""".stripMargin)
      val jsonFormatSchema =
        """
          |{
          |  "type": "record",
          |  "name": "struct",
          |  "fields": [{
          |    "name": "u",
          |    "type": ["int","string"]
          |  }]
          |}
          |""".stripMargin
      val toAvroSql =
        s"""
           |select to_avro(s, '$jsonFormatSchema') as result from t
           |""".stripMargin
      val avroResult = spark.sql(toAvroSql).collect()
      assert(avroResult != null)
      checkAnswer(
        spark.sql(s"select from_avro(result, '$jsonFormatSchema', map()).u from ($toAvroSql)"),
        Seq(Row(Row(1, null)),
          Row(Row(null, "a"))))
      // The 'jsonFormatSchema' argument of the 'to_avro' function is optional.
      checkAnswer(
        spark.sql(s"select length(to_avro(s)) > 0 from t"),
        Seq(Row(true), Row(true)))

      // Negative tests.
      checkError(
        exception = intercept[AnalysisException](sql(
          s"""
             |select to_avro(s, 42) as result from t
             |""".stripMargin)),
        condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map("sqlExpr" -> "\"to_avro(s, 42)\"",
          "msg" -> ("The second argument of the TO_AVRO SQL function must be a constant string " +
            "containing the JSON representation of the schema to use for converting the value to " +
            "AVRO format"),
          "hint" -> ""),
        queryContext = Array(ExpectedContext(
          fragment = "to_avro(s, 42)",
          start = 8,
          stop = 21)))
      checkError(
        exception = intercept[AnalysisException](sql(
          s"""
             |select from_avro(s, 42, '') as result from t
             |""".stripMargin)),
        condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map("sqlExpr" -> "\"from_avro(s, 42, )\"",
          "msg" -> ("The second argument of the FROM_AVRO SQL function must be a constant string " +
            "containing the JSON representation of the schema to use for converting the value " +
            "from AVRO format"),
          "hint" -> ""),
        queryContext = Array(ExpectedContext(
          fragment = "from_avro(s, 42, '')",
          start = 8,
          stop = 27)))
      checkError(
        exception = intercept[AnalysisException](sql(
          s"""
             |select from_avro(s, '$jsonFormatSchema', 42) as result from t
             |""".stripMargin)),
        condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map(
          "sqlExpr" ->
            s"\"from_avro(s, $jsonFormatSchema, 42)\"".stripMargin,
          "msg" -> ("The third argument of the FROM_AVRO SQL function must be a constant map of " +
            "strings to strings containing the options to use for converting the value " +
            "from AVRO format"),
          "hint" -> ""),
        queryContext = Array(ExpectedContext(
          fragment = s"from_avro(s, '$jsonFormatSchema', 42)",
          start = 8,
          stop = 138)))
    }
  }


  test("roundtrip in to_avro and from_avro - recursive schema") {
    val catalystSchema =
      StructType(Seq(
        StructField("Id", IntegerType),
        StructField("Name", StructType(Seq(
          StructField("Id", IntegerType),
          StructField("Name", StructType(Seq(
            StructField("Id", IntegerType)))))))))

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
      spark.sparkContext.parallelize(Seq(Row(2, Row(3, Row(4))), Row(1, null))),
      catalystSchema).select(struct("Id", "Name").as("struct"))

    val avroStructDF = df.select(functions.to_avro($"struct", avroSchema).as("avro"))
    checkAnswer(avroStructDF.select(
      functions.from_avro($"avro", avroSchema, Map(
        "recursiveFieldMaxDepth" -> "3").asJava)), df)
  }

  private def serialize(record: GenericRecord, avroSchema: String): Array[Byte] = {
    val schema = new Schema.Parser().parse(avroSchema)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    var outputStream: ByteArrayOutputStream = null
    var bytes: Array[Byte] = null
    try {
      outputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(outputStream, null)
      datumWriter.write(record, encoder)
      encoder.flush()
      bytes = outputStream.toByteArray
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
    bytes
  }

  private def deserialize(bytes: Array[Byte], avroSchema: String): GenericRecord = {
    val schema = new Schema.Parser().parse(avroSchema)
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    var inputStream: SeekableByteArrayInput = null
    var record: GenericRecord = null
    try {
      inputStream = new SeekableByteArrayInput(bytes)
      val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)
      record = datumReader.read(null, decoder)
    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
    }
    record
  }

  // write: `GenericRecord` -> binary (by `serialize`) -> dataframe
  // read: dataframe -> binary -> `GenericRecord` (by `deserialize`)
  test("roundtrip in serialize and deserialize - GenericRecord") {
    val avroSchema =
      """
        |{
        |  "type": "record",
        |  "name": "person",
        |  "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"},
        |    {"name": "country", "type": "string"}
        |  ]
        |}
        |""".stripMargin
    val testTable = "test_avro"
    withTable(testTable) {
      val schema = new Schema.Parser().parse(avroSchema)
      val person1 = new GenericRecordBuilder(schema)
        .set("name", "sparkA")
        .set("age", 18)
        .set("country", "usa")
        .build()
      val person2 = new GenericRecordBuilder(schema)
        .set("name", "sparkB")
        .set("age", 19)
        .set("country", "usb")
        .build()
      Seq(person1, person2)
        .map(p => serialize(p, avroSchema))
        .toDF("data")
        .repartition(1)
        .writeTo(testTable)
        .create()

      val expectedSchema = new StructType().add("data", BinaryType)
      assert(spark.table(testTable).schema === expectedSchema)

      // Note that what is returned here is `Row[Array[Byte]]`
      val avroDF = sql(s"SELECT data FROM $testTable")
      val readbacks = avroDF
        .collect()
        .map(row => deserialize(row.get(0).asInstanceOf[Array[Byte]], avroSchema))

      val readbackPerson1 = readbacks.head
      assert(readbackPerson1.get(0).toString === person1.get(0))
      assert(readbackPerson1.get(1).asInstanceOf[Int] === person1.get(1).asInstanceOf[Int])
      assert(readbackPerson1.get(2).toString === person1.get(2))

      val readbackPerson2 = readbacks(1)
      assert(readbackPerson2.get(0).toString === person2.get(0))
      assert(readbackPerson2.get(1).asInstanceOf[Int] === person2.get(1).asInstanceOf[Int])
      assert(readbackPerson2.get(2).toString === person2.get(2))
    }
  }

  // write: `GenericRecord` -> binary (by `serialize`) -> dataframe
  // read: dataframe -> binary -> struct (by `from_avro`) -> `GenericRecord`
  test("use `serialize` to write GenericRecord and `from_avro` to read GenericRecord") {
    val avroSchema =
      """
        |{
        |  "type": "record",
        |  "name": "person",
        |  "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"},
        |    {"name": "country", "type": "string"}
        |  ]
        |}
        |""".stripMargin
    val testTable = "test_avro"
    withTable(testTable) {
      val schema = new Schema.Parser().parse(avroSchema)
      val person1 = new GenericRecordBuilder(schema)
        .set("name", "sparkA")
        .set("age", 18)
        .set("country", "usa")
        .build()
      val person2 = new GenericRecordBuilder(schema)
        .set("name", "sparkB")
        .set("age", 19)
        .set("country", "usb")
        .build()
      Seq(person1, person2)
        .map(p => serialize(p, avroSchema))
        .toDF("data")
        .repartition(1)
        .writeTo(testTable)
        .create()

      val expectedSchema = new StructType().add("data", BinaryType)
      assert(spark.table(testTable).schema === expectedSchema)

      // Note that what is returned here is `Row[Struct]`
      val avroDF = sql(s"SELECT from_avro(data, '$avroSchema', map()) FROM $testTable")
      val readbacks = avroDF
        .collect()
        .map(row =>
          new GenericRecordBuilder(schema)
            .set("name", row.getStruct(0).getString(0))
            .set("age", row.getStruct(0).getInt(1))
            .set("country", row.getStruct(0).getString(2))
            .build())

      val readbackPerson1 = readbacks.head
      assert(readbackPerson1.get(0) === person1.get(0))
      assert(readbackPerson1.get(1).asInstanceOf[Int] === person1.get(1).asInstanceOf[Int])
      assert(readbackPerson1.get(2) === person1.get(2))

      val readbackPerson2 = readbacks(1)
      assert(readbackPerson2.get(0) === person2.get(0))
      assert(readbackPerson2.get(1).asInstanceOf[Int] === person2.get(1).asInstanceOf[Int])
      assert(readbackPerson2.get(2) === person2.get(2))
    }
  }

  // write: `GenericRecord` (to `struct`) -> binary (by `to_avro`) -> dataframe
  // read: dataframe -> binary -> `GenericRecord` (by `deserialize`)
  test("use `to_avro` to write GenericRecord and `deserialize` to read GenericRecord") {
    val avroSchema =
      """
        |{
        |  "type": "record",
        |  "name": "person",
        |  "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"},
        |    {"name": "country", "type": "string"}
        |  ]
        |}
        |""".stripMargin
    val testTable = "test_avro"
    withTable(testTable) {
      val schema = new Schema.Parser().parse(avroSchema)
      val person1 = new GenericRecordBuilder(schema)
        .set("name", "sparkA")
        .set("age", 18)
        .set("country", "usa")
        .build()
      val person2 = new GenericRecordBuilder(schema)
        .set("name", "sparkB")
        .set("age", 19)
        .set("country", "usb")
        .build()
      Seq(person1, person2)
        .map(p => (
          p.get(0).asInstanceOf[String],
          p.get(1).asInstanceOf[Int],
          p.get(2).asInstanceOf[String]))
        .toDF("name", "age", "country")
        .select(Fns.to_avro(struct($"name", $"age", $"country"), avroSchema).as("data"))
        .repartition(1)
        .writeTo(testTable)
        .create()

      val expectedSchema = new StructType().add("data", BinaryType)
      assert(spark.table(testTable).schema === expectedSchema)

      // Note that what is returned here is `Row[Array[Byte]]`
      val avroDF = sql(s"select data from $testTable")
      val readbacks = avroDF
        .collect()
        .map(row => row.get(0).asInstanceOf[Array[Byte]])
        .map(bytes => deserialize(bytes, avroSchema))

      val readbackPerson1 = readbacks.head
      assert(readbackPerson1.get(0).toString === person1.get(0))
      assert(readbackPerson1.get(1).asInstanceOf[Int] === person1.get(1).asInstanceOf[Int])
      assert(readbackPerson1.get(2).toString === person1.get(2))

      val readbackPerson2 = readbacks(1)
      assert(readbackPerson2.get(0).toString === person2.get(0))
      assert(readbackPerson2.get(1).asInstanceOf[Int] === person2.get(1).asInstanceOf[Int])
      assert(readbackPerson2.get(2).toString === person2.get(2))
    }
  }

  test("schema_of_avro") {
    val df = spark.range(1)
    val avroIntType = s"""
      |{
      |  "type": "int",
      |  "name": "id"
      |}""".stripMargin
    checkAnswer(df.select(functions.schema_of_avro(avroIntType)), Row("INT"))

    val avroStructType =
      """
        |{
        |  "type": "record",
        |  "name": "person",
        |  "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"},
        |    {"name": "country", "type": "string"}
        |  ]
        |}""".stripMargin
    checkAnswer(df.select(functions.schema_of_avro(avroStructType)),
      Row("STRUCT<name: STRING NOT NULL, age: INT NOT NULL, country: STRING NOT NULL>"))

    val avroMultiType =
      """
        |{
        |  "type": "record",
        |  "name": "person",
        |  "fields": [
        |     {"name": "u", "type": ["int", "string"]}
        |  ]
        |}""".stripMargin
    checkAnswer(df.select(functions.schema_of_avro(avroMultiType)),
      Row("STRUCT<u: STRUCT<member0: INT, member1: STRING> NOT NULL>"))
  }

  test("roundtrip in to_avro and from_avro - TIME type with different precisions") {
    // Avro stores TIME as the time-micros logical type, so this lossless round-trip is limited to
    // precision 0-6. TIME(7-9) truncates to microseconds over Avro (no time-nanos logical type,
    // upstream AVRO-4043); that behavior is covered in AvroSuite.
    val df = spark.sql("""
      SELECT
        CAST(TIME'12:34:56' AS TIME(0)) as time_p0,
        CAST(TIME'12:34:56.1' AS TIME(1)) as time_p1,
        CAST(TIME'12:34:56.12' AS TIME(2)) as time_p2,
        CAST(TIME'12:34:56.123' AS TIME(3)) as time_p3,
        CAST(TIME'12:34:56.1234' AS TIME(4)) as time_p4,
        CAST(TIME'12:34:56.12345' AS TIME(5)) as time_p5,
        CAST(TIME'12:34:56.123456' AS TIME(6)) as time_p6
    """)

    val precisions = Seq(0, 1, 2, 3, 4, 5, 6)
    precisions.foreach { p =>
      val fieldName = s"time_p$p"
      // Generate correct schema for each precision
      val avroTimeSchema = s"""
        |{
        |  "type": "long",
        |  "logicalType": "time-micros",
        |  "spark.sql.catalyst.type": "time($p)"
        |}
      """.stripMargin

      val avroDF = df.select(to_avro(col(fieldName)).as("avro"))
      val readBack = avroDF.select(from_avro($"avro", avroTimeSchema).as(fieldName))

      checkAnswer(readBack, df.select(col(fieldName)))
    }
  }

  test("roundtrip in to_avro and from_avro - TIME type in struct") {
    val df = spark.sql("""
      SELECT
        struct(
          TIME'09:00:00.123' as start,
          TIME'17:30:45.987654' as end,
          'Morning Shift' as description
        ) as schedule
    """)

    val avroStructDF = df.select(to_avro($"schedule").as("avro"))

    val avroStructSchema = """
      |{
      |  "type": "record",
      |  "name": "schedule",
      |  "fields": [
      |    {
      |      "name": "start",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "time-micros",
      |        "spark.sql.catalyst.type": "time(3)"
      |      }
      |    },
      |    {
      |      "name": "end",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "time-micros",
      |        "spark.sql.catalyst.type": "time(6)"
      |      }
      |    },
      |    {
      |      "name": "description",
      |      "type": "string"
      |    }
      |  ]
      |}
    """.stripMargin

    val readBack = avroStructDF.select(
      from_avro($"avro", avroStructSchema).as("schedule"))
    checkAnswer(readBack, df)
  }

  test("SPARK-56043: from_avro with invalid schema should not throw NPE") {
    // An Avro schema that references an undefined type should produce MALFORMED_AVRO_MESSAGE
    // rather than a raw NullPointerException from the Avro library's ParseContext.resolve().
    val invalidSchema =
      """
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "value", "type": "UndefinedType"}
        |  ]
        |}
      """.stripMargin

    val df = spark.range(1).select(lit(Array[Byte](1, 2, 3)).as("data"))
    val ex = intercept[Exception] {
      df.select(from_avro($"data", invalidSchema)).collect()
    }
    assert(!ex.isInstanceOf[NullPointerException],
      s"Should not throw NPE, but got: ${ex.getClass.getName}: ${ex.getMessage}")
  }

  test("SPARK-60242: from_avro CHAR/VARCHAR length enforcement") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val stringSchema = """{"type": "string"}"""
      val charSchema = SchemaConverters.toAvroType(CharType(4), nullable = false).toString
      val varcharSchema = SchemaConverters.toAvroType(VarcharType(4), nullable = false).toString
      val stringMapSchema =
        SchemaConverters.toAvroType(MapType(StringType, IntegerType), nullable = false).toString
      val charMapSchema =
        SchemaConverters.toAvroType(MapType(CharType(2), IntegerType), nullable = false).toString
      val varcharMapSchema =
        SchemaConverters.toAvroType(
          MapType(VarcharType(3), IntegerType), nullable = false).toString

      val padded = spark.range(1).select(
        from_avro(to_avro(lit("ab"), stringSchema), charSchema).as("c"))
      assert(padded.schema.head.dataType === CharType(4))
      checkAnswer(padded.selectExpr("concat('<', c, '>')"), Row("<ab  >"))

      val varcharShort = spark.range(1).select(
        from_avro(to_avro(lit("ab"), stringSchema), varcharSchema).as("v"))
      assert(varcharShort.schema.head.dataType === VarcharType(4))
      checkAnswer(varcharShort, Row("ab"))

      val roundTrip = spark.range(1).selectExpr("cast('ab' AS CHAR(4)) AS c")
        .select(from_avro(to_avro($"c", charSchema), charSchema).as("c"))
      checkAnswer(roundTrip.selectExpr("concat('<', c, '>')"), Row("<ab  >"))

      def assertExceedLimit(body: => Any): Unit = {
        val e = intercept[SparkException] { body }
        val cause = e.getCause match {
          case r: SparkRuntimeException => r
          case other =>
            Option(other).flatMap(t => Option(t.getCause)).getOrElse(other) match {
              case r: SparkRuntimeException => r
              case _ => fail(s"expected EXCEED_LIMIT_LENGTH cause, got: $e")
            }
        }
        checkError(
          exception = cause,
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "4"))
      }
      val oversize = spark.range(1).select(to_avro(lit("abcde"), stringSchema).as("bin"))
      assertExceedLimit(oversize.select(from_avro($"bin", charSchema)).collect())
      assertExceedLimit(oversize.select(from_avro($"bin", varcharSchema)).collect())

      val permissive = oversize.select(
        from_avro($"bin", charSchema, Map("mode" -> "PERMISSIVE").asJava))
      checkAnswer(permissive, Row(null))

      def assertUnsupportedAvroMapKey(
          body: => Any, key: String, dataTypeSql: String): Unit = {
        val error = intercept[Exception] { body }
        val cause = Iterator.iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .collectFirst {
            case e: SparkRuntimeException
              if e.getCondition == "UNSUPPORTED_AVRO_CHAR_VARCHAR_MAP_KEY" => e
          }
          .getOrElse(
            fail("expected UNSUPPORTED_AVRO_CHAR_VARCHAR_MAP_KEY cause", error))
        checkError(
          exception = cause,
          condition = "UNSUPPORTED_AVRO_CHAR_VARCHAR_MAP_KEY",
          sqlState = "0A000",
          parameters = Map("key" -> s"'$key'", "dataType" -> s""""$dataTypeSql""""))
      }

      val exactCharKey = spark.range(1).select(
        from_avro(
          to_avro(map(lit("ab"), lit(1)), stringMapSchema), charMapSchema).as("m"))
      assert(exactCharKey.schema.head.dataType.asInstanceOf[MapType].keyType ===
        CharType(2))
      checkAnswer(exactCharKey, Row(Map("ab" -> 1)))

      assertUnsupportedAvroMapKey(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("a"), lit(1)), stringMapSchema), charMapSchema)).collect(),
        "a",
        "CHAR(2)")
      assertUnsupportedAvroMapKey(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("abc"), lit(1)), stringMapSchema), charMapSchema)).collect(),
        "abc",
        "CHAR(2)")

      val varcharKey = spark.range(1).select(
        from_avro(
          to_avro(map(lit("ab"), lit(1)), stringMapSchema), varcharMapSchema).as("m"))
      checkAnswer(varcharKey, Row(Map("ab" -> 1)))
      val varcharSpacedKey = spark.range(1).select(
        from_avro(
          to_avro(map(lit("ab "), lit(1)), stringMapSchema), varcharMapSchema).as("m"))
      checkAnswer(varcharSpacedKey, Row(Map("ab " -> 1)))
      assertUnsupportedAvroMapKey(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("abcd"), lit(1)), stringMapSchema),
            varcharMapSchema)).collect(),
        "abcd",
        "VARCHAR(3)")

      checkAnswer(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("a"), lit(1)), stringMapSchema),
            charMapSchema,
            Map("mode" -> "PERMISSIVE").asJava)),
        Row(null))

      val char0MapSchema =
        SchemaConverters.toAvroType(MapType(CharType(0), IntegerType), nullable = false)
          .toString
      checkAnswer(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit(""), lit(1)), stringMapSchema), char0MapSchema).as("m")),
        Row(Map("" -> 1)))
      assertUnsupportedAvroMapKey(
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("a"), lit(1)), stringMapSchema), char0MapSchema)).collect(),
        "a",
        "CHAR(0)")

      val stringStructSchema =
        SchemaConverters.toAvroType(
          new StructType().add("c", StringType), nullable = false).toString
      val charStructSchema =
        SchemaConverters.toAvroType(
          new StructType().add("c", CharType(4)), nullable = false).toString
      checkAnswer(
        spark.range(1).select(
          from_avro(
            to_avro(struct(lit("ab").as("c")), stringStructSchema),
            charStructSchema).as("s"))
          .selectExpr("concat('<', s.c, '>')"),
        Row("<ab  >"))

      withTempView("avro_cv_map_view") {
        spark.range(1).select(
          from_avro(
            to_avro(map(lit("a"), lit(1)), stringMapSchema), charMapSchema).as("m"))
          .createOrReplaceTempView("avro_cv_map_view")
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
          assertUnsupportedAvroMapKey(
            spark.sql("SELECT * FROM avro_cv_map_view").collect(),
            "a",
            "CHAR(2)")
        }
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).select(
          map(lit("a"), lit(1), lit("a "), lit(2)).as("m"))
          .write.mode("overwrite").format("avro").save(path)
        assertUnsupportedAvroMapKey(
          spark.read.schema("m MAP<CHAR(2), INT>").format("avro").load(path).collect(),
          "a",
          "CHAR(2)")
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).select(
          map(lit("ab"), lit(1), lit("a "), lit(2)).as("m"))
          .write.mode("overwrite").format("avro").save(path)
        checkAnswer(
          spark.read.schema("m MAP<CHAR(2), INT>").format("avro").load(path),
          Row(Map("ab" -> 1, "a " -> 2)))
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).select(map(lit("ab"), lit("xy")).as("m"))
          .write.mode("overwrite").format("avro").save(path)
        val nested = spark.read.schema("m MAP<CHAR(2), CHAR(4)>").format("avro").load(path)
        checkAnswer(nested.selectExpr("concat('<', m['ab'], '>')"), Row("<xy  >"))
      }

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).select(map(lit("abcd"), lit(1)).as("m"))
          .write.mode("overwrite").format("avro").save(path)
        assertUnsupportedAvroMapKey(
          spark.read.schema("m MAP<VARCHAR(3), INT>").format("avro").load(path).collect(),
          "abcd",
          "VARCHAR(3)")
      }
    }

    withSQLConf(
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      val stringSchema = """{"type": "string"}"""
      val charSchema = SchemaConverters.toAvroType(CharType(4), nullable = false).toString
      val stringMapSchema =
        SchemaConverters.toAvroType(MapType(StringType, IntegerType), nullable = false).toString
      val charMapSchema =
        SchemaConverters.toAvroType(MapType(CharType(2), IntegerType), nullable = false).toString
      val padded = spark.range(1).select(
        from_avro(to_avro(lit("ab"), stringSchema), charSchema).as("c"))
      assert(padded.schema.head.dataType === CharType(4))
      checkAnswer(padded.selectExpr("concat('<', c, '>')"), Row("<ab  >"))
      // Off-width CHAR keys are not rejected or rewritten when standardSemantics is off.
      // Exclude ConstantFolding so a CHAR Cast cannot pad the folded map key. Collecting
      // MAP<CHAR(n), _> would still pad via the encoder, so to_json shows the raw key.
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
        val shortKey = spark.range(1).select(
          from_avro(
            to_avro(map(lit("a"), lit(1)), stringMapSchema), charMapSchema).as("m"))
        assert(shortKey.schema.head.dataType.asInstanceOf[MapType].keyType === CharType(2))
        checkAnswer(shortKey.selectExpr("to_json(m)"), Row("""{"a":1}"""))
      }
    }

    withSQLConf(
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false",
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "false") {
      val stringSchema = """{"type": "string"}"""
      val charSchema = SchemaConverters.toAvroType(CharType(4), nullable = false).toString
      val df = spark.range(1).select(
        from_avro(to_avro(lit("abcdef"), stringSchema), charSchema).as("c"))
      assert(df.schema.head.dataType === StringType)
      checkAnswer(df, Row("abcdef"))

      withTempView("avro_cv_string_view") {
        df.createOrReplaceTempView("avro_cv_string_view")
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
          val fromView = spark.sql("SELECT * FROM avro_cv_string_view")
          assert(fromView.schema.head.dataType === StringType)
          checkAnswer(fromView, Row("abcdef"))
        }
      }
    }
  }

  test("SPARK-56043: from_avro with completely unparseable schema should not throw NPE") {
    val garbageSchema = "this is not valid JSON at all"

    val df = spark.range(1).select(lit(Array[Byte](1, 2, 3)).as("data"))
    val ex = intercept[Exception] {
      df.select(from_avro($"data", garbageSchema)).collect()
    }
    assert(!ex.isInstanceOf[NullPointerException],
      s"Should not throw NPE, but got: ${ex.getClass.getName}: ${ex.getMessage}")
  }

  private val avroIntSchema = """{"type": "int"}"""

  private def avroExpression(
      functionName: String,
      argument: String,
      onEval: () => Unit,
      duplicateKey: Boolean = false): (Expression, AttributeReference) = {
    val secondKey = StringLiteralWithEvalHook(if (duplicateKey) "key1" else "key2", onEval)
    val value1 = if (argument == "schema") avroIntSchema else "value1"
    val value2 = if (argument == "schema") avroIntSchema else "value2"
    val mapExpression = CreateMap(Seq(
      Literal("key1"), Literal(value1), secondKey, Literal(value2)))
    val schema = if (argument == "schema") {
      ElementAt(mapExpression, Literal("key1"))
    } else {
      Literal(avroIntSchema)
    }
    val options = if (argument == "options") mapExpression else Literal(null)
    avroExpressionOf(functionName, schema, options)
  }

  // A lookup in a map whose keys are 0.0 and -0.0, which are one key unless
  // spark.sql.legacy.disableMapKeyNormalization is true.
  private def signedZeroKeyLookup(value: String): Expression = ElementAt(
    CreateMap(Seq(Literal(0.0D), Literal(value), Literal(-0.0D), Literal(value))),
    Literal(0.0D))

  private def avroExpressionWithSignedZeroKeys(
      functionName: String,
      argument: String): (Expression, AttributeReference) = {
    val schema =
      if (argument == "schema") signedZeroKeyLookup(avroIntSchema) else Literal(avroIntSchema)
    val options = if (argument == "options") {
      CreateMap(Seq(Literal("key1"), signedZeroKeyLookup("value1")))
    } else {
      Literal(null)
    }
    avroExpressionOf(functionName, schema, options)
  }

  private def avroExpressionOf(
      functionName: String,
      schema: Expression,
      options: Expression): (Expression, AttributeReference) = {
    val dataType = if (functionName == "TO_AVRO") IntegerType else BinaryType
    val data = AttributeReference("data", dataType)(exprId = ExprId(1))
    val expression = functionName match {
      case "FROM_AVRO" => new FromAvro(data, schema, options)
      case "TO_AVRO" => new ToAvro(data, schema)
      case "SCHEMA_OF_AVRO" => new SchemaOfAvroExpression(schema, options)
    }
    (expression, data)
  }

  private def avroOptions(expression: Expression): Map[String, String] = expression match {
    case from: AvroDataToCatalyst =>
      assert(from.jsonFormatSchema == avroIntSchema)
      from.options
    case to: CatalystDataToAvro =>
      assert(to.jsonFormatSchema.contains(avroIntSchema))
      Map.empty
    case schema: SchemaOfAvro =>
      assert(schema.jsonFormatSchema == avroIntSchema)
      schema.options
    case other => fail(s"Unexpected Avro expression: ${other.getClass.getName}")
  }

  private def canonicalizedOptions(
      expression: Expression,
      data: AttributeReference): Map[String, String] = {
    avroOptions(QueryPlan.normalizeExpressions(expression, Seq(data)))
  }

  private def replacementOptions(expression: Expression): Map[String, String] = {
    avroOptions(expression.asInstanceOf[RuntimeReplaceable].replacement)
  }

  // SCHEMA_OF_AVRO has no column input, so canonicalization keeps it; clone it to get a copy.
  private def copyOptions(
      functionName: String,
      expression: Expression,
      data: AttributeReference): Map[String, String] = {
    if (functionName == "SCHEMA_OF_AVRO") {
      replacementOptions(expression.clone())
    } else {
      canonicalizedOptions(expression, data)
    }
  }

  private def avroDataFrame(
      functionName: String,
      argument: String,
      signedZeroKeys: Boolean = false): DataFrame = {
    val schema = (argument, signedZeroKeys) match {
      case ("schema", false) =>
        s"element_at(map('schema', '$avroIntSchema', 'schema', '$avroIntSchema'), 'schema')"
      case ("schema", true) =>
        s"element_at(map(0.0D, '$avroIntSchema', -0.0D, '$avroIntSchema'), 0.0D)"
      case _ => s"'$avroIntSchema'"
    }
    val options = (argument, signedZeroKeys) match {
      case ("options", false) => "map('mode', 'PERMISSIVE', 'mode', 'FAILFAST')"
      case ("options", true) =>
        "map('mode', element_at(map(0.0D, 'PERMISSIVE', -0.0D, 'PERMISSIVE'), 0.0D))"
      case _ => "map()"
    }
    functionName match {
      case "FROM_AVRO" =>
        spark.range(1).select(lit(Array[Byte](2)).as("data"))
          .selectExpr(s"FROM_AVRO(data, $schema, $options)")
      case "TO_AVRO" =>
        spark.range(1).select(lit(1).as("data")).selectExpr(s"TO_AVRO(data, $schema)")
      case "SCHEMA_OF_AVRO" =>
        spark.range(1).selectExpr(s"SCHEMA_OF_AVRO($schema, $options)")
    }
  }

  private def avroDataFrameResult(functionName: String): Row = functionName match {
    case "FROM_AVRO" => Row(1)
    case "TO_AVRO" => Row(Array[Byte](2))
    case "SCHEMA_OF_AVRO" => Row("INT")
  }

  Seq("FROM_AVRO", "TO_AVRO", "SCHEMA_OF_AVRO").foreach { functionName =>
    val arguments = if (functionName == "TO_AVRO") Seq("schema") else Seq("schema", "options")
    arguments.foreach { argument =>
      val expectedOptions = if (argument == "options") {
        Map("key1" -> "value1", "key2" -> "value2")
      } else {
        Map.empty[String, String]
      }

      // Skip SCHEMA_OF_AVRO: it has no column input, so canonicalization reuses the same instance,
      // whose lazy replacement runs once, instead of copying it.
      if (functionName != "SCHEMA_OF_AVRO") {
        gridTest(s"SPARK-61419: $functionName $argument survives overlapping canonicalization")(
          Seq("EXCEPTION", "LAST_WIN")) { policy =>
          withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> policy) {
            val firstEvaluationPaused = new CountDownLatch(1)
            val resumeFirstEvaluation = new CountDownLatch(1)
            val pauseOnNextEval = new AtomicBoolean(true)
            val (expression, data) = avroExpression(functionName, argument, () => {
              if (pauseOnNextEval.compareAndSet(true, false)) {
                firstEvaluationPaused.countDown()
                assert(resumeFirstEvaluation.await(1, TimeUnit.MINUTES),
                  "first evaluation was never resumed")
              }
            })
            val sqlConf = SQLConf.get
            val executor = ThreadUtils.newDaemonSingleThreadExecutor("avro-argument-race-test")
            val executionContext = ExecutionContext.fromExecutorService(executor)
            try {
              val firstEvaluation = Future {
                SQLConf.withExistingConf(sqlConf) {
                  canonicalizedOptions(expression, data)
                }
              }(executionContext)
              assert(firstEvaluationPaused.await(1, TimeUnit.MINUTES),
                "first canonicalization never paused")
              // The first evaluation has inserted key1 and is paused while evaluating key2.
              assert(canonicalizedOptions(expression, data) == expectedOptions)
              resumeFirstEvaluation.countDown()
              assert(ThreadUtils.awaitResult(firstEvaluation, 1.minute) == expectedOptions)
            } finally {
              resumeFirstEvaluation.countDown()
              executor.shutdownNow()
              assert(executor.awaitTermination(10, TimeUnit.SECONDS))
            }
          }
        }
      }

      test(s"SPARK-61419: $functionName $argument survives a failed evaluation") {
        withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> "EXCEPTION") {
          val failOnNextEval = new AtomicBoolean(true)
          val expectedFailure = new RuntimeException(s"first $argument evaluation failed")
          val (expression, _) = avroExpression(functionName, argument, () => {
            if (failOnNextEval.compareAndSet(true, false)) {
              throw expectedFailure
            }
          })
          val failure = intercept[RuntimeException] {
            replacementOptions(expression)
          }
          assert(failure eq expectedFailure)

          // Retrying must not reuse the partial map left by the failed evaluation.
          assert(replacementOptions(expression) == expectedOptions)
        }
      }

      gridTest(s"SPARK-61419: a copy of $functionName uses the analysis-time map key policy " +
        s"for $argument")(
        Seq("LAST_WIN", "EXCEPTION")) { capturedPolicy =>
        val expectedDuplicateOptions = if (argument == "options") {
          Map("key1" -> "value2")
        } else {
          Map.empty[String, String]
        }
        val (expression, data) = withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> capturedPolicy) {
          val result = avroExpression(functionName, argument, () => (), duplicateKey = true)
          if (capturedPolicy == "LAST_WIN") {
            // Like analysis, evaluate the expression once before the conf changes. Under EXCEPTION
            // this would already fail on the duplicate key, so only the LAST_WIN run does it.
            assert(replacementOptions(result._1) == expectedDuplicateOptions)
          }
          result
        }
        val currentPolicy = if (capturedPolicy == "LAST_WIN") "EXCEPTION" else "LAST_WIN"
        withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> currentPolicy) {
          if (capturedPolicy == "LAST_WIN") {
            assert(copyOptions(functionName, expression, data) == expectedDuplicateOptions)
          } else {
            checkError(
              exception = intercept[SparkRuntimeException] {
                copyOptions(functionName, expression, data)
              },
              condition = "DUPLICATED_MAP_KEY",
              parameters = Map(
                "key" -> "key1",
                "mapKeyDedupPolicy" -> s"\"${SQLConf.MAP_KEY_DEDUP_POLICY.key}\""))
          }
          assert(SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString == currentPolicy)
        }
      }

      test(s"SPARK-61419: $functionName $argument executes after the map key policy changes") {
        val df = withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> "LAST_WIN") {
          avroDataFrame(functionName, argument)
        }
        withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> "EXCEPTION") {
          checkAnswer(df, Seq(avroDataFrameResult(functionName)))
        }
      }

      gridTest(s"SPARK-61419: a copy of $functionName uses the analysis-time map key " +
        s"normalization for $argument")(Seq(true, false)) { capturedDisabled =>
        val policy = SQLConf.MAP_KEY_DEDUP_POLICY.key -> "EXCEPTION"
        val normalization = SQLConf.DISABLE_MAP_KEY_NORMALIZATION.key
        val expectedOptions = if (argument == "options") {
          Map("key1" -> "value1")
        } else {
          Map.empty[String, String]
        }
        val (expression, data) = withSQLConf(policy, normalization -> capturedDisabled.toString) {
          val result = avroExpressionWithSignedZeroKeys(functionName, argument)
          if (capturedDisabled) {
            // Like analysis, evaluate the expression once before the conf changes. With
            // normalization, 0.0 and -0.0 are one key, so only the disabled run does it.
            assert(replacementOptions(result._1) == expectedOptions)
          }
          result
        }
        withSQLConf(policy, normalization -> (!capturedDisabled).toString) {
          if (capturedDisabled) {
            assert(copyOptions(functionName, expression, data) == expectedOptions)
          } else {
            checkError(
              exception = intercept[SparkRuntimeException] {
                copyOptions(functionName, expression, data)
              },
              condition = "DUPLICATED_MAP_KEY",
              parameters = Map(
                "key" -> "-0.0",
                "mapKeyDedupPolicy" -> s"\"${SQLConf.MAP_KEY_DEDUP_POLICY.key}\""))
          }
        }
      }

      test(s"SPARK-61419: $functionName $argument executes after the map key normalization " +
        "changes") {
        val policy = SQLConf.MAP_KEY_DEDUP_POLICY.key -> "EXCEPTION"
        val normalization = SQLConf.DISABLE_MAP_KEY_NORMALIZATION.key
        val df = withSQLConf(policy, normalization -> "true") {
          avroDataFrame(functionName, argument, signedZeroKeys = true)
        }
        withSQLConf(policy, normalization -> "false") {
          checkAnswer(df, Seq(avroDataFrameResult(functionName)))
        }
      }

      // Skip SCHEMA_OF_AVRO for the same reason: a cache lookup does not copy it.
      if (functionName != "SCHEMA_OF_AVRO") {
        test(s"SPARK-61419: $functionName $argument cached plan retains its map key policy " +
          "across sessions") {
          val cached = withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> "LAST_WIN") {
            avroDataFrame(functionName, argument).cache()
          }
          try {
            val otherSession = spark.newSession()
            otherSession.conf.set(SQLConf.MAP_KEY_DEDUP_POLICY.key, "EXCEPTION")
            checkAnswer(otherSession.range(1).toDF(), Seq(Row(0L)))
          } finally {
            withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> "LAST_WIN") {
              cached.unpersist(blocking = true)
            }
          }
        }
      }
    }
  }
}
