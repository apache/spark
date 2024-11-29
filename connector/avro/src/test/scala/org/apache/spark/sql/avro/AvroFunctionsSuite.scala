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

import scala.jdk.CollectionConverters._

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.avro.{functions => Fns}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}

class AvroFunctionsSuite extends QueryTest with SharedSparkSession {
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
}
