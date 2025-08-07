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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.VariantVal

/**
 * Test shredding Variant values in the Parquet reader/writer.
 */
class ParquetVariantShreddingSuite extends QueryTest with ParquetTest with SharedSparkSession {

  private def testWithTempDir(name: String)(block: File => Unit): Unit = test(name) {
    withTempDir { dir =>
      block(dir)
    }
  }

  test("timestamp physical type") {
    ParquetOutputTimestampType.values.foreach { timestampParquetType =>
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> timestampParquetType.toString) {
        withTempDir { dir =>
          val schema = "t timestamp, st struct<t timestamp>, at array<timestamp>"
          val fullSchema = "v struct<metadata binary, value binary, typed_value struct<" +
            "t struct<value binary, typed_value timestamp>," +
            "st struct<" +
            "value binary, typed_value struct<t struct<value binary, typed_value timestamp>>>," +
            "at struct<" +
              "value binary, typed_value array<struct<value binary, typed_value timestamp>>>" +
            ">>, " +
            "t1 timestamp, st1 struct<t1 timestamp>"
          val df = spark.sql(
            """
              | select
              |   to_variant_object(
              |     named_struct('t', 1::timestamp, 'st', named_struct('t', 2::timestamp),
              |     'at', array(5::timestamp))
              |   ) v, 3::timestamp t1, named_struct('t1', 4::timestamp) st1
              | from range(1)
              |""".stripMargin)
          withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
            SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
            SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {
            df.write.mode("overwrite").parquet(dir.getAbsolutePath)
            checkAnswer(
              spark.read.parquet(dir.getAbsolutePath).selectExpr("to_json(v)"),
              df.selectExpr("to_json(v)").collect()
            )
            val shreddedDf = spark.read.schema(fullSchema).parquet(dir.getAbsolutePath)
            checkAnswer(
              shreddedDf.selectExpr("v.typed_value.t.typed_value::long"),
              Seq(Row(1)))
            checkAnswer(
              shreddedDf.selectExpr("v.typed_value.st.typed_value.t.typed_value::long"),
              Seq(Row(2)))
            checkAnswer(
              shreddedDf.selectExpr("t1::long"),
              Seq(Row(3)))
            checkAnswer(
              shreddedDf.selectExpr("st1.t1::long"),
              Seq(Row(4)))
            checkAnswer(
              shreddedDf.selectExpr("v.typed_value.at.typed_value[0].typed_value::long"),
              Seq(Row(5)))
            val file = dir.listFiles().find(_.getName.endsWith(".parquet")).get
            val parquetFilePath = file.getAbsolutePath
            val inputFile = HadoopInputFile.fromPath(new Path(parquetFilePath), new Configuration())
            val reader = ParquetFileReader.open(inputFile)
            val footer = reader.getFooter
            val schema = footer.getFileMetaData.getSchema
            // v.typed_value.t.typed_value
            val vGroup = schema.getType(schema.getFieldIndex("v")).asGroupType()
            val typedValueGroup = vGroup.getType("typed_value").asGroupType()
            val tGroup = typedValueGroup.getType("t").asGroupType()
            val typedValue1 = tGroup.getType("typed_value").asPrimitiveType()
            assert(typedValue1.getPrimitiveTypeName == PrimitiveTypeName.INT64)
            assert(typedValue1.getLogicalTypeAnnotation == LogicalTypeAnnotation.timestampType(
              true, LogicalTypeAnnotation.TimeUnit.MICROS))

            // v.typed_value.st.typed_value.t.typed_value
            val stGroup = typedValueGroup.getType("st").asGroupType()
            val stTypedValueGroup = stGroup.getType("typed_value").asGroupType()
            val stTGroup = stTypedValueGroup.getType("t").asGroupType()
            val typedValue2 = stTGroup.getType("typed_value").asPrimitiveType()
            assert(typedValue2.getPrimitiveTypeName == PrimitiveTypeName.INT64)
            assert(typedValue2.getLogicalTypeAnnotation == LogicalTypeAnnotation.timestampType(
              true, LogicalTypeAnnotation.TimeUnit.MICROS))

            // v.typed_value.at.typed_value[0].typed_value
            val atGroup = typedValueGroup.getType("at").asGroupType()
            val atTypedValueGroup = atGroup.getType("typed_value").asGroupType()
            val atLGroup = atTypedValueGroup.getType("list").asGroupType()
            val atLEGroup = atLGroup.getType("element").asGroupType()
            val typedValue3 = atLEGroup.getType("typed_value").asPrimitiveType()
            assert(typedValue3.getPrimitiveTypeName == PrimitiveTypeName.INT64)
            assert(typedValue3.getLogicalTypeAnnotation == LogicalTypeAnnotation.timestampType(
              true, LogicalTypeAnnotation.TimeUnit.MICROS))

            def verifyNonVariantTimestampType(t: PrimitiveType): Unit = {
              timestampParquetType match {
                case ParquetOutputTimestampType.INT96 =>
                  assert(t.getPrimitiveTypeName == PrimitiveTypeName.INT96)
                  assert(t.getLogicalTypeAnnotation == null)
                case ParquetOutputTimestampType.TIMESTAMP_MICROS =>
                  assert(t.getPrimitiveTypeName == PrimitiveTypeName.INT64)
                  assert(t.getLogicalTypeAnnotation == LogicalTypeAnnotation.timestampType(
                    true, LogicalTypeAnnotation.TimeUnit.MICROS))
                case ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
                  assert(t.getPrimitiveTypeName == PrimitiveTypeName.INT64)
                  assert(t.getLogicalTypeAnnotation == LogicalTypeAnnotation.timestampType(
                    true, LogicalTypeAnnotation.TimeUnit.MILLIS))
              }
            }

            // t1
            val t1Value = schema.getType(schema.getFieldIndex("t1")).asPrimitiveType()
            verifyNonVariantTimestampType(t1Value)

            // st1.t1
            val st1Group = schema.getType(schema.getFieldIndex("st1")).asGroupType()
            val st1T1Value = st1Group.getType("t1").asPrimitiveType()
            verifyNonVariantTimestampType(st1T1Value)
            reader.close()
          }
        }
      }
    }
  }


  testWithTempDir("write shredded variant basic") { dir =>
    val schema = "a int, b string, c decimal(15, 1)"
    val df = spark.sql(
      """
        | select case
        | when id = 0 then parse_json('{"a": 1, "b": "2", "c": 3.3, "d": 4.4}')
        | when id = 1 then parse_json('{"a": [1,2,3], "b": "hello", "c": {"x": 0}}')
        | when id = 2 then parse_json('{"A": 1, "c": 1.23}')
        | end v from range(3)
        |""".stripMargin)
    val fullSchema = "v struct<metadata binary, value binary, typed_value struct<" +
      "a struct<value binary, typed_value int>, b struct<value binary, typed_value string>," +
      "c struct<value binary, typed_value decimal(15, 1)>>>"
    withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
      SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)


      // Verify that we can read the full variant. The exact binary layout can change before and
      // after shredding, so just check that the JSON representation matches.
      checkAnswer(
        spark.read.parquet(dir.getAbsolutePath).selectExpr("to_json(v)"),
        df.selectExpr("to_json(v)").collect()
      )

      // Verify that it was shredded to the expected fields.

      val shreddedDf = spark.read.schema(fullSchema).parquet(dir.getAbsolutePath)
      // Metadata should be unchanaged.
      checkAnswer(shreddedDf.selectExpr("v.metadata"),
        df.collect().map(v => Row(v.get(0).asInstanceOf[VariantVal].getMetadata))
      )

      // Check typed values.
      // Second row is not an integer, and third is A, not a
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.a.typed_value"),
        Seq(Row(1), Row(null), Row(null)))
      // b is missing from third row.
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.b.typed_value"),
        Seq(Row("2"), Row("hello"), Row(null)))
      // Second row is an object, third is the wrong scale. (Note: we may eventually allow the
      // latter, in which case this test should be updated.)
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.c.typed_value"),
        Seq(Row(3.3), Row(null), Row(null)))

      // Untyped values are more awkward to check, so for now just check their nullness. We
      // can do more thorough checking once the reader is ready.
      checkAnswer(
        shreddedDf.selectExpr("v.value is null"),
        // First row has "d" and third has "A".
        Seq(Row(false), Row(true), Row(false)))
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.a.value is null"),
        // First row is fully shredded, third is missing.
        Seq(Row(true), Row(false), Row(true)))
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.b.value is null"),
        // b is always fully shredded or missing.
        Seq(Row(true), Row(true), Row(true)))
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.c.value is null"),
        Seq(Row(true), Row(false), Row(false)))
      // The a/b/c levels are not null, even if the field is missing.
      checkAnswer(
        shreddedDf.selectExpr(
          "v.typed_value.a is null or v.typed_value.b is null or v.typed_value.c is null"),
        Seq(Row(false), Row(false), Row(false)))
    }
  }

  testWithTempDir("write shredded variant array") { dir =>
    val schema = "array<int>"
    val df = spark.sql(
      """
        | select case
        | when id = 0 then parse_json('[1, "2", 3.5, null, 5]')
        | when id = 1 then parse_json('{"a": [1, 2, 3]}')
        | when id = 2 then parse_json('1')
        | when id = 3 then parse_json('null')
        | end v from range(4)
        |""".stripMargin)
    val fullSchema = "v struct<metadata binary, value binary, typed_value array<" +
      "struct<value binary, typed_value int>>>"
    withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
      SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)

      // Verify that we can read the full variant.
      checkAnswer(
        spark.read.parquet(dir.getAbsolutePath).selectExpr("to_json(v)"),
        df.selectExpr("to_json(v)").collect()
      )

      // Verify that it was shredded to the expected fields.

      val shreddedDf = spark.read.schema(fullSchema).parquet(dir.getAbsolutePath)
      // Metadata should be unchanaged.
      checkAnswer(shreddedDf.selectExpr("v.metadata"),
        df.collect().map(v => Row(v.get(0).asInstanceOf[VariantVal].getMetadata))
      )

      // Check typed values.
      checkAnswer(
        shreddedDf.selectExpr("v.typed_value.typed_value"),
        Seq(Row(Array(1, null, null, null, 5)), Row(null), Row(null), Row(null)))

      // All the other array elements should have non-null value.
      checkAnswer(
        shreddedDf.selectExpr("transform(v.typed_value.value, v -> v is null)"),
        Seq(Row(Array(true, false, false, false, true)), Row(null), Row(null), Row(null)))

      // The non-arrays should have non-null top-level value.
      checkAnswer(
        shreddedDf.selectExpr("v.value is null"),
        Seq(Row(true), Row(false), Row(false), Row(false)))
    }
  }

  testWithTempDir("write no shredding schema") { dir =>
    // Check that we can write and read normally when shredding is enabled if
    // we don't provide a shredding schema.
    withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString) {
      val df = spark.sql(
        """
          | select parse_json('{"a": ' || id || ', "b": 2}') as v,
          | array(parse_json('{"c": 3}'), 123::variant) as a
          | from range(1, 3, 1, 1)
          |""".stripMargin)
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)
      checkAnswer(
        spark.read.parquet(dir.getAbsolutePath), df.collect()
      )
    }
  }

  testWithTempDir("arrays and maps ignore shredding schema") { dir =>
    // Check that we don't try to shred array or map elements, even if a shredding schema
    // is specified.
    val schema = "a int"
    val df = spark.sql(
      """ select v, array(v) as arr, map('myKey', v) as m from
        | (select parse_json('{"a":' || id || '}') v from range(3))
        |""".stripMargin)
    val fullSchema = "v struct<metadata binary, value binary, typed_value struct<" +
      "a struct<value binary, typed_value int>>>, " +
      "arr array<struct<metadata binary, value binary>>, " +
      "m map<string, struct<metadata binary, value binary>>"
    withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
      SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)

      // Verify that we can read the full variant.
      checkAnswer(
        spark.read.parquet(dir.getAbsolutePath).selectExpr("to_json(v)"),
        df.selectExpr("to_json(v)").collect()
      )

      // Verify that it was shredded to the expected fields.

      val shreddedDf = spark.read.schema(fullSchema).parquet(dir.getAbsolutePath)
      // Metadata should be unchanaged.
      checkAnswer(shreddedDf.selectExpr("v.metadata"),
        df.selectExpr("v").collect().map(v => Row(v.get(0).asInstanceOf[VariantVal].getMetadata))
      )
      checkAnswer(shreddedDf.selectExpr("arr[0].metadata"),
        df.selectExpr("arr[0]").collect().map(v =>
          Row(v.get(0).asInstanceOf[VariantVal].getMetadata))
      )
      checkAnswer(shreddedDf.selectExpr("m['myKey'].metadata"),
        df.selectExpr("m['myKey']").collect().map(
          v => Row(v.get(0).asInstanceOf[VariantVal].getMetadata))
      )

      // v should be fully shredded, but the array and map should not be.
      checkAnswer(
        shreddedDf.selectExpr(
          "v.value is null"),
        Seq(Row(true), Row(true), Row(true)))
      checkAnswer(
        shreddedDf.selectExpr(
          "arr[0].value is null"),
        Seq(Row(false), Row(false), Row(false)))
      checkAnswer(
        shreddedDf.selectExpr(
          "m['myKey'].value is null"),
        Seq(Row(false), Row(false), Row(false)))
    }
  }
}
