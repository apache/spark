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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetTest, ParquetToSparkSchemaConverter, SparkShreddingUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.unsafe.types.VariantVal

// Unit tests for variant shredding inference.
class VariantInferShreddingSuite extends QueryTest with SharedSparkSession with ParquetTest {
  override def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.PUSH_VARIANT_INTO_SCAN.key, "true")
      .set(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key, "true")
      .set(SQLConf.VARIANT_INFER_SHREDDING_SCHEMA.key, "true")
      // We cannot check the physical shredding schemas if the variant logical type annotation is
      // used
      .set(SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key, "false")
  }

  private def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }


  private def testWithTempDir(name: String)(block: File => Unit): Unit = test(name) {
    withTempDir { dir =>
      block(dir)
    }
  }

  def getFooters(dir: File): Seq[org.apache.parquet.hadoop.Footer] = {
    val fs = FileSystem.get(spark.sessionState.newHadoopConf())
    val fileStatuses = fs.listStatus(new Path(dir.getPath))
      .filter(_.getPath.toString.endsWith(".parquet"))
      .toIndexedSeq
    ParquetFileFormat.readParquetFootersInParallel(
      spark.sessionState.newHadoopConf(), fileStatuses, ignoreCorruptFiles = false)
  }

  // Checks that exactly one parquet file exists at the provided path, and returns its schema.
  def getFileSchema(dir: File): StructType = {
    val footers = getFooters(dir)
    assert(footers.size == 1)
    new ParquetToSparkSchemaConverter()
      .convert(footers(0).getParquetMetadata.getFileMetaData.getSchema)
  }

  // Check that the parquet file at the given path contains exactly one field, "v",
  // with the expected schema.
  def checkFileSchema(expectedSimpleSchema: DataType, dir: File): Unit = {
    val expected = SparkShreddingUtils.variantShreddingSchema(expectedSimpleSchema)
    val actual = getFileSchema(dir)
    // Depending on the data, Spark may be able to infer that the top-level column is
    // non-nullable, so accept either one.
    assert(actual == StructType(Seq(StructField("v", expected, nullable = false))) ||
      actual == StructType(Seq(StructField("v", expected, nullable = true))))
  }

  // Given a DF with a field named "v", check that the string representation and schema_of_variant
  // match. We can't check the binary values directly, because they can change before and after
  // shredding.
  def checkStringAndSchema(dir: File, expected: DataFrame, field: String = "v"): Unit = {
    checkAnswer(
      spark.read.parquet(dir.getAbsolutePath).selectExpr(s"$field::string",
        s"schema_of_variant($field)"),
      expected.selectExpr(s"$field::string", s"schema_of_variant($field)").collect()
    )
  }

  testWithTempDir("infer shredding schema basic") { dir =>
    // Check that we can write and read normally when shredding is enabled if
    // we don't provide a shredding schema.
    val df = spark.sql(
      """
        | select parse_json('{"a": ' || id || ', "b": "' || id || '"}') as v
        | from range(0, 3, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    // Inferred integer columns are always widened to long
    val expected = DataType.fromDDL("struct<a long, b string>")
    checkFileSchema(expected, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }

  test("infer shredding does not infer rare rows") {
    Seq(2, 9, 10, 11, 19, 20, 21, 100).foreach { inverseFreq =>
      withTempDir { dir =>
        // Test two infrequent array-of-object cases:
        // 1) rareArray: If an array is rarely present, it is dropped from the schema, even if a
        //    field in the array appears many times when the array is present.
        // 2) rareArray2: If an array is usually present, then its fields are weighted by how many
        //    times they occur in total. E.g. a field that is only present in 1% of rows, but
        //    occurs in more than 10 elements in that row will be included in the schema, since it
        //    appears "on average" in 10% of rows.
        // These rules are a bit arbitrary, and the second one especially is mainly done to
        // keep the algorithm simple, but validate it here, and we can consider revisiting it
        // in the future.
        val df = spark.sql(
          s"""
             | select case when id % $inverseFreq = 0 then
             |  parse_json('{"a": ' || id ||
             |  ', "rareArray": [{"x": 1}, {"x": 2}, {"y": 3}]' ||
             |  ', "rareArray2": [{"x": 1}, {"x": 2}, {"y": 3}]' ||
             |  ', "rareField" : "xyz"}')
             |  else
             |  parse_json('{"a": ' || id ||
             |  ', "rareArray2": []' ||
             |  ', "b": "' || id || '"}')
             |  end as v
             |  from range(0, 10000, 1, 1)
             |""".stripMargin)
        df.write.mode("overwrite").parquet(dir.getAbsolutePath)
        // "a" appears in all rows, and "b" appears in at least 50% of rows, so they should always
        // show up in the schema. The other fields depend on how frequently they appear.
        val expected = if (inverseFreq > 20) {
          // rareArray2 is always present, but none of its fields are frequent enough to
          // include in the schema, so the element shows up as `variant`.
          DataType.fromDDL("struct<a long, b string, rareArray2 array<variant>>")
        } else if (inverseFreq > 10) {
          // The "x" field in rareArray2 appears twice in each of the 5-10% of rows, so on average
          // it appears in over 10% of rows.
          DataType.fromDDL("struct<a long, b string, rareArray2 array<struct<x long>>>")
        } else {
          // All fields appear in at least 10% of rows, so should be in the inferred schema.
          DataType.fromDDL( "struct<a long, b string, " +
                "rareArray array<struct<x long, y long>>, " +
                "rareArray2 array<struct<x long, y long>>, " +
                "rareField string>")
        }
        checkFileSchema(expected, dir)
        checkStringAndSchema(dir, df)
      }
    }
  }

  test("infer shredding does not infer wide schemas") {
    Seq(50, 60, 70).foreach { topLevelFields =>
      // If this changes, we should change the test, or set it explicitly.
      assert(SQLConf.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.defaultValue.get == 300)
      withTempDir { dir =>

        // Each field is a 2-element object. the leaf fields will consume 4 columns, and
        // the value column for the field will consume one, for a total of 5, so we should
        // hit the limit at around 60 columns.
        val bigObject = (0 until topLevelFields).map { i =>
          s""" "col_$i": {"x": $i, "y": "${i + 1}"}  """
        }.mkString(start = "{", sep = ",", end = "}")
        val df = spark.sql(
          // In addition to the large object, add a smaller one. Once we hit the limit, we should
          // not shred that one, becaused it comes later in the schema.
          s"""select parse_json('$bigObject') as v,
                     parse_json('{"x": ' || id || ', "y": 2}') as v2
            from range(0, 100, 1, 1) """)
        df.write.mode("overwrite").parquet(dir.getAbsolutePath)
        val footers = getFooters(dir)
        assert(footers.size == 1)
        // Checking the exact schema is a hassle, so just check that we get the expected number
        // of column chunks.
        val cols = footers(0).getParquetMetadata.getFileMetaData.getSchema.getColumns
        if (topLevelFields == 50) {
          // Columns should be 50 * 5 for the nested fields, plus a top-level value and metadata
          // The second object should shred with 6 columns, using a top-level value and metadata,
          // and a value and typed_value for each of "x" and "y".
          assert(cols.size() == 252 + 6)
        } else {
          // Limit is 300, and metadata does not count towards the limit. Once we hit the limit on
          // the first column, we'll use unshredded for the second, which adds two more columns.
          assert(cols.size() == 301 + 2)
        }
        // Binary should be identical, so we can call checkAnswer directly.
        checkStringAndSchema(dir, df)
      }
    }
  }

  testWithTempDir("infer shredding key as data") { dir =>
      // The first 10 fields in each object include the row ID in the field name, so they'll be
      // unique. Because we impose a 1000-field limit when building up the schema, we'll end up
      // dropping all but the first 1000, so we won't include the non-unique fields in the schema.
      // Since the unique names are below the count threshold, we'll end up with an unshredded
      // schema.
      // In the future, we could consider trying to improve this by dropping the least-common fields
      // when we hit the limit of 1000.
      val bigObject = (0 until 100).map { i =>
        if (i < 50) {
          s""" "first_${i}_' || id || '": {"x": $i, "y": "${i + 1}"}  """
        } else {
          s""" "last_${i}": {"x": $i, "y": "${i + 1}"}  """
        }
      }.mkString(start = "{", sep = ",", end = "}")
      val df = spark.sql(
        // In addition to the large object, add a smaller one. It should be shredded correctly.
        s"""select parse_json('$bigObject') as v,
                   parse_json('{"x": ' || id || ', "y": 2}') as v2
          from range(0, 100, 1, 1) """)
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)
      val footers = getFooters(dir)
      assert(footers.size == 1)

      // We can't call checkFileSchema, because it only handles the case of one Variant column in
      // the file.
      val largeExpected = SparkShreddingUtils.variantShreddingSchema(DataType.fromDDL("variant"))
      val smallExpected = SparkShreddingUtils.variantShreddingSchema(
        DataType.fromDDL("struct<x long, y long>"))
      val actual = getFileSchema(dir)
      assert(actual == StructType(Seq(
              StructField("v", largeExpected, nullable = false),
              StructField("v2", smallExpected, nullable = false))))
      checkStringAndSchema(dir, df)
  }

  testWithTempDir("infer shredding from sparse data") { dir =>
    // Infer a schema when there is only one row per batch.
    // The second case only starts at row 4096 * 2048, but we should still see it when
    // we infer a schema, since there is only one active row per batch.
    val df = spark.sql(
      """
        | select
        |  case when floor(id / (4096 * 2048)) = 0 then
        |    parse_json('{"a": ' || id || ', "c": "' || id || '"}')
        |  else parse_json('{"d": ' || id || ', "b": "' || id || '"}')
        |  end as v
        | from range(0, 4096 * 4096, 1, 1)
        | where id % 4096 = 0
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("struct<a long, b string, c string, d long>")
    checkFileSchema(expected, dir)
    checkStringAndSchema(dir, df)
  }

  testWithTempDir("infer shredding non-null after null") { dir =>
    // When the first batch is less than max batch size, the writer buffers it, and eventually
    // infers a schema based on the buffered data and non-buffered data. Ensure that we behave
    // correctly if either batch is all-null.
    val df = spark.sql(
      """
        | select
        |  case when id >= 4096 then
        |    parse_json('{"a": ' || id || ', "b": "' || id || '"}')
        |  else null
        |  end as v
        | from range(0, 10000, 1, 1)
        | -- Filter out one row per batch so that we buffer the first batch rather than
        | -- immediately finalizing the schema.
        | where id % 4096 != 0
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("struct<a long, b string>")
    checkFileSchema(expected, dir)
    checkStringAndSchema(dir, df)
  }

  testWithTempDir("infer shredding null after non-null") { dir =>
    // Same as the previous test, but the first batch is non-null, and the second is all-null.
    val df = spark.sql(
      """
        | select
        |  case when id < 4096 then
        |    parse_json('{"a": ' || id || ', "b": "' || id || '"}')
        |  else null
        |  end as v
        | from range(0, 10000, 1, 1)
        | -- Filter out one row per batch so that we buffer the first batch rather than
        | -- immediately finalizing the schema.
        | where id % 4096 != 0
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("struct<a long, b string>")
    checkFileSchema(expected, dir)
    checkStringAndSchema(dir, df)
  }

  testWithTempDir("infer shredding with empty file") { dir =>
    // When there is no data, we shoul produce a sane schema.
    val df = spark.sql(
      """
        | select
        |  case when id < 4096 then
        |    parse_json('{"a": ' || id || ', "b": "' || id || '"}')
        |  else null
        |  end as v
        | from range(0, 10000, 1, 1)
        | where cast(id * id as string) = '123'
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("variant")
    checkFileSchema(expected, dir)
    checkStringAndSchema(dir, df)
  }

  testWithTempDir("infer a simple schema when data is null") { dir =>
    // The second case only starts at row 4096, so we won't use it in the shredding schema.
    val df = spark.sql(
      """
        | select
        |  case when floor(id / 4096) = 0 then null
        |  else parse_json('{"a": ' || id || ', "b": "' || id || '"}')
        |  end as v
        | from range(0, 20000, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    checkFileSchema(VariantType, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }

  testWithTempDir("infer a schema when data is mostly null") { dir =>
    // Even if there is only one non-null row, use it to infer a schema.
    val df = spark.sql(
      """
        | select
        |  case when id % 4096 != 123 then null
        |  else parse_json('{"a": ' || id || ', "b": "' || id || '"}')
        |  end as v
        | from range(0, 20000, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("struct<a long, b string>")
    checkFileSchema(expected, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }

  testWithTempDir("infer a schema when there is one row") { dir =>
    // The second case only starts at row 4096, so we won't use it in the shredding schema.
    val df = spark.sql(
      """ select parse_json('{"a": ' || id || ', "b": "' || id || '"}') as v
        | from range(0, 1, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL("struct<a long, b string>")
    checkFileSchema(expected, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }

  testWithTempDir("Nested variant values") { dir =>
    val df = spark.sql(
      """
        | select
        |  struct(
        |    struct(
        |      id,
        |      parse_json('{"a": ' || id || ', "b": "' || id || '"}') as v,
        |      array(parse_json("1"), parse_json("2")) as a
        |    ) as s1,
        |    array(struct(parse_json('{"a": 1, "b": 2}') as v2)) as a1
        |  ) as s
        | from range(0, 3, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    // The struct field that is not in an array should be shredded.
    val variantSchema = DataType.fromDDL("struct<a long, b string>")
    val shreddedSchema = SparkShreddingUtils.variantShreddingSchema(variantSchema)
    // Unshredded variant prints "value" before "metadata". According to the current wording
    // of the spec "The Parquet columns used to store variant metadata and values must be accessed
    // by name, not by position", so this should be okay, but should we do anything to be
    // consistent between the shredded and unshredded versions?
    val unshreddedSchema = StructType(Seq(
      StructField("value", BinaryType, nullable = false),
      StructField("metadata", BinaryType, nullable = false)))
    // Only the nested struct field should be shredded, none of the fields that are in an array.
    val expected = StructType(Seq(StructField("s", StructType(Seq(
      StructField("s1", StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("v", shreddedSchema, nullable = false),
        StructField("a", ArrayType(unshreddedSchema, containsNull = false), nullable = false))),
        nullable = false),
      StructField("a1", ArrayType(StructType(Seq(
        StructField("v2", unshreddedSchema, nullable = false))), containsNull = false),
        nullable = false))),
      nullable = false)))
    val actual = getFileSchema(dir)
    assert(actual == expected)
    // I think the binary should be identical, but right now the reader doesn't support reading
    // a full struct that contains a shredded Variant, so we need to check the individual fields.
    // TODO(cashmand) re-enable once we have support.
    // checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
    checkAnswer(
      spark.read.parquet(dir.getAbsolutePath).selectExpr(
        "s.s1.id", "s.s1.v", "s.s1.a", "s.a1"
      ),
      df.selectExpr(
        "s.s1.id", "s.s1.v", "s.s1.a", "s.a1"
      ).collect())
  }

  test("infer shredding with mixed scale") {
    withTempDir { dir =>
      // a: Test combining large positive/negative integers with decimal to produce a decimal
      // that doesn't fit in precision 18.
      // b: Mix of numeric and string, won't shred.
      // c: Similar to a, different ordering of decimal and integer.
      // d: Test that Long.MinValue is handled correctly, merges appropriately with decimals.
      // e: Test that an integer (127) that fits in int8_t but not Decimal(2, 0) is correctly
      //    merged with an integer that is represented as a Decimal(18, 0)
      val df = spark.sql(
        s"""
          | select
          | case when id % 3 = 0 then
          |   parse_json('{"a": -123456789012345, "b": ' || id || ', "c": 0.1, "d": -123,
          |              "e": 127}')
          | when id % 3 = 1 then
          |   parse_json('{"a": 0.03, "b": 1.23, "c": -0.123456789012345678, "d": 0.1234567890,
          |              "e": 123456789012345678}')
          | when id % 3 = 2 then
          |   parse_json('{"a": -1.10000, "b": "' || id || '", "c": 0.12,
          |               "d": ${Long.MinValue}, "e": 0.123}')
          | end as v
          | from range(0, 3, 1, 1)
          |""".stripMargin)
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)
      // We don't narrow in the presence of trailing zeros, so scale of a should be 5.  We
      // always use a scale of at least 18, to leave room for larger values.
      val expected = DataType.fromDDL(
        "struct<a decimal(38, 5), b variant, c decimal(18, 18), d decimal(38, 10), " +
        "       e decimal(38, 3)>")

      checkFileSchema(expected, dir)
      // We can't call checkStringAndSchema, because the schema_of_variant doesn't match: the
      // first "a" value is reported as a Decimal(1, 0) after shredding. I think the right way
      // to fix this is to change schema_of_variant to return BIGINT for Decimal(N, 0) when N is
      // <= 18. There are a number of related behaviour changes that we probably need for
      // schema_of_variant.
      checkAnswer(
        spark.read.parquet(dir.getAbsolutePath).selectExpr("v::string"),
        df.selectExpr("v::string").collect())

      // Check that the values were actually shredded into typed_value.
      val fullSchema = StructType(Seq(StructField("v",
        SparkShreddingUtils.variantShreddingSchema(expected))))
      val shreddedDf = spark.read.schema(fullSchema).parquet(dir.getAbsolutePath)
      checkAnswer(shreddedDf.selectExpr("v.typed_value.a.value"),
        Seq(Row(null), Row(null), Row(null)))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.c.value"),
        Seq(Row(null), Row(null), Row(null)))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.d.value"),
        Seq(Row(null), Row(null), Row(null)))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.e.value"),
        Seq(Row(null), Row(null), Row(null)))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.a.typed_value"),
        Seq(
          Row(BigDecimal("-123456789012345")),
          Row(BigDecimal("0.03000")),
          Row(BigDecimal("-1.10000"))))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.c.typed_value"),
        Seq(
          Row(BigDecimal("0.100000000000000000")),
          Row(BigDecimal("-0.123456789012345678")),
          Row(BigDecimal("0.120000000000000000"))))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.d.typed_value"),
        Seq(
          Row(BigDecimal("-123.0000000000")),
          Row(BigDecimal("0.1234567890")),
          Row(BigDecimal("-9223372036854775808.0000000000"))))
      checkAnswer(shreddedDf.selectExpr("v.typed_value.e.typed_value"),
        Seq(
          Row(BigDecimal("127.000")),
          Row(BigDecimal("123456789012345678.000")),
          Row(BigDecimal("0.123"))))
    }
  }

  // Test with a few values of maxRecordsPerFile. It is the other situation besides partitioning
  // where we write multiple files within a task. 0 means no limit.
  Seq((0, 100),
      (0, 50000),
      (23, 200),
      (9950, 50000)).foreach { case (maxRecordsPerFile, numRows) =>
    Seq(false, true).foreach { useSort =>
      val sortStr = if (useSort) "sorted" else "clustered"
      testWithTempDir(
          s"infer shredding with partitions: $numRows $sortStr rows, " +
          s"$maxRecordsPerFile per file") { dir =>
        withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> maxRecordsPerFile.toString) {
          val sortClause = if (useSort) "sort by p, v:a::string" else ""
          val df = spark.sql(
            s"""
              | select
              |   id % 5 as p,
              |   case
              |   when id % 5 = 0 then parse_json('{"a": 1, "b": "hello"}')
              |   when id % 5 = 1 then parse_json('{"a": 1.2, "b": "world"}')
              |   else parse_json('{"a": "not a number", "b": "goodbye"}')
              |   end as v
              | from range(0, $numRows, 1, 1)
              | $sortClause
              |""".stripMargin)
          df.write.mode("overwrite").partitionBy("p").parquet(dir.getAbsolutePath)
          // Depending on the data, Spark may be able to infer that the top-level column is
          // non-nullable, so accept either one.
          val possibleSchemas = Seq(
            "struct<a long, b string>",
            "struct<a decimal(18, 1), b: string>",
            "struct<a string, b string>")
            .map(DataType.fromDDL)
            .map(SparkShreddingUtils.variantShreddingSchema(_))
            .map(shreddedType => StructType(Seq(StructField("v", shreddedType, nullable = false))))
          // Each partition is stored in a sub-directory
          dir.listFiles().filter(_.isDirectory).foreach { subdir =>
            // We compute a new shredding schema for every partition. Check that each schema we see
            // is in the list of possibliities.
            val footers = getFooters(subdir)
            footers.foreach { footer =>
              val actual = new ParquetToSparkSchemaConverter()
                .convert(footer.getParquetMetadata.getFileMetaData.getSchema)
              assert(possibleSchemas.contains(actual))
            }
          }
          checkAnswer(spark.read.parquet(dir.getAbsolutePath).selectExpr("p", "v"), df.collect())
        }
      }
    }
  }

  // Spark hits JSON parsing limits at depth 1000. Ensure that we can shred until fairly close
  // to that limit. If we tried to shred the full schema, we'd hit other Spark limits on schema
  // size, but shredding inference should limit the depth at which we shred.
  // At depth 40, we should shred the full schema.
  Seq(40, 900).foreach { depth =>
    test(s"infer shredding on deep schemas - depth=$depth ") {
      withTempDir { dir =>
        val deepArray = "[" * depth + "1" + "]" * depth
        val deepStruct = """{"a": """ * depth + "1" + "}" * depth
        val deepMixed = """[{"a": """ * (depth / 2) + "1" + "}]" * (depth / 2)
        val df = spark.sql(
          s"""select parse_json('$deepArray') as a,
                     parse_json('$deepStruct') as s,
                     parse_json('$deepMixed') as m
            from range(0, 100, 1, 1) """)
        df.write.mode("overwrite").parquet(dir.getAbsolutePath)
        val footers = getFooters(dir)
        assert(footers.size == 1)
        // Checking the exact schema is a hassle, so just check that we get the expected number of
        // column chunks.
        val cols = footers(0).getParquetMetadata.getFileMetaData.getSchema.getColumns.size()
        if (depth == 40) {
          // There are 40 value columns associated with each array/struct level, and a value and
          // typed_value column at the leaf, plus metadata, for a total of 43 * 3 columns.
          assert(cols == 129)
        } else {
          // Each level produces a `value` column, and the max depth is 50, so about 100 across both
          // fields.  Don't be too picky about exactly how deep the inferred schema is; the test is
          // mainly meant to ensure correctness/stability.
          assert(cols > 150 && cols < 160)
        }
        checkStringAndSchema(dir, df, field = "a")
        checkStringAndSchema(dir, df, field = "s")
        checkStringAndSchema(dir, df, field = "m")
      }
    }
  }

  testWithTempDir("non-json types") { dir =>
    // Ensure that we infer a correct schema for types that do not appear in JSON.
    val df = spark.sql(
      """
        | -- Note: field names must be alphabetically ordered, or binary details will differ and
        | -- cause `checkAnswer` to fail.
        | select
        |  to_variant_object(struct(
        |         cast('abc' as binary) as _bin,
        |         cast(id as boolean) as _bool,
        |         date_from_unix_date(id) as _date,
        |         cast(id as float) as _float,
        |         cast(id as timestamp) as _time,
        |         cast(cast(id as timestamp) as timestamp_ntz) as _time_ntz
        |         ))
        |         as v
        | from range(0, 3, 1, 1)
        |""".stripMargin)
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    val expected = DataType.fromDDL(
        "struct<_bin binary, _bool boolean, _date date, _float float, _time timestamp, " +
        "_time_ntz timestamp_ntz>")
    checkFileSchema(expected, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }

  testWithTempDir("uuid") { dir =>
    // There's no way to generate a UUID Variant value in Spark, so we need to do it manually.
    val numRows = 10
    val rdd = spark.sparkContext.parallelize[InternalRow](Nil, numSlices = 1).mapPartitions { _ =>
      val uuid = java.util.UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")
      val builder = new VariantBuilder(false)
      val start = builder.getWritePos
      val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](2)
      val a_id = builder.addKey("a")
      fields.add(new VariantBuilder.FieldEntry("a", a_id, builder.getWritePos - start))
      builder.appendUuid(uuid)
      // Add an extra field to make Variant reconstruction a bit more interesting.
      val b_id = builder.addKey("b")
      fields.add(new VariantBuilder.FieldEntry("b", b_id, builder.getWritePos - start))
      builder.appendLong(123)
      builder.finishWritingObject(start, fields)
      val v = builder.result()
      Iterator.tabulate(numRows) { _ =>
        InternalRow(new VariantVal(v.getValue, v.getMetadata))
      }
    }
    // Ensure that we infer a correct schema for types that do not appear in JSON.
    val writeSchema = DataType.fromDDL("struct<v variant>").asInstanceOf[StructType]
    val df = Dataset.ofRows(spark, LogicalRDD(DataTypeUtils.toAttributes(writeSchema), rdd)(spark))
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    // The field should not be shredded.
    val expected = DataType.fromDDL("struct<a variant, b long>")
    checkFileSchema(expected, dir)
    checkAnswer(spark.read.parquet(dir.getAbsolutePath), df.collect())
  }
}
