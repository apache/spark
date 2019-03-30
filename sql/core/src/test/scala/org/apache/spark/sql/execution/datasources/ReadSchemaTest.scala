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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

/**
 * The reader schema is said to be evolved (or projected) when it changed after the data is
 * written by writers. The followings are supported in file-based data sources.
 * Note that partition columns are not maintained in files. Here, `column` means non-partition
 * column.
 *
 *   1. Add a column
 *   2. Hide a column
 *   3. Change a column position
 *   4. Change a column type (Upcast)
 *
 * Here, we consider safe changes without data loss. For example, data type changes should be
 * from small types to larger types like `int`-to-`long`, not vice versa.
 *
 * So far, file-based data sources have the following coverages.
 *
 *   | File Format  | Coverage     | Note                                                   |
 *   | ------------ | ------------ | ------------------------------------------------------ |
 *   | TEXT         | N/A          | Schema consists of a single string column.             |
 *   | CSV          | 1, 2, 4      |                                                        |
 *   | JSON         | 1, 2, 3, 4   |                                                        |
 *   | ORC          | 1, 2, 3, 4   | Native vectorized ORC reader has the widest coverage.  |
 *   | PARQUET      | 1, 2, 3      |                                                        |
 *   | AVRO         | 1, 2, 3      |                                                        |
 *
 * This aims to provide an explicit test coverage for reader schema change on file-based data
 * sources. Since a file format has its own coverage, we need a test suite for each file-based
 * data source with corresponding supported test case traits.
 *
 * The following is a hierarchy of test traits.
 *
 *   ReadSchemaTest
 *     -> AddColumnTest
 *     -> AddColumnIntoTheMiddleTest
 *     -> HideColumnAtTheEndTest
 *     -> HideColumnInTheMiddleTest
 *     -> ChangePositionTest
 *     -> BooleanTypeTest
 *     -> ToStringTypeTest
 *     -> IntegralTypeTest
 *     -> ToDoubleTypeTest
 *     -> ToDecimalTypeTest
 */

trait ReadSchemaTest extends QueryTest with SQLTestUtils with SharedSQLContext {
  val format: String
  val options: Map[String, String] = Map.empty[String, String]
}

/**
 * Add column (Case 1-1).
 * This test suite assumes that the missing column should be `null`.
 */
trait AddColumnTest extends ReadSchemaTest {
  import testImplicits._

  test("append column at the end") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = Seq("a", "b").toDF("col1")
      val df2 = df1.withColumn("col2", lit("x"))
      val df3 = df2.withColumn("col3", lit("y"))

      val dir1 = s"$path${File.separator}part=one"
      val dir2 = s"$path${File.separator}part=two"
      val dir3 = s"$path${File.separator}part=three"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)
      df3.write.format(format).options(options).save(dir3)

      val df = spark.read
        .schema(df3.schema)
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, Seq(
        Row("a", null, null, "one"),
        Row("b", null, null, "one"),
        Row("a", "x", null, "two"),
        Row("b", "x", null, "two"),
        Row("a", "x", "y", "three"),
        Row("b", "x", "y", "three")))
    }
  }
}

/**
 * Add column into the middle (Case 1-2).
 */
trait AddColumnIntoTheMiddleTest extends ReadSchemaTest {
  import testImplicits._

  test("append column into middle") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = Seq((1, 2, "abc"), (4, 5, "def"), (8, 9, null)).toDF("col1", "col2", "col3")
      val df2 = Seq((10, null, 20, null), (40, "uvw", 50, "xyz"), (80, null, 90, null))
        .toDF("col1", "col4", "col2", "col3")

      val dir1 = s"$path${File.separator}part=one"
      val dir2 = s"$path${File.separator}part=two"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)

      val df = spark.read
        .schema(df2.schema)
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, Seq(
        Row(1, null, 2, "abc", "one"),
        Row(4, null, 5, "def", "one"),
        Row(8, null, 9, null, "one"),
        Row(10, null, 20, null, "two"),
        Row(40, "uvw", 50, "xyz", "two"),
        Row(80, null, 90, null, "two")))
    }
  }
}

/**
 * Hide column (Case 2-1).
 */
trait HideColumnAtTheEndTest extends ReadSchemaTest {
  import testImplicits._

  test("hide column at the end") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = Seq(("1", "a"), ("2", "b")).toDF("col1", "col2")
      val df2 = df1.withColumn("col3", lit("y"))

      val dir1 = s"$path${File.separator}part=two"
      val dir2 = s"$path${File.separator}part=three"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)

      val df = spark.read
        .schema(df1.schema)
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, Seq(
        Row("1", "a", "two"),
        Row("2", "b", "two"),
        Row("1", "a", "three"),
        Row("2", "b", "three")))

      val df3 = spark.read
        .schema("col1 string")
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df3, Seq(
        Row("1", "two"),
        Row("2", "two"),
        Row("1", "three"),
        Row("2", "three")))
    }
  }
}

/**
 * Hide column in the middle (Case 2-2).
 */
trait HideColumnInTheMiddleTest extends ReadSchemaTest {
  import testImplicits._

  test("hide column in the middle") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = Seq(("1", "a"), ("2", "b")).toDF("col1", "col2")
      val df2 = df1.withColumn("col3", lit("y"))

      val dir1 = s"$path${File.separator}part=two"
      val dir2 = s"$path${File.separator}part=three"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)

      val df = spark.read
        .schema("col2 string")
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, Seq(
        Row("a", "two"),
        Row("b", "two"),
        Row("a", "three"),
        Row("b", "three")))
    }
  }
}

/**
 * Change column positions (Case 3).
 * This suite assumes that all data set have the same number of columns.
 */
trait ChangePositionTest extends ReadSchemaTest {
  import testImplicits._

  test("change column position") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = Seq(("1", "a"), ("2", "b"), ("3", "c")).toDF("col1", "col2")
      val df2 = Seq(("d", "4"), ("e", "5"), ("f", "6")).toDF("col2", "col1")
      val unionDF = df1.unionByName(df2)

      val dir1 = s"$path${File.separator}part=one"
      val dir2 = s"$path${File.separator}part=two"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)

      val df = spark.read
        .schema(unionDF.schema)
        .format(format)
        .options(options)
        .load(path)
        .select("col1", "col2")

      checkAnswer(df, unionDF)
    }
  }
}

/**
 * Change a column type (Case 4).
 * This suite assumes that a user gives a wider schema intentionally.
 */
trait BooleanTypeTest extends ReadSchemaTest {
  import testImplicits._

  test("change column type from boolean to byte/short/int/long") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val values = (1 to 10).map(_ % 2)
      val booleanDF = (1 to 10).map(_ % 2 == 1).toDF("col1")
      val byteDF = values.map(_.toByte).toDF("col1")
      val shortDF = values.map(_.toShort).toDF("col1")
      val intDF = values.toDF("col1")
      val longDF = values.map(_.toLong).toDF("col1")

      booleanDF.write.mode("overwrite").format(format).options(options).save(path)

      Seq(
        ("col1 byte", byteDF),
        ("col1 short", shortDF),
        ("col1 int", intDF),
        ("col1 long", longDF)).foreach { case (schema, answerDF) =>
        checkAnswer(spark.read.schema(schema).format(format).options(options).load(path), answerDF)
      }
    }
  }
}

/**
 * Change a column type (Case 4).
 * This suite assumes that a user gives a wider schema intentionally.
 */
trait ToStringTypeTest extends ReadSchemaTest {
  import testImplicits._

  test("read as string") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val byteDF = (Byte.MaxValue - 2 to Byte.MaxValue).map(_.toByte).toDF("col1")
      val shortDF = (Short.MaxValue - 2 to Short.MaxValue).map(_.toShort).toDF("col1")
      val intDF = (Int.MaxValue - 2 to Int.MaxValue).toDF("col1")
      val longDF = (Long.MaxValue - 2 to Long.MaxValue).toDF("col1")
      val unionDF = byteDF.union(shortDF).union(intDF).union(longDF)
        .selectExpr("cast(col1 AS STRING) col1")

      val byteDir = s"$path${File.separator}part=byte"
      val shortDir = s"$path${File.separator}part=short"
      val intDir = s"$path${File.separator}part=int"
      val longDir = s"$path${File.separator}part=long"

      byteDF.write.format(format).options(options).save(byteDir)
      shortDF.write.format(format).options(options).save(shortDir)
      intDF.write.format(format).options(options).save(intDir)
      longDF.write.format(format).options(options).save(longDir)

      val df = spark.read
        .schema("col1 string")
        .format(format)
        .options(options)
        .load(path)
        .select("col1")

      checkAnswer(df, unionDF)
    }
  }
}

/**
 * Change a column type (Case 4).
 * This suite assumes that a user gives a wider schema intentionally.
 */
trait IntegralTypeTest extends ReadSchemaTest {

  import testImplicits._

  private lazy val values = 1 to 10
  private lazy val byteDF = values.map(_.toByte).toDF("col1")
  private lazy val shortDF = values.map(_.toShort).toDF("col1")
  private lazy val intDF = values.toDF("col1")
  private lazy val longDF = values.map(_.toLong).toDF("col1")

  test("change column type from byte to short/int/long") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      byteDF.write.format(format).options(options).save(path)

      Seq(
        ("col1 short", shortDF),
        ("col1 int", intDF),
        ("col1 long", longDF)).foreach { case (schema, answerDF) =>
        checkAnswer(spark.read.schema(schema).format(format).options(options).load(path), answerDF)
      }
    }
  }

  test("change column type from short to int/long") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      shortDF.write.format(format).options(options).save(path)

      Seq(("col1 int", intDF), ("col1 long", longDF)).foreach { case (schema, answerDF) =>
        checkAnswer(spark.read.schema(schema).format(format).options(options).load(path), answerDF)
      }
    }
  }

  test("change column type from int to long") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      intDF.write.format(format).options(options).save(path)

      Seq(("col1 long", longDF)).foreach { case (schema, answerDF) =>
        checkAnswer(spark.read.schema(schema).format(format).options(options).load(path), answerDF)
      }
    }
  }

  test("read byte, int, short, long together") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val byteDF = (Byte.MaxValue - 2 to Byte.MaxValue).map(_.toByte).toDF("col1")
      val shortDF = (Short.MaxValue - 2 to Short.MaxValue).map(_.toShort).toDF("col1")
      val intDF = (Int.MaxValue - 2 to Int.MaxValue).toDF("col1")
      val longDF = (Long.MaxValue - 2 to Long.MaxValue).toDF("col1")
      val unionDF = byteDF.union(shortDF).union(intDF).union(longDF)

      val byteDir = s"$path${File.separator}part=byte"
      val shortDir = s"$path${File.separator}part=short"
      val intDir = s"$path${File.separator}part=int"
      val longDir = s"$path${File.separator}part=long"

      byteDF.write.format(format).options(options).save(byteDir)
      shortDF.write.format(format).options(options).save(shortDir)
      intDF.write.format(format).options(options).save(intDir)
      longDF.write.format(format).options(options).save(longDir)

      val df = spark.read
        .schema(unionDF.schema)
        .format(format)
        .options(options)
        .load(path)
        .select("col1")

      checkAnswer(df, unionDF)
    }
  }
}

/**
 * Change a column type (Case 4).
 * This suite assumes that a user gives a wider schema intentionally.
 */
trait ToDoubleTypeTest extends ReadSchemaTest {
  import testImplicits._

  private lazy val values = 1 to 10
  private lazy val floatDF = values.map(_.toFloat).toDF("col1")
  private lazy val doubleDF = values.map(_.toDouble).toDF("col1")
  private lazy val unionDF = floatDF.union(doubleDF)

  test("change column type from float to double") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      floatDF.write.format(format).options(options).save(path)

      val df = spark.read.schema("col1 double").format(format).options(options).load(path)

      checkAnswer(df, doubleDF)
    }
  }

  test("read float and double together") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val floatDir = s"$path${File.separator}part=float"
      val doubleDir = s"$path${File.separator}part=double"

      floatDF.write.format(format).options(options).save(floatDir)
      doubleDF.write.format(format).options(options).save(doubleDir)

      val df = spark.read
        .schema(unionDF.schema)
        .format(format)
        .options(options)
        .load(path)
        .select("col1")

      checkAnswer(df, unionDF)
    }
  }
}

/**
 * Change a column type (Case 4).
 * This suite assumes that a user gives a wider schema intentionally.
 */
trait ToDecimalTypeTest extends ReadSchemaTest {
  import testImplicits._

  private lazy val values = 1 to 10
  private lazy val floatDF = values.map(_.toFloat).toDF("col1")
  private lazy val doubleDF = values.map(_.toDouble).toDF("col1")
  private lazy val decimalDF = values.map(BigDecimal(_)).toDF("col1")
  private lazy val unionDF = floatDF.union(doubleDF).union(decimalDF)

  test("change column type from float to decimal") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      floatDF.write.format(format).options(options).save(path)

      val df = spark.read
        .schema("col1 decimal(38,18)")
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, decimalDF)
    }
  }

  test("change column type from double to decimal") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      doubleDF.write.format(format).options(options).save(path)

      val df = spark.read
        .schema("col1 decimal(38,18)")
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, decimalDF)
    }
  }

  test("read float, double, decimal together") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val floatDir = s"$path${File.separator}part=float"
      val doubleDir = s"$path${File.separator}part=double"
      val decimalDir = s"$path${File.separator}part=decimal"

      floatDF.write.format(format).options(options).save(floatDir)
      doubleDF.write.format(format).options(options).save(doubleDir)
      decimalDF.write.format(format).options(options).save(decimalDir)

      val df = spark.read
        .schema(unionDF.schema)
        .format(format)
        .options(options)
        .load(path)
        .select("col1")

      checkAnswer(df, unionDF)
    }
  }
}
