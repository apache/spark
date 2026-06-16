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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JdbcUtilsSuite extends SparkFunSuite {

  val tableSchema = StructType(Seq(
    StructField("C1", StringType, false), StructField("C2", IntegerType, false)))
  val caseSensitive = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
  val caseInsensitive = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  test("Parse user specified column types") {
    assert(JdbcUtils.getCustomSchema(tableSchema, null, caseInsensitive) === tableSchema)
    assert(JdbcUtils.getCustomSchema(tableSchema, "", caseInsensitive) === tableSchema)

    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", IntegerType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", IntegerType, false))))

    assert(
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", StringType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", StringType, false))))

    // Throw AnalysisException
    val duplicate = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, c1 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, false), StructField("c1", StringType, false)))
    }
    checkError(
      exception = duplicate,
      condition = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`c1`"))

    // Throw ParseException
    checkError(
      exception = intercept[ParseException]{
        JdbcUtils.getCustomSchema(tableSchema, "c3 DATEE, C2 STRING", caseInsensitive)
      },
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"DATEE\""))

    checkError(
      exception = intercept[ParseException]{
        JdbcUtils.getCustomSchema(tableSchema, "c3 DATE. C2 STRING", caseInsensitive)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ""))
  }

  test("redactUrl keeps only the jdbc:<subprotocol>: prefix") {
    val redaction = Utils.REDACTION_REPLACEMENT_TEXT

    // Only the "jdbc:<subprotocol>:" prefix is kept; everything after it (host, port, database,
    // userinfo and connection properties) is redacted, regardless of the driver-specific syntax.
    // This covers credentials embedded as a "//user:pwd@host" authority, ...
    assert(JDBCOptions.redactUrl("jdbc:mysql://user:secret@host:3306/db", None) ===
      s"jdbc:mysql:$redaction")
    assert(JDBCOptions.redactUrl("jdbc:mysql://user:p@ss@host:3306/db?password=other", None) ===
      s"jdbc:mysql:$redaction")
    // ... as Oracle Thin's "user/pwd@host" form (no "//" authority), ...
    assert(JDBCOptions.redactUrl("jdbc:oracle:thin:scott/tiger@host:1521/svc", None) ===
      s"jdbc:oracle:$redaction")
    assert(JDBCOptions.redactUrl("jdbc:oracle:thin:scott/tiger@//host:1521/svc?x=1", None) ===
      s"jdbc:oracle:$redaction")
    // ... and as "?"- or ";"-delimited connection properties.
    assert(JDBCOptions.redactUrl(
      "jdbc:postgresql://host/db?user=alice&password=secret", None) ===
      s"jdbc:postgresql:$redaction")
    assert(JDBCOptions.redactUrl(
      "jdbc:sqlserver://localhost:1433;databaseName=testdb;password=secret", None) ===
      s"jdbc:sqlserver:$redaction")

    // Even URLs that carry no credentials are reduced to the prefix -- nothing past the
    // subprotocol is assumed safe.
    assert(JDBCOptions.redactUrl("jdbc:mysql://localhost/db", None) === s"jdbc:mysql:$redaction")
    assert(JDBCOptions.redactUrl("jdbc:h2:mem:testdb", None) === s"jdbc:h2:$redaction")

    // A URL with no subname delimiter (no second colon) is redacted wholesale.
    assert(JDBCOptions.redactUrl("jdbc:weird-url", None) === redaction)

    // The user-configured regex is still applied on top of the kept prefix.
    assert(JDBCOptions.redactUrl("jdbc:mysql://host/db", Some("mysql".r)) ===
      s"jdbc:$redaction:$redaction")

    // Null and empty inputs are passed through.
    assert(JDBCOptions.redactUrl(null, None) === null)
    assert(JDBCOptions.redactUrl("", None) === "")
  }

  test("SPARK-57471: estimateInternalRowSize estimates ArrayType by element count") {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = new GenericInternalRow(Array[Any](new GenericArrayData(Array(1, 2, 3))))
    // 3 elements * 4 bytes (IntegerType.defaultSize) = 12
    assert(JdbcUtils.estimateInternalRowSize(row, schema) === 12L)
  }

  test("SPARK-57471: estimateRowSize estimates ArrayType by element count") {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = Row(Seq(1, 2, 3))
    // 3 elements * 4 bytes (IntegerType.defaultSize) = 12
    assert(JdbcUtils.estimateRowSize(row, schema) === 12L)
  }

  test("SPARK-57471: estimateInternalRowSize uses actual size for String and Binary") {
    import org.apache.spark.unsafe.types.UTF8String
    val schema = StructType(Seq(
      StructField("s", StringType),
      StructField("b", BinaryType),
      StructField("n", StringType)))
    val row = new GenericInternalRow(Array[Any](
      UTF8String.fromString("hello"), // 5 bytes
      Array[Byte](1, 2, 3),          // 3 bytes
      null))                          // null -> 0
    assert(JdbcUtils.estimateInternalRowSize(row, schema) === 8L)
  }

  test("SPARK-57471: estimateRowSize uses actual size for String and Binary") {
    val schema = StructType(Seq(
      StructField("s", StringType),
      StructField("b", BinaryType),
      StructField("n", StringType)))
    val row = Row("hello", Array[Byte](1, 2, 3), null)
    // "hello".length=5 + 3 bytes + null=0
    assert(JdbcUtils.estimateRowSize(row, schema) === 8L)
  }

  test("SPARK-57471: estimateInternalRowSize measures ArrayType(StringType) by actual content") {
    import org.apache.spark.unsafe.types.UTF8String
    val schema = StructType(Seq(StructField("a", ArrayType(StringType))))
    val arr = new GenericArrayData(
      Array(UTF8String.fromString("abc"), UTF8String.fromString("de")))
    val row = new GenericInternalRow(Array[Any](arr))
    // "abc"=3 + "de"=2 = 5, NOT 2*20
    assert(JdbcUtils.estimateInternalRowSize(row, schema) === 5L)
  }

  test("SPARK-57471: estimateRowSize measures ArrayType(StringType) by actual content") {
    val schema = StructType(Seq(StructField("a", ArrayType(StringType))))
    val row = Row(Seq("abc", "de"))
    // "abc".length=3 + "de".length=2 = 5, NOT 2*20
    assert(JdbcUtils.estimateRowSize(row, schema) === 5L)
  }

  test("SPARK-57471: estimateInternalRowSize ArrayType(BinaryType) uses actual lengths") {
    val schema = StructType(Seq(StructField("a", ArrayType(BinaryType))))
    val arr = new GenericArrayData(Array(Array[Byte](1, 2), null, Array[Byte](3, 4, 5)))
    val row = new GenericInternalRow(Array[Any](arr))
    // 2 + 0 (null) + 3 = 5
    assert(JdbcUtils.estimateInternalRowSize(row, schema) === 5L)
  }

  test("SPARK-57471: estimateRowSize ArrayType(BinaryType) uses actual lengths") {
    val schema = StructType(Seq(StructField("a", ArrayType(BinaryType))))
    val row = Row(Seq(Array[Byte](1, 2), null, Array[Byte](3, 4, 5)))
    // 2 + 0 (null) + 3 = 5
    assert(JdbcUtils.estimateRowSize(row, schema) === 5L)
  }
}
