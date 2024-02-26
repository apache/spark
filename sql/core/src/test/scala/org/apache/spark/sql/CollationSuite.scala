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

package org.apache.spark.sql

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.MapHasAsJava

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.{DatasourceV2SQLBase, FakeV2ProviderWithCustomSchema}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.types.StringType

class CollationSuite extends DatasourceV2SQLBase {
  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName

  test("collate returns proper type") {
    Seq("ucs_basic", "ucs_basic_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate '$collationName'"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate '$collationName'").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation name is case insensitive") {
    Seq("uCs_BasIc", "uCs_baSic_Lcase", "uNicOde", "UNICODE_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate '$collationName'"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate '$collationName'").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation expression returns name of collation") {
    Seq("ucs_basic", "ucs_basic_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(
        sql(s"select collation('aaa' collate '$collationName')"), Row(collationName.toUpperCase()))
    }
  }

  test("collate function syntax") {
    assert(sql(s"select collate('aaa', 'ucs_basic')").schema(0).dataType == StringType(0))
    assert(sql(s"select collate('aaa', 'ucs_basic_lcase')").schema(0).dataType == StringType(1))
  }

  test("collate function syntax invalid arg count") {
    Seq("'aaa','a','b'", "'aaa'", "", "'aaa'").foreach(args => {
      val paramCount = if (args == "") 0 else args.split(',').length.toString
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select collate($args)")
        },
        errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        sqlState = "42605",
        parameters = Map(
          "functionName" -> "`collate`",
          "expectedNum" -> "2",
          "actualNum" -> paramCount.toString,
          "docroot" -> "https://spark.apache.org/docs/latest"),
        context = ExpectedContext(fragment = s"collate($args)", start = 7, stop = 15 + args.length)
      )
    })
  }

  test("collate function invalid collation data type") {
    checkError(
      exception = intercept[AnalysisException](sql("select collate('abc', 123)")),
      errorClass = "UNEXPECTED_INPUT_TYPE",
      sqlState = "42K09",
      Map(
        "functionName" -> "`collate`",
        "paramIndex" -> "first",
        "inputSql" -> "\"123\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRING\""),
      context = ExpectedContext(fragment = s"collate('abc', 123)", start = 7, stop = 25)
    )
  }

  test("NULL as collation name") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("select collate('abc', cast(null as string))") },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      sqlState = "42K09",
      Map("exprName" -> "`collation`", "sqlExpr" -> "\"CAST(NULL AS STRING)\""),
      context = ExpectedContext(
        fragment = s"collate('abc', cast(null as string))", start = 7, stop = 42)
    )
  }

  test("collate function invalid input data type") {
    checkError(
      exception = intercept[ExtendedAnalysisException] { sql(s"select collate(1, 'UCS_BASIC')") },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      sqlState = "42K09",
      parameters = Map(
        "sqlExpr" -> "\"collate(1)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"1\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRING\""),
      context = ExpectedContext(
        fragment = s"collate(1, 'UCS_BASIC')", start = 7, stop = 29))
  }

  test("collation expression returns default collation") {
    checkAnswer(sql(s"select collation('aaa')"), Row("UCS_BASIC"))
  }

  test("invalid collation name throws exception") {
    checkError(
      exception = intercept[SparkException] { sql("select 'aaa' collate 'UCS_BASIS'") },
      errorClass = "COLLATION_INVALID_NAME",
      sqlState = "42704",
      parameters = Map("proposal" -> "UCS_BASIC", "collationName" -> "UCS_BASIS"))
  }

  test("equality check respects collation") {
    Seq(
      ("ucs_basic", "aaa", "AAA", false),
      ("ucs_basic", "aaa", "aaa", true),
      ("ucs_basic_lcase", "aaa", "aaa", true),
      ("ucs_basic_lcase", "aaa", "AAA", true),
      ("ucs_basic_lcase", "aaa", "bbb", false),
      ("unicode", "aaa", "aaa", true),
      ("unicode", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "aaa", true),
      ("unicode_CI", "aaa", "AAA", true),
      ("unicode_CI", "aaa", "bbb", false)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate '$collationName' = '$right' collate '$collationName'"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') = collate('$right', '$collationName')"),
          Row(expected))
    }
  }

  test("comparisons respect collation") {
    Seq(
      ("ucs_basic", "AAA", "aaa", true),
      ("ucs_basic", "aaa", "aaa", false),
      ("ucs_basic", "aaa", "BBB", false),
      ("ucs_basic_lcase", "aaa", "aaa", false),
      ("ucs_basic_lcase", "AAA", "aaa", false),
      ("ucs_basic_lcase", "aaa", "bbb", true),
      ("unicode", "aaa", "aaa", false),
      ("unicode", "aaa", "AAA", true),
      ("unicode", "aaa", "BBB", true),
      ("unicode_CI", "aaa", "aaa", false),
      ("unicode_CI", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "bbb", true)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate '$collationName' < '$right' collate '$collationName'"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') < collate('$right', '$collationName')"),
          Row(expected))
    }
  }

  test("create table with collation") {
    val tableName = "parquet_dummy_tbl"
    val collationName = "UCS_BASIC_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (c1 STRING COLLATE '$collationName')
           |USING PARQUET
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"), Seq(Row(collationName)))
      assert(sql(s"select c1 FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }

  test("create table with collations inside a struct") {
    val tableName = "struct_collation_tbl"
    val collationName = "UCS_BASIC_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName
           |(c1 STRUCT<name: STRING COLLATE '$collationName', age: INT>)
           |USING PARQUET
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES (named_struct('name', 'aaa', 'id', 1))")
      sql(s"INSERT INTO $tableName VALUES (named_struct('name', 'AAA', 'id', 2))")

      checkAnswer(sql(s"SELECT DISTINCT collation(c1.name) FROM $tableName"),
        Seq(Row(collationName)))
      assert(sql(s"SELECT c1.name FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }

  test("add collated column with alter table") {
    val tableName = "alter_column_tbl"
    val defaultCollation = "UCS_BASIC"
    val collationName = "UCS_BASIC_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (c1 STRING)
           |USING PARQUET
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"),
          Seq(Row(defaultCollation)))

      sql(
      s"""
         |ALTER TABLE $tableName
         |ADD COLUMN c2 STRING COLLATE '$collationName'
         |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('aaa', 'aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA', 'AAA')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $tableName"),
        Seq(Row(collationName)))
      assert(sql(s"select c2 FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }

  test("create v2 table with collation column") {
    val tableName = "testcat.table_name"
    val collationName = "UCS_BASIC_LCASE"
    val collationId = CollationFactory.collationNameToId(collationName)

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (c1 string COLLATE '$collationName')
           |USING $v2Source
           |""".stripMargin)

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

      assert(table.name == tableName)
      assert(table.partitioning.isEmpty)
      assert(table.properties == withDefaultOwnership(Map("provider" -> v2Source)).asJava)
      assert(table.columns().head.dataType() == StringType(collationId))

      val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
      checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)

      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")

      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"),
        Seq(Row(collationName)))
      assert(sql(s"select c1 FROM $tableName").schema.head.dataType == StringType(collationId))
    }
  }
}
