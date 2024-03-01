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

  test("checkCollation throws exception for incompatible collationIds") {
    val left: String = "abc" // collate with 'UNICODE_CI'
    val leftCollationName: String = "UNICODE_CI";
    var right: String = null // collate with 'UNICODE'
    val rightCollationName: String = "UNICODE";
    // contains
    right = left.substring(1, 2);
    checkError(
      exception = intercept[SparkException] {
        spark.sql(s"SELECT contains(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))").collect()
      },
      errorClass = "COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"$leftCollationName",
        "collationNameRight" -> s"$rightCollationName"
      )
    )
    // startsWith
    right = left.substring(0, 1);
    checkError(
      exception = intercept[SparkException] {
        spark.sql(s"SELECT startsWith(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))").collect()
      },
      errorClass = "COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"$leftCollationName",
        "collationNameRight" -> s"$rightCollationName"
      )
    )
    // endsWith
    right = left.substring(2, 3);
    checkError(
      exception = intercept[SparkException] {
        spark.sql(s"SELECT endsWith(collate('$left', '$leftCollationName')," +
          s"collate('$right', '$rightCollationName'))").collect()
      },
      errorClass = "COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"$leftCollationName",
        "collationNameRight" -> s"$rightCollationName"
      )
    )
  }

  test("Support contains string expression with Collation") {
    // Test 'contains' with different collations
    var listLeft: List[String] = List()
    var listRight: List[String] = List()
    var listResult: List[Boolean] = List()

    // UCS_BASIC (default) & UNICODE collation
    listLeft = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listRight = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, false, false, false, false, false,  //   c
      true, true, true, false, false, false, false, false, false, false, false,   // abc
      true, true, false, true, false, false, false, false, false, false, false,   //   cde
      true, false, false, false, true, false, false, false, false, false, false,  // abde
      true, true, true, true, false, true, false, false, false, false, false,     // abcde
      true, false, false, false, false, false, true, false, false, false, false,  //   C
      true, false, false, false, false, false, true, true, false, false, false,   // ABC
      true, false, false, false, false, false, true, false, true, false, false,   //   CDE
      true, false, false, false, false, false, false, false, false, true, false,  // ABDE
      true, false, false, false, false, false, true, true, true, false, true)     // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC (default)
      checkAnswer(sql("SELECT contains('" + left + "', '" + right + "')"), Row(expectedAnswer))
      // UCS_BASIC
      checkAnswer(sql("SELECT contains('" + left + "', collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      checkAnswer(sql("SELECT contains(collate('" + left + "', 'UCS_BASIC'), collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      // UNICODE
      checkAnswer(sql("SELECT contains(collate('" + left + "', 'UNICODE'), collate('" +
        right + "', 'UNICODE'))"), Row(expectedAnswer))
    }

    // UCS_BASIC_LCASE & UNICODE_CI collation
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, true, false, false, false, false,   //   c
      true, true, true, false, false, false, true, true, false, false, false,     // abc
      true, true, false, true, false, false, true, false, true, false, false,     //    cde
      true, false, false, false, true, false, false, false, false, true, false,   // abde
      true, true, true, true, false, true, true, true, true, false, true,         // abcde
      true, true, false, false, false, false, true, false, false, false, false,   //   C
      true, true, true, false, false, false, true, true, false, false, false,     // ABC
      true, true, false, true, false, false, true, false, true, false, false,     //   CDE
      true, false, false, false, true, false, false, false, false, true, false,   // ABDE
      true, true, true, true, false, true, true, true, true, false, true)         // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC_LCASE
      checkAnswer(sql("SELECT contains(collate('" + left + "', 'UCS_BASIC_LCASE'), collate('" +
        right + "', 'UCS_BASIC_LCASE'))"), Row(expectedAnswer))
      // UNICODE_CI
      checkError(
        exception = intercept[SparkException] {
          spark.sql(s"SELECT contains(collate('$left', 'UNICODE_CI')," +
            s"collate('$right', 'UNICODE_CI'))").collect()
        },
        errorClass = "COLLATION_NOT_SUPPORTED_FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "contains",
          "collationName" -> "UNICODE_CI"
        )
      )
    }
  }

  test("Support startsWith string expression with Collation") {
    // Test 'startsWith' with different collations
    var listLeft: List[String] = List()
    var listRight: List[String] = List()
    var listResult: List[Boolean] = List()

    // UCS_BASIC (default) & UNICODE collation
    listLeft = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listRight = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, false, false, false, false, false,  //   c
      true, false, true, false, false, false, false, false, false, false, false,  // abc
      true, true, false, true, false, false, false, false, false, false, false,   //   cde
      true, false, false, false, true, false, false, false, false, false, false,  // abde
      true, false, true, false, false, true, false, false, false, false, false,   // abcde
      true, false, false, false, false, false, true, false, false, false, false,  //   C
      true, false, false, false, false, false, false, true, false, false, false,  // ABC
      true, false, false, false, false, false, true, false, true, false, false,   //   CDE
      true, false, false, false, false, false, false, false, false, true, false,  // ABDE
      true, false, false, false, false, false, false, true, false, false, true)   // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC (default)
      checkAnswer(sql("SELECT startswith('" + left + "', '" + right + "')"), Row(expectedAnswer))
      // UCS_BASIC
      checkAnswer(sql("SELECT startswith('" + left + "', collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      checkAnswer(sql("SELECT startswith(collate('" + left + "', 'UCS_BASIC'), collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      // UNICODE
      checkAnswer(sql("SELECT startswith(collate('" + left + "', 'UNICODE'), collate('" +
        right + "', 'UNICODE'))"), Row(expectedAnswer))
    }

    // UCS_BASIC_LCASE & UNICODE_CI collation
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, true, false, false, false, false,   //   c
      true, false, true, false, false, false, false, true, false, false, false,   // abc
      true, true, false, true, false, false, true, false, true, false, false,     //   cde
      true, false, false, false, true, false, false, false, false, true, false,   // abde
      true, false, true, false, false, true, false, true, false, false, true,     // abcde
      true, true, false, false, false, false, true, false, false, false, false,   //   C
      true, false, true, false, false, false, false, true, false, false, false,   // ABC
      true, true, false, true, false, false, true, false, true, false, false,     //   CDE
      true, false, false, false, true, false, false, false, false, true, false,   // ABDE
      true, false, true, false, false, true, false, true, false, false, true)     // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC_LCASE
      checkAnswer(sql("SELECT startswith(collate('" + left + "', 'UCS_BASIC_LCASE'), collate('" +
        right + "', 'UCS_BASIC_LCASE'))"), Row(expectedAnswer))
      // UNICODE_CI
      checkAnswer(sql("SELECT startswith(collate('" + left + "', 'UNICODE_CI'), collate('" +
        right + "', 'UNICODE_CI'))"), Row(expectedAnswer))
    }
  }

  test("Support endsWith string expression with Collation") {
    // Test 'endsWith' with different collations
    var listLeft: List[String] = List()
    var listRight: List[String] = List()
    var listResult: List[Boolean] = List()

    // UCS_BASIC (default) & UNICODE collation
    listLeft = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listRight = List("", "c", "abc", "cde", "abde", "abcde", "C", "ABC", "CDE", "ABDE", "ABCDE")
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, false, false, false, false, false,  //   c
      true, true, true, false, false, false, false, false, false, false, false,   // abc
      true, false, false, true, false, false, false, false, false, false, false,  //   cde
      true, false, false, false, true, false, false, false, false, false, false,  // abde
      true, false, false, true, false, true, false, false, false, false, false,   // abcde
      true, false, false, false, false, false, true, false, false, false, false,  //   C
      true, false, false, false, false, false, true, true, false, false, false,   // ABC
      true, false, false, false, false, false, false, false, true, false, false,  //   CDE
      true, false, false, false, false, false, false, false, false, true, false,  // ABDE
      true, false, false, false, false, false, false, false, true, false, true)   // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC (default)
      checkAnswer(sql("SELECT endswith('" + left + "', '" + right + "')"), Row(expectedAnswer))
      // UCS_BASIC
      checkAnswer(sql("SELECT endswith('" + left + "', collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      checkAnswer(sql("SELECT endswith(collate('" + left + "', 'UCS_BASIC'), collate('" +
        right + "', 'UCS_BASIC'))"), Row(expectedAnswer))
      // UNICODE
      checkAnswer(sql("SELECT endswith(collate('" + left + "', 'UNICODE'), collate('" +
        right + "', 'UNICODE'))"), Row(expectedAnswer))
    }

    // UCS_BASIC_LCASE & UNICODE_CI collation
    listResult = List(
    //  ""     c     abc    cde   abde  abcde    C     ABC    CDE    ABDE  ABCDE
      true, false, false, false, false, false, false, false, false, false, false, //  ""
      true, true, false, false, false, false, true, false, false, false, false,   //   c
      true, true, true, false, false, false, true, true, false, false, false,     // abc
      true, false, false, true, false, false, false, false, true, false, false,   //   cde
      true, false, false, false, true, false, false, false, false, true, false,   // abde
      true, false, false, true, false, true, false, false, true, false, true,     // abcde
      true, true, false, false, false, false, true, false, false, false, false,   //   C
      true, true, true, false, false, false, true, true, false, false, false,     // ABC
      true, false, false, true, false, false, false, false, true, false, false,   //   CDE
      true, false, false, false, true, false, false, false, false, true, false,   // ABDE
      true, false, false, true, false, true, false, false, true, false, true)     // ABCDE
    for {
      (left, index_left) <- listLeft.zipWithIndex
      (right, index_right) <- listRight.zipWithIndex
    } {
      val expectedAnswer = listResult(index_left * listRight.length + index_right)
      // UCS_BASIC_LCASE
      checkAnswer(sql("SELECT endswith(collate('" + left + "', 'UCS_BASIC_LCASE'), collate('" +
        right + "', 'UCS_BASIC_LCASE'))"), Row(expectedAnswer))
      // UNICODE_CI
      checkAnswer(sql("SELECT endswith(collate('" + left + "', 'UNICODE_CI'), collate('" +
        right + "', 'UNICODE_CI'))"), Row(expectedAnswer))

  test("disable partition on collated string column") {
    def createTable(partitionColumns: String*): Unit = {
      val tableName = "test_partition_tbl"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $tableName
             |(id INT, c1 STRING COLLATE 'UNICODE', c2 string)
             |USING parquet
             |PARTITIONED BY (${partitionColumns.mkString(",")})
             |""".stripMargin)
      }
    }

    // should work fine on non collated columns
    createTable("id")
    createTable("c2")
    createTable("id", "c2")

    Seq(Seq("c1"), Seq("c1", "id"), Seq("c1", "c2")).foreach { partitionColumns =>
      checkError(
        exception = intercept[AnalysisException] {
          createTable(partitionColumns: _*)
        },
        errorClass = "INVALID_PARTITION_COLUMN_DATA_TYPE",
        parameters = Map("type" -> "\"STRING COLLATE 'UNICODE'\"")
      );

    }
  }
}
