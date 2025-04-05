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

import org.apache.spark.{SparkConf, SparkRuntimeException}
import org.apache.spark.sql.connector.catalog.{Column => ColumnV2, Identifier, InMemoryTableCatalog, TableInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StructType}

class RuntimeNullChecksV2Writes extends QueryTest with SQLTestUtils with SharedSparkSession {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private final val FORMAT = "foo"
  private final val CATALOG_NAME = "testcat"

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog." + CATALOG_NAME, classOf[InMemoryTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, CATALOG_NAME)
  }

  protected def catalog: InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog(CATALOG_NAME)
    catalog.asTableCatalog.asInstanceOf[InMemoryTableCatalog]
  }

  test("NOT NULL checks for atomic top-level fields (byName)") {
    checkNotNullTopLevelFields(byName = true)
  }

  test("NOT NULL checks for atomic top-level fields (byPosition)") {
    checkNotNullTopLevelFields(byName = false)
  }

  private def checkNotNullTopLevelFields(byName: Boolean): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (s STRING, i INT NOT NULL) USING $FORMAT")

      val e = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql("SELECT 'txt' AS s, null AS i")
          inputDF.writeTo("t").append()
        } else {
          sql("INSERT INTO t VALUES ('txt', null)")
        }
      }
      assert(e.getCondition == "NOT_NULL_ASSERT_VIOLATION")
    }
  }

  test("NOT NULL checks for nested structs, arrays, maps (byName)") {
    checkNotNullNestedStructArrayMap(byName = true)
  }

  test("NOT NULL checks for nested structs, arrays, maps (byPosition)") {
    checkNotNullNestedStructArrayMap(byName = false)
  }

  private def checkNotNullNestedStructArrayMap(byName: Boolean): Unit = {
    withTable("t") {
      sql(
        s"""CREATE TABLE t (
           | i INT,
           | s STRUCT<
           |  ns: STRUCT<x: INT, y: INT> NOT NULL,
           |  arr: ARRAY<INT> NOT NULL,
           |  m: MAP<INT, INT> NOT NULL>)
           |USING $FORMAT
         """.stripMargin)

      val e1 = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | named_struct('ns', null, 'arr', array(1), 'm', map(1, 1)) AS s
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | named_struct('ns', null, 'arr', array(1), 'm', map(1, 1)) AS s)
             """.stripMargin)
        }
      }
      assertNotNullException(e1, Seq("s", "ns"))

      val e2 = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | named_struct('ns', named_struct('x', 1, 'y', 1), 'arr', null, 'm', map(1, 1)) AS s
           """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | named_struct('ns', named_struct('x', 1, 'y', 1), 'arr', null, 'm', map(1, 1)) AS s)
             """.stripMargin)
        }
      }
      assertNotNullException(e2, Seq("s", "arr"))

      val e3 = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | named_struct('ns', named_struct('x', 1, 'y', 1), 'arr', array(1), 'm', null) AS s
           """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | named_struct('ns', named_struct('x', 1, 'y', 1), 'arr', array(1), 'm', null))
             """.stripMargin)
        }
      }
      assertNotNullException(e3, Seq("s", "m"))
    }
  }

  test("NOT NULL checks for nested struct fields (byName)") {
    checkNotNullNestedStructFields(byName = true)
  }

  test("NOT NULL checks for nested struct fields (byPosition)") {
    checkNotNullNestedStructFields(byName = false)
  }

  private def checkNotNullNestedStructFields(byName: Boolean): Unit = {
    withTable("t") {
      sql(
        s"""CREATE TABLE t (
           | i INT,
           | s STRUCT<ni: INT, ns: STRUCT<x: INT NOT NULL, y: INT>>)
           |USING $FORMAT
         """.stripMargin)

      if (byName) {
        val inputDF = sql(
          s"""SELECT
             | 1 AS i,
             | named_struct('ni', 1, 'ns', null) AS s
           """.stripMargin)
        inputDF.writeTo("t").append()
      } else {
        sql(
          s"""INSERT INTO t VALUES (
             | 1 AS i,
             | named_struct('ni', 1, 'ns', null) AS s)
           """.stripMargin)
      }
      checkAnswer(spark.table("t"), Row(1, Row(1, null)))

      val e = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | named_struct('ni', 1, 'ns', named_struct('x', null, 'y', 1)) AS s
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | named_struct('ni', 1, 'ns', named_struct('x', null, 'y', 1)) AS s)
             """.stripMargin)
        }
      }
      assertNotNullException(e, Seq("s", "ns", "x"))
    }
  }

  test("NOT NULL checks for nullable array with required element (byName)") {
    checkNullableArrayWithNotNullElement(byName = true)
  }

  test("NOT NULL checks for nullable array with required element (byPosition)") {
    checkNullableArrayWithNotNullElement(byName = false)
  }

  private def checkNullableArrayWithNotNullElement(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int").add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("arr", ArrayType(structType, containsNull = false))))
        .build()

      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, null AS arr")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, null AS arr)")
      }
      checkAnswer(spark.table("t"), Row(1, null))

      val e = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | array(null, named_struct('x', 1, 'y', 1)) AS arr
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | array(null, named_struct('x', 1, 'y', 1)) AS arr)
             """.stripMargin)
        }
      }
      assertNotNullException(e, Seq("arr", "element"))
    }
  }

  test("NOT NULL checks for fields inside nullable array (byName)") {
    checkNotNullFieldsInsideNullableArray(byName = true)
  }

  test("not null checks for fields inside nullable array (byPosition)") {
    checkNotNullFieldsInsideNullableArray(byName = false)
  }

  private def checkNotNullFieldsInsideNullableArray(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("arr", ArrayType(structType, containsNull = true))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql(
          s"""SELECT
             | 1 AS i,
             | array(null, named_struct('x', 1, 'y', 1)) AS arr
           """.stripMargin)
        inputDF.writeTo("t").append()
      } else {
        sql(
          s"""INSERT INTO t VALUES (
             | 1 AS i,
             | array(null, named_struct('x', 1, 'y', 1)) AS arr)
           """.stripMargin)
      }
      checkAnswer(spark.table("t"), Row(1, List(null, Row(1, 1))))

      val e = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | array(null, named_struct('x', null, 'y', 1)) AS arr
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | array(null, named_struct('x', null, 'y', 1)) AS arr)
             """.stripMargin)
        }
      }
      assertNotNullException(e, Seq("arr", "element", "x"))
    }
  }

  test("NOT NULL checks for nullable map with required values (byName)") {
    checkNullableMapWithNonNullValues(byName = true)
  }

  test("NOT NULL checks for nullable map with required values (byPosition)") {
    checkNullableMapWithNonNullValues(byName = false)
  }

  private def checkNullableMapWithNonNullValues(byName: Boolean): Unit = {
    withTable("t") {
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(IntegerType, IntegerType, valueContainsNull = false))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, null AS m")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, null AS m)")
      }
      checkAnswer(spark.table("t"), Row(1, null))

      val e = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql("SELECT 1 AS i, map(1, null) AS m")
          inputDF.writeTo("t").append()
        } else {
          sql("INSERT INTO t VALUES (1 AS i, map(1, null) AS m)")
        }
      }
      assertNotNullException(e, Seq("m", "value"))
    }
  }

  test("NOT NULL checks for fields inside nullable maps (byName)") {
    checkNotNullFieldsInsideNullableMap(byName = true)
  }

  test("NOT NULL checks for fields inside nullable maps (byPosition)") {
    checkNotNullFieldsInsideNullableMap(byName = false)
  }

  private def checkNotNullFieldsInsideNullableMap(byName: Boolean): Unit = {
    withTable("t") {
      val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
      val tableInfo = new TableInfo.Builder()
        .withColumns(Array(
          ColumnV2.create("i", IntegerType),
          ColumnV2.create("m", MapType(structType, structType, valueContainsNull = true))))
        .build()
      catalog.createTable(
        ident = Identifier.of(Array(), "t"),
        tableInfo = tableInfo)

      if (byName) {
        val inputDF = sql("SELECT 1 AS i, map(named_struct('x', 1, 'y', 1), null) AS m")
        inputDF.writeTo("t").append()
      } else {
        sql("INSERT INTO t VALUES (1 AS i, map(named_struct('x', 1, 'y', 1), null) AS m)")
      }
      checkAnswer(spark.table("t"), Row(1, Map(Row(1, 1) -> null)))

      val e1 = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | map(named_struct('x', null, 'y', 1), null) AS m
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | map(named_struct('x', null, 'y', 1), null) AS m)
             """.stripMargin)
        }
      }
      assertNotNullException(e1, Seq("m", "key", "x"))

      val e2 = intercept[SparkRuntimeException] {
        if (byName) {
          val inputDF = sql(
            s"""SELECT
               | 1 AS i,
               | map(named_struct('x', 1, 'y', 1), named_struct('x', null, 'y', 1)) AS m
             """.stripMargin)
          inputDF.writeTo("t").append()
        } else {
          sql(
            s"""INSERT INTO t VALUES (
               | 1 AS i,
               | map(named_struct('x', 1, 'y', 1), named_struct('x', null, 'y', 1)) AS m)
             """.stripMargin)
        }
      }
      assertNotNullException(e2, Seq("m", "value", "x"))
    }
  }

  private def assertNotNullException(e: SparkRuntimeException, colPath: Seq[String]): Unit = {
    e.getCause match {
      case _ if e.getCondition == "NOT_NULL_ASSERT_VIOLATION" =>
      case other =>
        fail(s"Unexpected exception cause: $other")
    }
  }
}
