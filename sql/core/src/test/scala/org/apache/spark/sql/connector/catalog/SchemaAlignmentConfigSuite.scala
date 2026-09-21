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

package org.apache.spark.sql.connector.catalog

import scala.util.{Failure, Try}

import org.apache.spark.SparkConf
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig.AnsiStoreAssignmentCastCheck
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * A catalog that creates [[InMemoryRowLevelOperationTable]]s carrying the fixed
 * [[SchemaAlignmentConfig]] supplied by the concrete subclass.
 */
abstract class SchemaAlignmentTestCatalog extends InMemoryRowLevelOperationTableCatalog {

  protected def tableConfig: SchemaAlignmentConfig

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }
    val name = s"${this.name}.${ident.quoted}"
    val schema = CatalogV2Util.v2ColumnsToStructType(tableInfo.columns)
    val table = new InMemoryRowLevelOperationTable(
      name, schema, tableInfo.partitions, tableInfo.properties, tableInfo.constraints(),
      schemaAlignmentConfig = tableConfig)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

/** A catalog whose tables defer the ANSI store-assignment cast check to runtime. */
class RelaxedSchemaAlignmentCatalog extends SchemaAlignmentTestCatalog {
  override protected def tableConfig: SchemaAlignmentConfig = new SchemaAlignmentConfig {
    override def ansiStoreAssignmentCastCheck(): AnsiStoreAssignmentCastCheck =
      AnsiStoreAssignmentCastCheck.AT_RUNTIME
  }
}

/** A catalog whose tables keep the strict data source v2 defaults. */
class StrictSchemaAlignmentCatalog extends SchemaAlignmentTestCatalog {
  override protected def tableConfig: SchemaAlignmentConfig = SchemaAlignmentConfig.DEFAULT
}

/**
 * End-to-end coverage for [[SchemaAlignmentConfig]]: a table that opts into a relaxation gets the
 * more permissive analyzer behavior, while an otherwise identical table using the default (strict)
 * config keeps the data source v2 behavior. Exercised on both the INSERT path
 * ([[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveOutputRelation]]) and the row-level
 * path ([[org.apache.spark.sql.catalyst.analysis.ResolveRowLevelCommandAssignments]]).
 */
class SchemaAlignmentConfigSuite extends QueryTest with SharedSparkSession {

  private val relaxed = "relaxed"
  private val strict = "strict"

  override def sparkConf: SparkConf =
    super.sparkConf
      .set(s"spark.sql.catalog.$relaxed", classOf[RelaxedSchemaAlignmentCatalog].getName)
      .set(s"spark.sql.catalog.$strict", classOf[StrictSchemaAlignmentCatalog].getName)

  private def withAnsiPolicy(f: => Unit): Unit =
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString)(f)

  test("AT_RUNTIME: INSERT of an ANSI-incompatible cast") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT) USING foo")
      withAnsiPolicy {
        sql(s"INSERT INTO $relaxed.t VALUES ('1')")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1))
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO $strict.t VALUES ('1')")
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> s"`$strict`.`t`",
            "colName" -> "`id`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\""))
      }
    }
  }

  test("AT_RUNTIME: UPDATE with an ANSI-incompatible cast") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT, data INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT, data INT) USING foo")
      sql(s"INSERT INTO $relaxed.t VALUES (1, 0)")
      sql(s"INSERT INTO $strict.t VALUES (1, 0)")
      withAnsiPolicy {
        sql(s"UPDATE $relaxed.t SET data = '5' WHERE id = 1")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1, 5))
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"UPDATE $strict.t SET data = '5' WHERE id = 1")
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> "``",
            "colName" -> "`data`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\""))
      }
    }
  }

  test("AT_RUNTIME: MERGE with an ANSI-incompatible cast") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT, data INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT, data INT) USING foo")
      sql(s"INSERT INTO $relaxed.t VALUES (1, 0)")
      sql(s"INSERT INTO $strict.t VALUES (1, 0)")
      def merge(target: String): String =
        s"""MERGE INTO $target t
           |USING (SELECT 1 AS id, '5' AS data) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.data = s.data""".stripMargin
      withAnsiPolicy {
        sql(merge(s"$relaxed.t"))
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1, 5))
        checkError(
          exception = intercept[AnalysisException](sql(merge(s"$strict.t"))),
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> "``",
            "colName" -> "`data`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\""))
      }
    }
  }

  test("AT_RUNTIME: structurally impossible casts are still rejected") {
    withTable(s"$relaxed.t") {
      sql(s"CREATE TABLE $relaxed.t (d DATE) USING foo")
      withAnsiPolicy {
        // BOOLEAN cannot be cast to DATE at all, so the write is rejected even though the table
        // defers store-assignment cast validation to runtime.
        intercept[AnalysisException] {
          sql(s"INSERT INTO $relaxed.t VALUES (true)")
        }
      }
    }
  }

  test("AT_RUNTIME: complex-to-string cast is deferred") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (c STRING) USING foo")
      sql(s"CREATE TABLE $strict.t (c STRING) USING foo")
      withAnsiPolicy {
        sql(s"INSERT INTO $relaxed.t VALUES (array(1, 2))")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row("[1, 2]"))
        val strictInsert = s"INSERT INTO $strict.t VALUES (array(1, 2))"
        checkError(
          exception = intercept[AnalysisException](sql(strictInsert)),
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> s"`$strict`.`t`",
            "colName" -> "`c`",
            "srcType" -> "\"ARRAY<INT>\"",
            "targetType" -> "\"STRING\""))
      }
    }
  }

  test("schema alignment config survives ALTER TABLE ADD COLUMNS") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT) USING foo")
      sql(s"ALTER TABLE $relaxed.t ADD COLUMNS (data INT)")
      sql(s"ALTER TABLE $strict.t ADD COLUMNS (data INT)")
      withAnsiPolicy {
        sql(s"INSERT INTO $relaxed.t VALUES (1, '5')")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1, 5))
        checkError(
          exception = intercept[AnalysisException](sql(s"INSERT INTO $strict.t VALUES (1, '5')")),
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> s"`$strict`.`t`",
            "colName" -> "`data`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\""))
      }
    }
  }

  test("schema alignment config survives schema evolution") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT) USING foo")
      val evolving = spark.createDataFrame(
        java.util.Arrays.asList(Row(1, 5)),
        new StructType().add("id", IntegerType).add("data", IntegerType))
      evolving.write.mode("append").withSchemaEvolution().insertInto(s"$relaxed.t")
      evolving.write.mode("append").withSchemaEvolution().insertInto(s"$strict.t")
      withAnsiPolicy {
        sql(s"INSERT INTO $relaxed.t VALUES (2, '5')")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Seq(Row(1, 5), Row(2, 5)))
        checkError(
          exception = intercept[AnalysisException](sql(s"INSERT INTO $strict.t VALUES (2, '5')")),
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> s"`$strict`.`t`",
            "colName" -> "`data`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\""))
      }
    }
  }

  private def appendByName(
      catalog: String, targetSchema: StructType, source: DataFrame): Try[Seq[Row]] = {
    var result: Try[Seq[Row]] = Try(Seq.empty[Row])
    withTable(s"$catalog.t") {
      spark.createDataFrame(new java.util.ArrayList[Row](), targetSchema)
        .writeTo(s"$catalog.t").create()
      result = Try {
        source.writeTo(s"$catalog.t").append()
        spark.table(s"$catalog.t").collect().toSeq
      }
    }
    result
  }

  private def assertBothReject(
      targetSchema: StructType, source: DataFrame, errorClass: String): Unit =
    withAnsiPolicy {
      Seq(relaxed, strict).foreach { catalog =>
        appendByName(catalog, targetSchema, source) match {
          case Failure(error: SparkThrowable) =>
            assert(error.getCondition == errorClass,
              s"$catalog: expected $errorClass, got ${error.getCondition}")
          case other =>
            fail(s"$catalog: expected rejection with $errorClass, got $other")
        }
      }
    }

  test("AT_RUNTIME: renamed nested struct field is still rejected") {
    val target = new StructType()
      .add("s", new StructType().add("a", IntegerType).add("b", IntegerType))
    val source = spark.createDataFrame(
      java.util.Arrays.asList(Row(Row(1, 2))),
      new StructType().add("s", new StructType().add("a", IntegerType).add("c", IntegerType)))
    assertBothReject(target, source, "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
  }

}
