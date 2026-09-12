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

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

/**
 * A catalog that creates [[InMemoryRowLevelOperationTable]]s carrying a fixed
 * [[SchemaAlignmentConfig]] supplied by the concrete subclass. It returns the live table instance
 * on load (rather than a copy) so the config is preserved for the analyzer.
 */
abstract class SchemaAlignmentTestCatalog extends InMemoryRowLevelOperationTableCatalog {

  protected def tableConfig: SchemaAlignmentConfig

  override def loadTable(ident: Identifier): Table = liveTable(ident)

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

/** A catalog whose tables opt into every [[SchemaAlignmentConfig]] relaxation. */
class RelaxedSchemaAlignmentCatalog extends SchemaAlignmentTestCatalog {
  override protected def tableConfig: SchemaAlignmentConfig = new SchemaAlignmentConfig {
    override def allowLegacyStoreAssignmentPolicy(): Boolean = true
    override def deferCastValidationToRuntime(): Boolean = true
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

  private def withLegacyPolicy(f: => Unit): Unit =
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString)(f)

  private def withAnsiPolicy(f: => Unit): Unit =
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString)(f)

  private def legacyRejected(f: => Unit): Unit =
    checkError(
      exception = intercept[AnalysisException](f),
      condition = "_LEGACY_ERROR_TEMP_1000",
      parameters = Map("configKey" -> SQLConf.STORE_ASSIGNMENT_POLICY.key))

  test("allowLegacyStoreAssignmentPolicy: INSERT under LEGACY policy") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT) USING foo")
      withLegacyPolicy {
        sql(s"INSERT INTO $relaxed.t VALUES (1)")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1))
        legacyRejected(sql(s"INSERT INTO $strict.t VALUES (1)"))
      }
    }
  }

  test("allowLegacyStoreAssignmentPolicy: UPDATE under LEGACY policy") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT, data STRING) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $relaxed.t VALUES (1, 'a')")
      sql(s"INSERT INTO $strict.t VALUES (1, 'a')")
      withLegacyPolicy {
        sql(s"UPDATE $relaxed.t SET data = 'b' WHERE id = 1")
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1, "b"))
        legacyRejected(sql(s"UPDATE $strict.t SET data = 'b' WHERE id = 1"))
      }
    }
  }

  test("deferCastValidationToRuntime: INSERT of an ANSI-incompatible cast") {
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

  test("deferCastValidationToRuntime: UPDATE with an ANSI-incompatible cast") {
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

  test("allowLegacyStoreAssignmentPolicy: MERGE under LEGACY policy") {
    withTable(s"$relaxed.t", s"$strict.t") {
      sql(s"CREATE TABLE $relaxed.t (id INT, data STRING) USING foo")
      sql(s"CREATE TABLE $strict.t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $relaxed.t VALUES (1, 'a')")
      sql(s"INSERT INTO $strict.t VALUES (1, 'a')")
      def merge(target: String): String =
        s"""MERGE INTO $target t
           |USING (SELECT 1 AS id, 'b' AS data) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.data = s.data""".stripMargin
      withLegacyPolicy {
        sql(merge(s"$relaxed.t"))
        checkAnswer(sql(s"SELECT * FROM $relaxed.t"), Row(1, "b"))
        legacyRejected(sql(merge(s"$strict.t")))
      }
    }
  }

  test("deferCastValidationToRuntime: MERGE with an ANSI-incompatible cast") {
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

  test("deferCastValidationToRuntime: structurally impossible casts are still rejected") {
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

  private def assertRelaxedMatchesStrict(targetSchema: StructType, source: DataFrame): Unit =
    withAnsiPolicy {
      val fromRelaxed = appendByName(relaxed, targetSchema, source)
      val fromStrict = appendByName(strict, targetSchema, source)
      (fromRelaxed, fromStrict) match {
        case (Success(relaxedRows), Success(strictRows)) =>
          assert(relaxedRows.map(_.toString).sorted == strictRows.map(_.toString).sorted,
            s"relaxed=$relaxedRows strict=$strictRows")
        case (Failure(relaxedError: AnalysisException), Failure(strictError: AnalysisException)) =>
          assert(relaxedError.getCondition == strictError.getCondition,
            s"relaxed=${relaxedError.getCondition} strict=${strictError.getCondition}")
        case (Failure(_), Failure(_)) =>
        case _ =>
          fail(s"relaxed and strict diverged: relaxed=$fromRelaxed strict=$fromStrict")
      }
    }

  test("deferCastValidationToRuntime: renamed nested struct field is still rejected") {
    val target = new StructType()
      .add("s", new StructType().add("a", IntegerType).add("b", IntegerType))
    val source = spark.createDataFrame(
      java.util.Arrays.asList(Row(Row(1, 2))),
      new StructType().add("s", new StructType().add("a", IntegerType).add("c", IntegerType)))
    assertRelaxedMatchesStrict(target, source)
  }

  test("deferCastValidationToRuntime: nullable array element into non-null element type") {
    val target = new StructType().add("a", ArrayType(IntegerType, containsNull = false))
    val source = spark.createDataFrame(
      java.util.Arrays.asList(Row(Seq(1, 2))),
      new StructType().add("a", ArrayType(IntegerType, containsNull = true)))
    assertRelaxedMatchesStrict(target, source)
  }

  test("deferCastValidationToRuntime: nullable map value into non-null value type") {
    val target = new StructType()
      .add("m", MapType(StringType, IntegerType, valueContainsNull = false))
    val source = spark.createDataFrame(
      java.util.Arrays.asList(Row(Map("k" -> 1))),
      new StructType().add("m", MapType(StringType, IntegerType, valueContainsNull = true)))
    assertRelaxedMatchesStrict(target, source)
  }

  test("deferCastValidationToRuntime: nullable child into non-null struct field") {
    val target = new StructType()
      .add("s", new StructType().add("a", IntegerType, nullable = false))
    val source = spark.createDataFrame(
      java.util.Arrays.asList(Row(Row(1))),
      new StructType().add("s", new StructType().add("a", IntegerType, nullable = true)))
    assertRelaxedMatchesStrict(target, source)
  }
}
