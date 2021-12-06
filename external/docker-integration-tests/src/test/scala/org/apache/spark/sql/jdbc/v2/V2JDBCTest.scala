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

package org.apache.spark.sql.jdbc.v2

import org.apache.log4j.Level

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Sample}
import org.apache.spark.sql.connector.catalog.{Catalogs, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.index.SupportsIndex
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.jdbc.DockerIntegrationFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

@DockerTest
private[v2] trait V2JDBCTest extends SharedSparkSession with DockerIntegrationFunSuite {
  import testImplicits._

  val catalogName: String
  // dialect specific update column type test
  def testUpdateColumnType(tbl: String): Unit

  def notSupportsTableComment: Boolean = false

  val defaultMetadata = new MetadataBuilder().putLong("scale", 0).build()

  def testUpdateColumnNullability(tbl: String): Unit = {
    sql(s"CREATE TABLE $catalogName.alt_table (ID STRING NOT NULL)")
    var t = spark.table(s"$catalogName.alt_table")
    // nullable is true in the expectedSchema because Spark always sets nullable to true
    // regardless of the JDBC metadata https://github.com/apache/spark/pull/18445
    var expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN ID DROP NOT NULL")
    t = spark.table(s"$catalogName.alt_table")
    expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
    // Update nullability of not existing column
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN bad_column DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Missing field bad_column"))
  }

  def testRenameColumn(tbl: String): Unit = {
    sql(s"ALTER TABLE $tbl RENAME COLUMN ID TO RENAMED")
    val t = spark.table(s"$tbl")
    val expectedSchema = new StructType().add("RENAMED", StringType, true, defaultMetadata)
      .add("ID1", StringType, true, defaultMetadata).add("ID2", StringType, true, defaultMetadata)
    assert(t.schema === expectedSchema)
  }

  def testCreateTableWithProperty(tbl: String): Unit = {}

  test("SPARK-33034: ALTER TABLE ... add new columns") {
    withTable(s"$catalogName.alt_table") {
      sql(s"CREATE TABLE $catalogName.alt_table (ID STRING)")
      var t = spark.table(s"$catalogName.alt_table")
      var expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata)
      assert(t.schema === expectedSchema)
      sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C1 STRING, C2 STRING)")
      t = spark.table(s"$catalogName.alt_table")
      expectedSchema = expectedSchema.add("C1", StringType, true, defaultMetadata)
        .add("C2", StringType, true, defaultMetadata)
      assert(t.schema === expectedSchema)
      sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C3 STRING)")
      t = spark.table(s"$catalogName.alt_table")
      expectedSchema = expectedSchema.add("C3", StringType, true, defaultMetadata)
      assert(t.schema === expectedSchema)
      // Add already existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table ADD COLUMNS (C3 DOUBLE)")
      }.getMessage
      assert(msg.contains("Cannot add column, because C3 already exists"))
    }
    // Add a column to not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ADD COLUMNS (C4 STRING)")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... drop column") {
    withTable(s"$catalogName.alt_table") {
      sql(s"CREATE TABLE $catalogName.alt_table (C1 INTEGER, C2 STRING, c3 INTEGER)")
      sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN C1")
      sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN c3")
      val t = spark.table(s"$catalogName.alt_table")
      val expectedSchema = new StructType().add("C2", StringType, true, defaultMetadata)
      assert(t.schema === expectedSchema)
      // Drop not existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table DROP COLUMN bad_column")
      }.getMessage
      assert(msg.contains(s"Missing field bad_column in table $catalogName.alt_table"))
    }
    // Drop a column from a not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table DROP COLUMN C1")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... update column type") {
    withTable(s"$catalogName.alt_table") {
      testUpdateColumnType(s"$catalogName.alt_table")
      // Update not existing column
      val msg2 = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }.getMessage
      assert(msg2.contains("Missing field bad_column"))
    }
    // Update column type in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ALTER COLUMN id TYPE DOUBLE")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... rename column") {
    withTable(s"$catalogName.alt_table") {
      sql(s"CREATE TABLE $catalogName.alt_table (ID STRING NOT NULL," +
        s" ID1 STRING NOT NULL, ID2 STRING NOT NULL)")
      testRenameColumn(s"$catalogName.alt_table")
      // Rename to already existing column
      val msg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalogName.alt_table RENAME COLUMN ID1 TO ID2")
      }.getMessage
      assert(msg.contains("Cannot rename column, because ID2 already exists"))
    }
    // Rename a column in a not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table RENAME COLUMN ID TO C")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("SPARK-33034: ALTER TABLE ... update column nullability") {
    withTable(s"$catalogName.alt_table") {
      testUpdateColumnNullability(s"$catalogName.alt_table")
    }
    // Update column nullability in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE $catalogName.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("CREATE TABLE with table comment") {
    withTable(s"$catalogName.new_table") {
      val logAppender = new LogAppender("table comment")
      withLogAppender(logAppender) {
        sql(s"CREATE TABLE $catalogName.new_table(i INT) COMMENT 'this is a comment'")
      }
      val createCommentWarning = logAppender.loggingEvents
        .filter(_.getLevel == Level.WARN)
        .map(_.getRenderedMessage)
        .exists(_.contains("Cannot create JDBC table comment"))
      assert(createCommentWarning === notSupportsTableComment)
    }
  }

  test("CREATE TABLE with table property") {
    withTable(s"$catalogName.new_table") {
      val m = intercept[AnalysisException] {
        sql(s"CREATE TABLE $catalogName.new_table (i INT) TBLPROPERTIES('a'='1')")
      }.message
      assert(m.contains("Failed table creation"))
      testCreateTableWithProperty(s"$catalogName.new_table")
    }
  }

  def supportsIndex: Boolean = false

  test("SPARK-36895: Test INDEX Using SQL") {
    if (supportsIndex) {
      withTable(s"$catalogName.new_table") {
        sql(s"CREATE TABLE $catalogName.new_table(col1 INT, col2 INT, col3 INT," +
          " col4 INT, col5 INT)")
        val loaded = Catalogs.load(catalogName, conf)
        val jdbcTable = loaded.asInstanceOf[TableCatalog]
          .loadTable(Identifier.of(Array.empty[String], "new_table"))
          .asInstanceOf[SupportsIndex]
        assert(jdbcTable.indexExists("i1") == false)
        assert(jdbcTable.indexExists("i2") == false)

        val indexType = "DUMMY"
        var m = intercept[UnsupportedOperationException] {
          sql(s"CREATE index i1 ON $catalogName.new_table USING $indexType (col1)")
        }.getMessage
        assert(m.contains(s"Index Type $indexType is not supported." +
          s" The supported Index Types are: BTREE and HASH"))

        sql(s"CREATE index i1 ON $catalogName.new_table USING BTREE (col1)")
        sql(s"CREATE index i2 ON $catalogName.new_table (col2, col3, col5)" +
          s" OPTIONS (KEY_BLOCK_SIZE=10)")

        assert(jdbcTable.indexExists("i1") == true)
        assert(jdbcTable.indexExists("i2") == true)

        // This should pass without exception
        sql(s"CREATE index IF NOT EXISTS i1 ON $catalogName.new_table (col1)")

        m = intercept[IndexAlreadyExistsException] {
          sql(s"CREATE index i1 ON $catalogName.new_table (col1)")
        }.getMessage
        assert(m.contains("Failed to create index i1 in new_table"))

        sql(s"DROP index i1 ON $catalogName.new_table")
        sql(s"DROP index i2 ON $catalogName.new_table")

        assert(jdbcTable.indexExists("i1") == false)
        assert(jdbcTable.indexExists("i2") == false)

        // This should pass without exception
        sql(s"DROP index IF EXISTS i1 ON $catalogName.new_table")

        m = intercept[NoSuchIndexException] {
          sql(s"DROP index i1 ON $catalogName.new_table")
        }.getMessage
        assert(m.contains("Failed to drop index i1 in new_table"))
      }
    }
  }

  def supportsTableSample: Boolean = false

  private def samplePushed(df: DataFrame): Boolean = {
    val sample = df.queryExecution.optimizedPlan.collect {
      case s: Sample => s
    }
    sample.isEmpty
  }

  private def filterPushed(df: DataFrame): Boolean = {
    val filter = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    filter.isEmpty
  }

  private def limitPushed(df: DataFrame, limit: Int): Boolean = {
    val filter = df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          return v1.pushedDownOperators.limit == Some(limit)
      }
    }
    false
  }

  private def columnPruned(df: DataFrame, col: String): Boolean = {
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    scan.schema.names.sameElements(Seq(col))
  }

  test("SPARK-37038: Test TABLESAMPLE") {
    if (supportsTableSample) {
      withTable(s"$catalogName.new_table") {
        sql(s"CREATE TABLE $catalogName.new_table (col1 INT, col2 INT)")
        spark.range(10).select($"id" * 2, $"id" * 2 + 1).write.insertInto(s"$catalogName.new_table")

        // sample push down + column pruning
        val df1 = sql(s"SELECT col1 FROM $catalogName.new_table TABLESAMPLE (BUCKET 6 OUT OF 10)" +
          " REPEATABLE (12345)")
        assert(samplePushed(df1))
        assert(columnPruned(df1, "col1"))
        assert(df1.collect().length < 10)

        // sample push down only
        val df2 = sql(s"SELECT * FROM $catalogName.new_table TABLESAMPLE (50 PERCENT)" +
          " REPEATABLE (12345)")
        assert(samplePushed(df2))
        assert(df2.collect().length < 10)

        // sample(BUCKET ... OUT OF) push down + limit push down + column pruning
        val df3 = sql(s"SELECT col1 FROM $catalogName.new_table TABLESAMPLE (BUCKET 6 OUT OF 10)" +
          " LIMIT 2")
        assert(samplePushed(df3))
        assert(limitPushed(df3, 2))
        assert(columnPruned(df3, "col1"))
        assert(df3.collect().length <= 2)

        // sample(... PERCENT) push down + limit push down + column pruning
        val df4 = sql(s"SELECT col1 FROM $catalogName.new_table" +
          " TABLESAMPLE (50 PERCENT) REPEATABLE (12345) LIMIT 2")
        assert(samplePushed(df4))
        assert(limitPushed(df4, 2))
        assert(columnPruned(df4, "col1"))
        assert(df4.collect().length <= 2)

        // sample push down + filter push down + limit push down
        val df5 = sql(s"SELECT * FROM $catalogName.new_table" +
          " TABLESAMPLE (BUCKET 6 OUT OF 10) WHERE col1 > 0 LIMIT 2")
        assert(samplePushed(df5))
        assert(filterPushed(df5))
        assert(limitPushed(df5, 2))
        assert(df5.collect().length <= 2)

        // sample + filter + limit + column pruning
        // sample pushed down, filer/limit not pushed down, column pruned
        // Todo: push down filter/limit
        val df6 = sql(s"SELECT col1 FROM $catalogName.new_table" +
          " TABLESAMPLE (BUCKET 6 OUT OF 10) WHERE col1 > 0 LIMIT 2")
        assert(samplePushed(df6))
        assert(!filterPushed(df6))
        assert(!limitPushed(df6, 2))
        assert(columnPruned(df6, "col1"))
        assert(df6.collect().length <= 2)

        // sample + limit
        // Push down order is sample -> filter -> limit
        // only limit is pushed down because in this test sample is after limit
        val df7 = spark.read.table(s"$catalogName.new_table").limit(2).sample(0.5)
        assert(!samplePushed(df7))
        assert(limitPushed(df7, 2))

        // sample + filter
        // Push down order is sample -> filter -> limit
        // only filter is pushed down because in this test sample is after filter
        val df8 = spark.read.table(s"$catalogName.new_table").where($"col1" > 1).sample(0.5)
        assert(!samplePushed(df8))
        assert(filterPushed(df8))
        assert(df8.collect().length < 10)
      }
    }
  }
}
