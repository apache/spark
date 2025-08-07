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

package org.apache.spark.sql.hive

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel.{DISK_ONLY, MEMORY_ONLY}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class CachedTableSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._

  def rddIdOf(tableName: String): Int = {
    val plan = table(tableName).queryExecution.sparkPlan
    plan.collect {
      case InMemoryTableScanExec(_, _, relation) =>
        relation.cacheBuilder.cachedColumnBuffers.id
      case _ =>
        fail(s"Table $tableName is not cached\n" + plan)
    }.head
  }

  def isMaterialized(rddId: Int): Boolean = {
    val maybeBlock = sparkContext.env.blockManager.get(RDDBlockId(rddId, 0))
    maybeBlock.foreach(_ => sparkContext.env.blockManager.releaseLock(RDDBlockId(rddId, 0)))
    maybeBlock.nonEmpty
  }

  test("cache table") {
    val preCacheResults = sql("SELECT * FROM src").collect().toSeq

    cacheTable("src")
    assertCached(sql("SELECT * FROM src"))

    checkAnswer(
      sql("SELECT * FROM src"),
      preCacheResults)

    assertCached(sql("SELECT * FROM src s"))

    checkAnswer(
      sql("SELECT * FROM src s"),
      preCacheResults)

    uncacheTable("src")
    assertCached(sql("SELECT * FROM src"), 0)
  }

  test("cache invalidation") {
    sql("CREATE TABLE cachedTable(key INT, value STRING)")

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    cacheTable("cachedTable")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(
      sql("SELECT * FROM cachedTable"),
      table("src").collect().toSeq ++ table("src").collect().toSeq)

    sql("DROP TABLE cachedTable")
  }

  test("Drop cached table") {
    sql("CREATE TABLE cachedTableTest(a INT)")
    cacheTable("cachedTableTest")
    sql("SELECT * FROM cachedTableTest").collect()
    sql("DROP TABLE cachedTableTest")
    intercept[AnalysisException] {
      sql("SELECT * FROM cachedTableTest").collect()
    }
  }

  test("DROP nonexistent table") {
    sql("DROP TABLE IF EXISTS nonexistentTable")
  }

  test("uncache of nonexistent tables") {
    // make sure table doesn't exist
    var e = intercept[AnalysisException](spark.table("nonexistentTable"))
    checkErrorTableNotFound(e, "`nonexistentTable`")
    e = intercept[AnalysisException] {
      uncacheTable("nonexistentTable")
    }
    checkErrorTableNotFound(e, "`nonexistentTable`")
     e = intercept[AnalysisException] {
      sql("UNCACHE TABLE nonexistentTable")
    }
    checkErrorTableNotFound(e, "`nonexistentTable`",
      ExpectedContext("nonexistentTable", 14, 13 + "nonexistentTable".length))
    sql("UNCACHE TABLE IF EXISTS nonexistentTable")
  }

  test("no error on uncache of non-cached table") {
    val tableName = "newTable"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(a INT)")
      // no error will be reported in the following three ways to uncache a table.
      uncacheTable(tableName)
      sql("UNCACHE TABLE newTable")
      sparkSession.table(tableName).unpersist(blocking = true)
    }
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' HiveQL statement") {
    sql("CACHE TABLE src")
    assertCached(table("src"))
    assert(spark.catalog.isCached("src"), "Table 'src' should be cached")

    sql("UNCACHE TABLE src")
    assertCached(table("src"), 0)
    assert(!spark.catalog.isCached("src"), "Table 'src' should not be cached")
  }

  test("CACHE TABLE tableName AS SELECT * FROM anotherTable") {
    withTempView("testCacheTable") {
      sql("CACHE TABLE testCacheTable AS SELECT * FROM src")
      assertCached(table("testCacheTable"))

      val rddId = rddIdOf("testCacheTable")
      assert(
        isMaterialized(rddId),
        "Eagerly cached in-memory table should have already been materialized")

      uncacheTable("testCacheTable")
      assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
    }
  }

  test("CACHE TABLE tableName AS SELECT ...") {
    withTempView("testCacheTable") {
      sql("CACHE TABLE testCacheTable AS SELECT key FROM src LIMIT 10")
      assertCached(table("testCacheTable"))

      val rddId = rddIdOf("testCacheTable")
      assert(
        isMaterialized(rddId),
        "Eagerly cached in-memory table should have already been materialized")

      uncacheTable("testCacheTable")
      assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
    }
  }

  test("CACHE LAZY TABLE tableName") {
    sql("CACHE LAZY TABLE src")
    assertCached(table("src"))

    val rddId = rddIdOf("src")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    sql("SELECT COUNT(*) FROM src").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")

    uncacheTable("src")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE with Hive UDF") {
    withTempView("udfTest") {
      sql("CACHE TABLE udfTest AS SELECT * FROM src WHERE floor(key) = 1")
      assertCached(table("udfTest"))
      uncacheTable("udfTest")
    }
  }

  test("REFRESH TABLE also needs to recache the data (data source tables)") {
    val tempPath: File = Utils.createTempDir()
    tempPath.delete()
    table("src").write.mode(SaveMode.Overwrite).parquet(tempPath.toString)
    sql("DROP TABLE IF EXISTS refreshTable")
    sparkSession.catalog.createTable("refreshTable", tempPath.toString, "parquet")
    checkAnswer(table("refreshTable"), table("src"))
    // Cache the table.
    sql("CACHE TABLE refreshTable")
    assertCached(table("refreshTable"))
    // Append new data.
    table("src").write.mode(SaveMode.Append).parquet(tempPath.toString)
    assertCached(table("refreshTable"))

    // We are using the new data.
    assertCached(table("refreshTable"))
    checkAnswer(
      table("refreshTable"),
      table("src").union(table("src")).collect().toImmutableArraySeq)

    // Drop the table and create it again.
    sql("DROP TABLE refreshTable")
    sparkSession.catalog.createTable("refreshTable", tempPath.toString, "parquet")
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")
    // Refresh the table. REFRESH TABLE command should not make a uncached
    // table cached.
    sql("REFRESH TABLE refreshTable")
    checkAnswer(
      table("refreshTable"),
      table("src").union(table("src")).collect().toImmutableArraySeq)
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")

    sql("DROP TABLE refreshTable")
    Utils.deleteRecursively(tempPath)
  }

  test("SPARK-15678: REFRESH PATH") {
    val tempPath: File = Utils.createTempDir()
    tempPath.delete()
    table("src").write.mode(SaveMode.Overwrite).parquet(tempPath.toString)
    sql("DROP TABLE IF EXISTS refreshTable")
    sparkSession.catalog.createTable("refreshTable", tempPath.toString, "parquet")
    checkAnswer(
      table("refreshTable"),
      table("src").collect().toImmutableArraySeq)
    // Cache the table.
    sql("CACHE TABLE refreshTable")
    assertCached(table("refreshTable"))
    // Append new data.
    table("src").write.mode(SaveMode.Append).parquet(tempPath.toString)
    assertCached(table("refreshTable"))

    // We are using the new data.
    assertCached(table("refreshTable"))
    checkAnswer(
      table("refreshTable"),
      table("src").union(table("src")).collect().toImmutableArraySeq)

    // Drop the table and create it again.
    sql("DROP TABLE refreshTable")
    sparkSession.catalog.createTable("refreshTable", tempPath.toString, "parquet")
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")
    // Refresh the table. REFRESH command should not make a uncached
    // table cached.
    sql(s"REFRESH ${tempPath.toString}")
    checkAnswer(
      table("refreshTable"),
      table("src").union(table("src")).collect().toImmutableArraySeq)
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")

    sql("DROP TABLE refreshTable")
    Utils.deleteRecursively(tempPath)
  }

  test("Cache/Uncache Qualified Tables") {
    withTempDatabase { db =>
      withTempView("cachedTable") {
        sql(s"CREATE TABLE $db.cachedTable STORED AS PARQUET AS SELECT 1")
        sql(s"CACHE TABLE $db.cachedTable")
        assertCached(spark.table(s"$db.cachedTable"))

        activateDatabase(db) {
          assertCached(spark.table("cachedTable"))
          sql("UNCACHE TABLE cachedTable")
          assert(!spark.catalog.isCached("cachedTable"), "Table 'cachedTable' should not be cached")
          sql(s"CACHE TABLE cachedTable")
          assert(spark.catalog.isCached("cachedTable"), "Table 'cachedTable' should be cached")
        }

        sql(s"UNCACHE TABLE $db.cachedTable")
        assert(!spark.catalog.isCached(s"$db.cachedTable"),
          "Table 'cachedTable' should not be cached")
      }
    }
  }

  test("Cache Table As Select - having database name") {
    withTempDatabase { db =>
      withTempView("cachedTable") {
        val e = intercept[ParseException] {
          sql(s"CACHE TABLE $db.cachedTable AS SELECT 1")
        }.getMessage
        assert(e.contains("It is not allowed to add catalog/namespace prefix ") &&
          e.contains("to the table name in CACHE TABLE AS SELECT"))
      }
    }
  }

  test("SPARK-11246 cache parquet table") {
    sql("CREATE TABLE cachedTable STORED AS PARQUET AS SELECT 1")

    cacheTable("cachedTable")
    val sparkPlan = sql("SELECT * FROM cachedTable").queryExecution.sparkPlan
    assert(sparkPlan.collect { case e: InMemoryTableScanExec => e }.size === 1)

    sql("DROP TABLE cachedTable")
  }

  test("cache a table using CatalogFileIndex") {
    withTable("test") {
      sql("CREATE TABLE test(i int) PARTITIONED BY (p int) STORED AS parquet")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      val dataSchema = StructType(tableMeta.schema.filterNot { f =>
        tableMeta.partitionColumnNames.contains(f.name)
      })
      val relation = HadoopFsRelation(
        location = catalogFileIndex,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)

      val plan = LogicalRelation(relation, tableMeta)
      val df = Dataset.ofRows(spark, plan)
      spark.sharedState.cacheManager.cacheQuery(df)

      assert(spark.sharedState.cacheManager.lookupCachedData(df).isDefined)

      val sameCatalog = new CatalogFileIndex(spark, tableMeta, 0)
      val sameRelation = HadoopFsRelation(
        location = sameCatalog,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)
      val samePlanDf = Dataset.ofRows(spark, LogicalRelation(sameRelation, tableMeta))

      assert(spark.sharedState.cacheManager.lookupCachedData(samePlanDf).isDefined)
    }
  }

  test("SPARK-27248 refreshTable should recreate cache with same cache name and storage level") {
    // This section tests when a table is cached with its qualified name but its is refreshed with
    // its unqualified name.
    withTempDatabase { db =>
      withTable(s"$db.cachedTable") {
        withCache(s"$db.cachedTable") {
          // Create table 'cachedTable' in default db for testing purpose.
          sql(s"CREATE TABLE $db.cachedTable AS SELECT 1 AS key")

          // Cache the table 'cachedTable' in temp db with qualified table name,
          // and then check whether the table is cached with expected name
          sql(s"CACHE TABLE $db.cachedTable OPTIONS('storageLevel' 'MEMORY_ONLY')")
          assertCached(sql(s"SELECT * FROM $db.cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
          assert(spark.catalog.isCached(s"$db.cachedTable"),
            s"Table '$db.cachedTable' should be cached.")

          // Refresh the table 'cachedTable' in temp db with qualified table name, and then check
          // whether the table is still cached with the same name and storage level.
          sql(s"REFRESH TABLE $db.cachedTable")
          assertCached(sql(s"select * from $db.cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
          assert(spark.catalog.isCached(s"$db.cachedTable"),
            s"Table '$db.cachedTable' should be cached after refreshing with its qualified name.")

          // Change the active database to the temp db and refresh the table with unqualified
          // table name, and then check whether the table is still cached with the same name and
          // storage level.
          // Without bug fix 'SPARK-27248', the recreated cache name will be changed to
          // 'cachedTable', instead of '$db.cachedTable'
          activateDatabase(db) {
            sql("REFRESH TABLE cachedTable")
            assertCached(sql("SELECT * FROM cachedTable"), s"$db.cachedTable", MEMORY_ONLY)
            assert(spark.catalog.isCached("cachedTable"),
              s"Table '$db.cachedTable' should be cached after refreshing with its " +
                "unqualified name.")
          }
        }
      }
    }


    // This section tests when a table is cached with its unqualified name but it is refreshed
    // with its qualified name.
    withTempDatabase { db =>
      withTable("cachedTable") {
        withCache("cachedTable") {
          // Create table 'cachedTable' in default db for testing purpose.
          sql("CREATE TABLE cachedTable AS SELECT 1 AS key")

          // Cache the table 'cachedTable' in default db without qualified table name , and then
          // check whether the table is cached with expected name.
          sql("CACHE TABLE cachedTable OPTIONS('storageLevel' 'DISK_ONLY')")
          assertCached(sql("SELECT * FROM cachedTable"), "cachedTable", DISK_ONLY)
          assert(spark.catalog.isCached("cachedTable"), "Table 'cachedTable' should be cached.")

          // Refresh the table 'cachedTable' in default db with unqualified table name, and then
          // check whether the table is still cached with the same name.
          sql("REFRESH TABLE cachedTable")
          assertCached(sql("SELECT * FROM cachedTable"), "cachedTable", DISK_ONLY)
          assert(spark.catalog.isCached("cachedTable"),
            "Table 'cachedTable' should be cached after refreshing with its unqualified name.")

          // Change the active database to the temp db and refresh the table with qualified
          // table name, and then check whether the table is still cached with the same name and
          // storage level.
          // Without bug fix 'SPARK-27248', the recreated cache name will be changed to
          // 'default.cachedTable', instead of 'cachedTable'
          activateDatabase(db) {
            sql("REFRESH TABLE default.cachedTable")
            assertCached(
              sql("SELECT * FROM default.cachedTable"), "cachedTable", DISK_ONLY)
            assert(spark.catalog.isCached("default.cachedTable"),
              "Table 'cachedTable' should be cached after refreshing with its qualified name.")
          }
        }
      }
    }
  }

  test("SPARK-33963: do not use table stats while looking in table cache") {
    val t = "table_on_test"
    withTable(t) {
      sql(s"CREATE TABLE $t (col int)")
      assert(!spark.catalog.isCached(t))
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
    }
  }

  test("SPARK-33965: cache table in spark_catalog") {
    withNamespace("spark_catalog.ns") {
      sql("CREATE NAMESPACE spark_catalog.ns")
      val t = "spark_catalog.ns.tbl"
      withTable(t) {
        sql(s"CREATE TABLE $t (col int)")
        assert(!spark.catalog.isCached(t))
        sql(s"CACHE TABLE $t")
        assert(spark.catalog.isCached(t))
      }
    }
  }

  test("SPARK-34076: should be able to drop temp view with cached tables") {
    val t = "cachedTable"
    val v = "tempView"
    withTable(t) {
      withTempView(v) {
        sql(s"CREATE TEMPORARY VIEW $v AS SELECT key FROM src LIMIT 10")
        sql(s"CREATE TABLE $t AS SELECT * FROM src")
        sql(s"CACHE TABLE $t")
      }
    }
  }

  test("SPARK-34076: should be able to drop global temp view with cached tables") {
    val t = "cachedTable"
    val v = "globalTempView"
    withTable(t) {
      withGlobalTempView(v) {
        sql(s"CREATE GLOBAL TEMPORARY VIEW $v AS SELECT key FROM src LIMIT 10")
        sql(s"CREATE TABLE $t AS SELECT * FROM src")
        sql(s"CACHE TABLE $t")
      }
    }
  }

  private def getPartitionLocation(t: String, partition: String): String = {
    val information = sql(s"SHOW TABLE EXTENDED LIKE '$t' PARTITION ($partition)")
      .select("information")
      .first().getString(0)
    information
      .split("\\r?\\n")
      .filter(_.startsWith("Location:"))
      .head
      .replace("Location: file:", "")
  }

  test("SPARK-34213: LOAD DATA refreshes cached table") {
    withTable("src_tbl") {
      withTable("dst_tbl") {
        sql("CREATE TABLE src_tbl (c0 int, part int) USING hive PARTITIONED BY (part)")
        sql("INSERT INTO src_tbl PARTITION (part=0) SELECT 0")
        sql("CREATE TABLE dst_tbl (c0 int, part int) USING hive PARTITIONED BY (part)")
        sql("INSERT INTO dst_tbl PARTITION (part=1) SELECT 1")
        sql("CACHE TABLE dst_tbl")
        assert(spark.catalog.isCached("dst_tbl"))
        checkAnswer(sql("SELECT * FROM dst_tbl"), Row(1, 1))
        val location = getPartitionLocation("src_tbl", "part=0")
        sql(s"LOAD DATA LOCAL INPATH '$location' INTO TABLE dst_tbl PARTITION (part=0)")
        assert(spark.catalog.isCached("dst_tbl"))
        checkAnswer(sql("SELECT * FROM dst_tbl"), Seq(Row(0, 0), Row(1, 1)))
      }
    }
  }

  test("SPARK-34262: ALTER TABLE .. SET LOCATION refreshes cached table") {
    withTable("src_tbl") {
      withTable("dst_tbl") {
        sql("CREATE TABLE src_tbl (c0 int, part int) USING hive PARTITIONED BY (part)")
        sql("INSERT INTO src_tbl PARTITION (part=0) SELECT 0")
        sql("CREATE TABLE dst_tbl (c0 int, part int) USING hive PARTITIONED BY (part)")
        sql("ALTER TABLE dst_tbl ADD PARTITION (part=0)")
        sql("INSERT INTO dst_tbl PARTITION (part=1) SELECT 1")
        sql("CACHE TABLE dst_tbl")
        assert(spark.catalog.isCached("dst_tbl"))
        checkAnswer(sql("SELECT * FROM dst_tbl"), Row(1, 1))
        val location = getPartitionLocation("src_tbl", "part=0")
        sql(s"ALTER TABLE dst_tbl PARTITION (part=0) SET LOCATION '$location'")
        assert(spark.catalog.isCached("dst_tbl"))
        checkAnswer(sql("SELECT * FROM dst_tbl"), Seq(Row(0, 0), Row(1, 1)))
      }
    }
  }
}
