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

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{CharVarcharTestSuite, QueryTest, Row}
import org.apache.spark.sql.execution.command.CharVarcharDDLTestBase
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class HiveCharVarcharTestSuite extends CharVarcharTestSuite with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }

  test("SPARK-33892: SHOW CREATE TABLE AS SERDE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      val rest = sql("SHOW CREATE TABLE t AS SERDE").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }

  test("SPARK-35700: Read char/varchar orc table with created and written by external systems") {
    withTable("t") {
      hiveClient.runSqlHive("CREATE TABLE t(c CHAR(5), v VARCHAR(7)) STORED AS ORC")
      hiveClient.runSqlHive("INSERT INTO t VALUES('Spark', 'kyuubi')")
      checkAnswer(sql("SELECT c, v from t"), Row("Spark", "kyuubi"))
      checkAnswer(sql("SELECT v from t where c = 'Spark' and v = 'kyuubi'"), Row("kyuubi"))
    }
  }

  test("SPARK-36552: Fix different behavior of writing char/varchar to hive and datasource table") {
    Seq("true", "false").foreach { v =>
      withSQLConf(
        "spark.sql.hive.convertMetastoreParquet" -> v,
        "spark.sql.legacy.charVarcharAsString" -> "true") {
        withTable("t") {
          sql(s"CREATE TABLE t (c varchar(2)) USING $format")
          sql("INSERT INTO t SELECT 'kyuubi'")
          checkAnswer(sql("SELECT c from t"), Row("kyuubi"))
        }
      }
    }
  }

  test("char/varchar type values length check: partitioned columns of other types") {
    val tableName = "t"
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName(i STRING, c $typ) USING $format PARTITIONED BY (c)")
        Seq(1, 10, 100, 1000, 10000).foreach { v =>
          sql(s"INSERT OVERWRITE $tableName VALUES ('1', $v)")
          checkPlainResult(spark.table(tableName), typ, v.toString)
          sql(s"ALTER TABLE $tableName DROP PARTITION(c=$v)")
          checkAnswer(spark.table(tableName), Nil)
        }
        assertLengthCheckFailure(s"INSERT OVERWRITE $tableName VALUES ('1', 100000)")
        assertLengthCheckFailure("ALTER TABLE t DROP PARTITION(c=100000)")
      }
    }
  }

  test("SPARK-59001: CHAR/VARCHAR partition filters prune client-side") {
    // Keep the relation a HiveTableRelation, otherwise the scan is converted to a file index
    // and never reaches HiveShim's metastore filter conversion. Hive convertFilters still
    // skips CHAR/VARCHAR keys; pruning is client-side under standardSemantics.
    withSQLConf(
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true",
        SQLConf.HIVE_METASTORE_PARTITION_PRUNING.key -> "true",
        HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
      val partitionValues = Seq("a", "b", "c", "d", "e")

      def withHivePartTable(partitionType: String)(body: => Unit): Unit = {
        withTable("std_hive_part") {
          sql(
            s"""CREATE TABLE std_hive_part (i INT, p $partitionType)
               |USING $format PARTITIONED BY (p)""".stripMargin)
          partitionValues.foreach { v =>
            sql(s"INSERT INTO std_hive_part PARTITION (p='$v') VALUES (1)")
          }
          body
        }
      }

      withHivePartTable("CHAR(5)") {
        HiveCatalogMetrics.reset()
        // Store assignment pads CHAR(5); compare is not PAD SPACE, so the literal must match.
        // checkToRDD = false: checkAnswer would otherwise scan twice and double the metric.
        QueryTest.checkAnswer(
          sql("SELECT i FROM std_hive_part WHERE p = 'a    '"),
          Seq(Row(1)),
          checkToRDD = false)
        assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount === 1)
        HiveCatalogMetrics.reset()
        QueryTest.checkAnswer(
          sql("SELECT i FROM std_hive_part WHERE p = 'a'"),
          Nil,
          checkToRDD = false)
        assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount === 0)
      }

      withHivePartTable("VARCHAR(5)") {
        HiveCatalogMetrics.reset()
        QueryTest.checkAnswer(
          sql("SELECT i FROM std_hive_part WHERE p = 'a'"),
          Seq(Row(1)),
          checkToRDD = false)
        assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount === 1)
      }

      // A supported conjunct must not bypass client-side pruning of the CHAR predicate.
      withTable("std_hive_part") {
        sql(
          s"""CREATE TABLE std_hive_part (i INT, ds INT, p CHAR(5))
             |USING $format PARTITIONED BY (ds, p)""".stripMargin)
        partitionValues.foreach { v =>
          sql(s"INSERT INTO std_hive_part PARTITION (ds=1, p='$v') VALUES (1)")
        }
        HiveCatalogMetrics.reset()
        QueryTest.checkAnswer(
          sql("SELECT i FROM std_hive_part WHERE ds = 1 AND p = 'a    '"),
          Seq(Row(1)),
          checkToRDD = false)
        assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount === 1)
      }
    }
  }
}

class HiveCharVarcharDDLTestSuite extends CharVarcharDDLTestBase with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  override def getTableName(name: String): String = s"`spark_catalog`.`default`.`$name`"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }
}
