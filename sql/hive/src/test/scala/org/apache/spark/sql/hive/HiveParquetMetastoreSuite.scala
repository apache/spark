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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

/**
 * A suite to test the automatic conversion of metastore tables with parquet data to use the
 * built in parquet support.
 */
class HiveParquetMetastoreSuite extends ParquetPartitioningTest {
  import hiveContext._
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet",
      "jt",
      "jt_array",
      "test_parquet")
    sql(
      s"""
        |create external table partitioned_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |PARTITIONED BY (p int)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |location '${partitionedTableDir.toURI}'
      """.stripMargin)

    sql(
      s"""
        |create external table partitioned_parquet_with_key
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |PARTITIONED BY (p int)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |location '${partitionedTableDirWithKey.toURI}'
      """.stripMargin)

    sql(
      s"""
        |create external table normal_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |location '${new File(normalTableDir, "normal").toURI}'
      """.stripMargin)

    sql(
      s"""
        |CREATE EXTERNAL TABLE partitioned_parquet_with_complextypes
        |(
        |  intField INT,
        |  stringField STRING,
        |  structField STRUCT<intStructField: INT, stringStructField: STRING>,
        |  arrayField ARRAY<INT>
        |)
        |PARTITIONED BY (p int)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |LOCATION '${partitionedTableDirWithComplexTypes.toURI}'
      """.stripMargin)

    sql(
      s"""
        |CREATE EXTERNAL TABLE partitioned_parquet_with_key_and_complextypes
        |(
        |  intField INT,
        |  stringField STRING,
        |  structField STRUCT<intStructField: INT, stringStructField: STRING>,
        |  arrayField ARRAY<INT>
        |)
        |PARTITIONED BY (p int)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        | STORED AS
        | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |LOCATION '${partitionedTableDirWithKeyAndComplexTypes.toURI}'
      """.stripMargin)

    sql(
      """
        |create table test_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_key ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_key_and_complextypes ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_parquet_with_complextypes ADD PARTITION (p=$p)")
    }

    (1 to 10).map(i => (i, s"str$i")).toDF("a", "b").createOrReplaceTempView("jt")
    (1 to 10).map(i => Tuple1(Seq(Integer.valueOf(i), null))).toDF("a")
      .createOrReplaceTempView("jt_array")

    assert(spark.conf.get(HiveUtils.CONVERT_METASTORE_PARQUET.key) == "true")
  }

  override def afterAll(): Unit = {
    try {
      dropTables("partitioned_parquet",
        "partitioned_parquet_with_key",
        "partitioned_parquet_with_complextypes",
        "partitioned_parquet_with_key_and_complextypes",
        "normal_parquet",
        "jt",
        "jt_array",
        "test_parquet")
    } finally {
      super.afterAll()
    }
  }

  test(s"conversion is working") {
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.sparkPlan.collect {
        case _: HiveTableScanExec => true
      }.isEmpty)
    assert(
      sql("SELECT * FROM normal_parquet").queryExecution.sparkPlan.collect {
        case _: DataSourceScanExec => true
      }.nonEmpty)
  }

  test("scan an empty parquet table") {
    checkAnswer(sql("SELECT count(*) FROM test_parquet"), Row(0))
  }

  test("scan an empty parquet table with upper case") {
    checkAnswer(sql("SELECT count(INTFIELD) FROM TEST_parquet"), Row(0))
  }

  test("insert into an empty parquet table") {
    dropTables("test_insert_parquet")
    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    // Insert into am empty table.
    sql("insert into table test_insert_parquet select a, b from jt where jt.a > 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField < 8"),
      Row(6, "str6") :: Row(7, "str7") :: Nil
    )
    // Insert overwrite.
    sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    dropTables("test_insert_parquet")

    // Create it again.
    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)
    // Insert overwrite an empty table.
    sql("insert overwrite table test_insert_parquet select a, b from jt where jt.a < 5")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet WHERE intField > 2"),
      Row(3, "str3") :: Row(4, "str4") :: Nil
    )
    // Insert into the table.
    sql("insert into table test_insert_parquet select a, b from jt")
    checkAnswer(
      sql(s"SELECT intField, stringField FROM test_insert_parquet"),
      (1 to 10).map(i => Row(i, s"str$i")) ++ (1 to 4).map(i => Row(i, s"str$i"))
    )
    dropTables("test_insert_parquet")
  }

  test("scan a parquet table created through a CTAS statement") {
    withTable("test_parquet_ctas") {
      sql(
        """
          |create table test_parquet_ctas ROW FORMAT
          |SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
          |AS select * from jt
        """.stripMargin)

      checkAnswer(
        sql(s"SELECT a, b FROM test_parquet_ctas WHERE a = 1"),
        Seq(Row(1, "str1"))
      )

      table("test_parquet_ctas").queryExecution.optimizedPlan match {
        case LogicalRelation(_: HadoopFsRelation, _, _, _) => // OK
        case _ => fail(
          "test_parquet_ctas should be converted to " +
            s"${classOf[HadoopFsRelation ].getCanonicalName }")
      }
    }
  }

  test("MetastoreRelation in InsertIntoTable will be converted") {
    withTable("test_insert_parquet") {
      sql(
        """
          |create table test_insert_parquet
          |(
          |  intField INT
          |)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      val df = sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt")
      df.queryExecution.analyzed match {
        case cmd: InsertIntoHadoopFsRelationCommand =>
          assert(cmd.catalogTable.map(_.identifier.table) === Some("test_insert_parquet"))
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[HadoopFsRelation ].getCanonicalName}. However, found a ${o.toString}")
      }

      checkAnswer(
        sql("SELECT intField FROM test_insert_parquet WHERE test_insert_parquet.intField > 5"),
        sql("SELECT a FROM jt WHERE jt.a > 5").collect()
      )
    }
  }

  test("MetastoreRelation in InsertIntoHiveTable will be converted") {
    withTable("test_insert_parquet") {
      sql(
        """
          |create table test_insert_parquet
          |(
          |  int_array array<int>
          |)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      val df = sql("INSERT INTO TABLE test_insert_parquet SELECT a FROM jt_array")
      df.queryExecution.analyzed match {
        case cmd: InsertIntoHadoopFsRelationCommand =>
          assert(cmd.catalogTable.map(_.identifier.table) === Some("test_insert_parquet"))
        case o => fail("test_insert_parquet should be converted to a " +
          s"${classOf[HadoopFsRelation ].getCanonicalName}. However, found a ${o.toString}")
      }

      checkAnswer(
        sql("SELECT int_array FROM test_insert_parquet"),
        sql("SELECT a FROM jt_array").collect()
      )
    }
  }

  test("SPARK-6450 regression test") {
    withTable("ms_convert") {
      sql(
        """CREATE TABLE IF NOT EXISTS ms_convert (key INT)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          |STORED AS
          |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)

      // This shouldn't throw AnalysisException
      val analyzed = sql(
        """SELECT key FROM ms_convert
          |UNION ALL
          |SELECT key FROM ms_convert
        """.stripMargin).queryExecution.analyzed

      assertResult(2) {
        analyzed.collect {
          case r @ LogicalRelation(_: HadoopFsRelation, _, _, _) => r
        }.size
      }
    }
  }

  def collectHadoopFsRelation(df: DataFrame): HadoopFsRelation = {
    val plan = df.queryExecution.analyzed
    plan.collectFirst {
      case LogicalRelation(r: HadoopFsRelation, _, _, _) => r
    }.getOrElse {
      fail(s"Expecting a HadoopFsRelation 2, but got:\n$plan")
    }
  }

  test("SPARK-7749: non-partitioned metastore Parquet table lookup should use cached relation") {
    withTable("nonPartitioned") {
      sql(
        """
          |CREATE TABLE nonPartitioned (
          |  key INT,
          |  value STRING
          |)
          |STORED AS PARQUET
        """.stripMargin)

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("nonPartitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("nonPartitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("SPARK-7749: partitioned metastore Parquet table lookup should use cached relation") {
    withTable("partitioned") {
      sql(
        """
          |CREATE TABLE partitioned (
          |  key INT,
          |  value STRING
          |)
          |PARTITIONED BY (part INT)
          |STORED AS PARQUET
        """.stripMargin)

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("partitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("partitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  test("SPARK-15968: nonempty partitioned metastore Parquet table lookup should use cached " +
    "relation") {
    withTable("partitioned") {
      sql(
        """
          |CREATE TABLE partitioned (
          |  key INT,
          |  value STRING
          |)
          |PARTITIONED BY (part INT)
          |STORED AS PARQUET
        """.stripMargin)
      sql("INSERT INTO TABLE partitioned PARTITION(part=0) SELECT 1 as key, 'one' as value")

      // First lookup fills the cache
      val r1 = collectHadoopFsRelation(table("partitioned"))
      // Second lookup should reuse the cache
      val r2 = collectHadoopFsRelation(table("partitioned"))
      // They should be the same instance
      assert(r1 eq r2)
    }
  }

  private def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    sessionState.catalog.asInstanceOf[HiveSessionCatalog].metastoreCatalog
      .getCachedDataSourceTable(table)
  }

  test("Caching converted data source Parquet Relations") {
    def checkCached(tableIdentifier: TableIdentifier): Unit = {
      // Converted test_parquet should be cached.
      getCachedDataSourceTable(tableIdentifier) match {
        case null => fail(s"Converted ${tableIdentifier.table} should be cached in the cache.")
        case LogicalRelation(_: HadoopFsRelation, _, _, _) => // OK
        case other =>
          fail(
            "The cached test_parquet should be a Parquet Relation. " +
              s"However, $other is returned form the cache.")
      }
    }

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")

    sql(
      """
        |create table test_insert_parquet
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    var tableIdentifier = TableIdentifier("test_insert_parquet", Some("default"))

    // First, make sure the converted test_parquet is not cached.
    assert(getCachedDataSourceTable(tableIdentifier) === null)
    // Table lookup will make the table cached.
    table("test_insert_parquet")
    checkCached(tableIdentifier)
    // For insert into non-partitioned table, we will do the conversion,
    // so the converted test_insert_parquet should be cached.
    spark.catalog.refreshTable("test_insert_parquet")
    assert(getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_insert_parquet
        |select a, b from jt
      """.stripMargin)
    assert(getCachedDataSourceTable(tableIdentifier) === null)
    // Make sure we can read the data.
    checkAnswer(
      sql("select * from test_insert_parquet"),
      sql("select a, b from jt").collect())
    // Invalidate the cache.
    spark.catalog.refreshTable("test_insert_parquet")
    assert(getCachedDataSourceTable(tableIdentifier) === null)

    // Create a partitioned table.
    sql(
      """
        |create table test_parquet_partitioned_cache_test
        |(
        |  intField INT,
        |  stringField STRING
        |)
        |PARTITIONED BY (`date` string)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      """.stripMargin)

    tableIdentifier = TableIdentifier("test_parquet_partitioned_cache_test", Some("default"))
    assert(getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-01')
        |select a, b from jt
      """.stripMargin)
    // Right now, insert into a partitioned data source Parquet table. We refreshed the table.
    // So, we expect it is not cached.
    assert(getCachedDataSourceTable(tableIdentifier) === null)
    sql(
      """
        |INSERT INTO TABLE test_parquet_partitioned_cache_test
        |PARTITION (`date`='2015-04-02')
        |select a, b from jt
      """.stripMargin)
    assert(getCachedDataSourceTable(tableIdentifier) === null)

    // Make sure we can cache the partitioned table.
    table("test_parquet_partitioned_cache_test")
    checkCached(tableIdentifier)
    // Make sure we can read the data.
    checkAnswer(
      sql("select STRINGField, `date`, intField from test_parquet_partitioned_cache_test"),
      sql(
        """
          |select b, '2015-04-01', a FROM jt
          |UNION ALL
          |select b, '2015-04-02', a FROM jt
        """.stripMargin).collect())

    spark.catalog.refreshTable("test_parquet_partitioned_cache_test")
    assert(getCachedDataSourceTable(tableIdentifier) === null)

    dropTables("test_insert_parquet", "test_parquet_partitioned_cache_test")
  }

  test("SPARK-15248: explicitly added partitions should be readable") {
    withTable("test_added_partitions", "test_temp") {
      withTempDir { src =>
        val partitionDir = new File(src, "partition").toURI
        sql(
          """
            |CREATE TABLE test_added_partitions (a STRING)
            |PARTITIONED BY (b INT)
            |STORED AS PARQUET
          """.stripMargin)

        // Temp view that is used to insert data into partitioned table
        Seq("foo", "bar").toDF("a").createOrReplaceTempView("test_temp")
        sql("INSERT INTO test_added_partitions PARTITION(b='0') SELECT a FROM test_temp")

        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(Row("foo", 0), Row("bar", 0)))

        // Create partition without data files and check whether it can be read
        sql(s"ALTER TABLE test_added_partitions ADD PARTITION (b='1') LOCATION '$partitionDir'")
        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(Row("foo", 0), Row("bar", 0)))

        // Add data files to partition directory and check whether they can be read
        sql("INSERT INTO TABLE test_added_partitions PARTITION (b=1) select 'baz' as a")
        checkAnswer(
          sql("SELECT * FROM test_added_partitions"),
          Seq(Row("foo", 0), Row("bar", 0), Row("baz", 1)))

        // Check it with pruning predicates
        checkAnswer(
          sql("SELECT * FROM test_added_partitions where b = 0"),
          Seq(Row("foo", 0), Row("bar", 0)))
        checkAnswer(
          sql("SELECT * FROM test_added_partitions where b = 1"),
          Seq(Row("baz", 1)))
        checkAnswer(
          sql("SELECT * FROM test_added_partitions where b = 2"),
          Seq.empty)

        // Also verify the inputFiles implementation
        assert(sql("select * from test_added_partitions").inputFiles.length == 2)
        assert(sql("select * from test_added_partitions where b = 0").inputFiles.length == 1)
        assert(sql("select * from test_added_partitions where b = 1").inputFiles.length == 1)
        assert(sql("select * from test_added_partitions where b = 2").inputFiles.length == 0)
      }
    }
  }

  test("Explicitly added partitions should be readable after load") {
    withTable("test_added_partitions") {
      withTempDir { src =>
        val newPartitionDir = src.toURI.toString
        spark.range(2).selectExpr("cast(id as string)").toDF("a").write
          .mode("overwrite")
          .parquet(newPartitionDir)

        sql(
          """
            |CREATE TABLE test_added_partitions (a STRING)
            |PARTITIONED BY (b INT)
            |STORED AS PARQUET
          """.stripMargin)

        // Create partition without data files and check whether it can be read
        sql(s"ALTER TABLE test_added_partitions ADD PARTITION (b='1')")
        // This table fetch is to fill the cache with zero leaf files
        checkAnswer(spark.table("test_added_partitions"), Seq.empty)

        sql(
          s"""
             |LOAD DATA LOCAL INPATH '$newPartitionDir' OVERWRITE
             |INTO TABLE test_added_partitions PARTITION(b='1')
           """.stripMargin)

        checkAnswer(
          spark.table("test_added_partitions"),
          Seq(Row("0", 1), Row("1", 1)))
      }
    }
  }

  test("Non-partitioned table readable after load") {
    withTable("tab") {
      withTempDir { src =>
        val newPartitionDir = src.toURI.toString
        spark.range(2).selectExpr("cast(id as string)").toDF("a").write
          .mode("overwrite")
          .parquet(newPartitionDir)

        sql("CREATE TABLE tab (a STRING) STORED AS PARQUET")

        // This table fetch is to fill the cache with zero leaf files
        checkAnswer(spark.table("tab"), Seq.empty)

        sql(
          s"""
             |LOAD DATA LOCAL INPATH '$newPartitionDir' OVERWRITE
             |INTO TABLE tab
           """.stripMargin)

        checkAnswer(spark.table("tab"), Seq(Row("0"), Row("1")))
      }
    }
  }

  test("self-join") {
    val table = spark.table("normal_parquet")
    val selfJoin = table.as("t1").crossJoin(table.as("t2"))
    checkAnswer(selfJoin,
      sql("SELECT * FROM normal_parquet x CROSS JOIN normal_parquet y"))
  }
}
