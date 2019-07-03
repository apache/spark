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

package org.apache.spark.sql.sources.v2

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, Identifier}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StringType, StructType}

class DataSourceV2SQLSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  private def getTestCatalog() = {
    spark.sessionState.catalog.catalogManager.getCatalog("testcat")
      .asInstanceOf[TestInMemoryTableCatalog]
  }

  after {
    getTestCatalog().clearTables()
    spark.sql("DROP TABLE source")
    spark.sql("DROP TABLE source2")
  }

  test("CreateTable: basic") {
    def checkTestCatalog(sql: String): Unit = {
      spark.sql(sql)
      val testCatalog = getTestCatalog()
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

      assert(table.name == "testcat.table_name")
      assert(table.partitioning.isEmpty)
      assert(table.properties == Map("provider" -> "foo").asJava)
      assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

      val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
      checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
    }

    withClue("table identifier specifies catalog") {
      withTable("testcat.table_name") {
        checkTestCatalog("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")
      }
    }

    withClue("table identifier doesn't specify catalog") {
      // This would create table in ExternalCatalog.
      val e = intercept[Exception] {
        spark.sql("CREATE TABLE table_name (id bigint, data string) USING foo")
      }
      assert(e.getMessage.contains("Failed to find data source: foo"))

      withTable("table_name") {
        spark.sql("CREATE TABLE table_name (id bigint, data string) USING json")
        intercept[NoSuchTableException] {
          getTestCatalog().loadTable(Identifier.of(Array(), "table_name"))
        }
        val table = spark.sharedState.externalCatalog.getTable("default", "table_name")
        assert(table.identifier == TableIdentifier("table_name", Some("default")))
      }
    }

    withClue("table identifier doesn't specify catalog and has more than 2 name parts") {
      // Spark tries to fallback to ExternalCatalog, which can't handle 3-part table name.
      val e = intercept[AnalysisException] {
        spark.sql("CREATE TABLE ns1.ns2.table_name (id bigint, data string) USING json")
      }
      assert(e.message.contains("No catalog specified for table ns1.ns2.table_name"))
      assert(e.message.contains("no default catalog is set"))
    }

    withClue("table identifier doesn't specify catalog but default catalog is set") {
      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        withTable("table_name") {
          checkTestCatalog("CREATE TABLE table_name (id bigint, data string) USING foo")
        }
      }
    }
  }

  test("CreateTable: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = getTestCatalog()

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // run a second create query that should fail
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql("CREATE TABLE testcat.table_name (id bigint, data string, id2 bigint) USING bar")
    }

    assert(exc.getMessage.contains("table_name"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = getTestCatalog()
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    spark.sql("CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING bar")

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq.empty)
  }

  test("CreateTable: fail analysis when default catalog is needed but missing") {
    val exc = intercept[AnalysisException] {
      spark.sql(s"CREATE TABLE a.b.c USING foo")
    }

    assert(exc.getMessage.contains("No catalog specified for table"))
    assert(exc.getMessage.contains("a.b.c"))
    assert(exc.getMessage.contains("no default catalog is set"))
  }

  test("CreateTableAsSelect: basic") {
    def checkTestCatalog(sql: String): Unit = {
      spark.sql(sql)
      val testCatalog = getTestCatalog()
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

      assert(table.name == "testcat.table_name")
      assert(table.partitioning.isEmpty)
      assert(table.properties == Map("provider" -> "foo").asJava)
      assert(table.schema == new StructType().add("a", StringType, false))

      val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
      checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Row("1"))
    }

    withClue("table identifier specifies catalog") {
      withTable("testcat.table_name") {
        checkTestCatalog("CREATE TABLE testcat.table_name USING foo AS SELECT '1' AS a")
      }
    }

    withClue("table identifier doesn't specify catalog") {
      // This would create table in ExternalCatalog.
      val e = intercept[Exception] {
        spark.sql("CREATE TABLE table_name USING foo AS SELECT '1' AS a")
      }
      assert(e.getMessage.contains("Failed to find data source: foo"))

      withTable("table_name") {
        spark.sql("CREATE TABLE table_name USING json AS SELECT '1' AS a")
        intercept[NoSuchTableException] {
          getTestCatalog().loadTable(Identifier.of(Array(), "table_name"))
        }
        val table = spark.sharedState.externalCatalog.getTable("default", "table_name")
        assert(table.schema == new StructType().add("a", StringType))
      }
    }

    withClue("table identifier doesn't specify catalog and has more than 2 name parts") {
      // Spark tries to fallback to ExternalCatalog, which can't handle 3-part table name.
      val e = intercept[AnalysisException] {
        spark.sql("CREATE TABLE ns1.ns2.table_name USING foo AS SELECT '1' AS a")
      }
      assert(e.message.contains("No catalog specified for table ns1.ns2.table_name"))
      assert(e.message.contains("no default catalog is set"))
    }

    withClue("table identifier doesn't specify catalog but default catalog is set") {
      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        withTable("table_name") {
          checkTestCatalog("CREATE TABLE table_name USING foo AS SELECT '1' AS a")
        }
      }
    }
  }

  test("CreateTableAsSelect: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = getTestCatalog()

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    // run a second CTAS query that should fail
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql(
        "CREATE TABLE testcat.table_name USING bar AS SELECT id, data, id as id2 FROM source2")
    }

    assert(exc.getMessage.contains("table_name"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = getTestCatalog()
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source2")

    // check that the table contains data from just the first CTAS
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: fail analysis when default catalog is needed but missing") {
    val exc = intercept[AnalysisException] {
      spark.sql(s"CREATE TABLE a.b.c USING foo AS SELECT id, data FROM source")
    }

    assert(exc.getMessage.contains("No catalog specified for table"))
    assert(exc.getMessage.contains("a.b.c"))
    assert(exc.getMessage.contains("no default catalog is set"))
  }

  test("DropTable: basic") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    sql(s"CREATE TABLE $tableName USING foo AS SELECT id, data FROM source")
    assert(getTestCatalog().tableExists(ident) === true)
    sql(s"DROP TABLE $tableName")
    assert(getTestCatalog().tableExists(ident) === false)
  }

  test("DropTable: if exists") {
    intercept[NoSuchTableException] {
      sql(s"DROP TABLE testcat.db.notbl")
    }
    sql(s"DROP TABLE IF EXISTS testcat.db.notbl")
  }

  test("Relation: basic") {
    def checkTableScan(tblNameParts: Array[String]): Unit = {
      val tblName = tblNameParts.mkString(".")
      withTable(tblName) {
        sql(s"CREATE TABLE $tblName USING json AS SELECT '1' AS a")
        checkAnswer(sql(s"TABLE $tblName"), Row("1"))
        checkAnswer(sql(s"SELECT * FROM $tblName"), Row("1"))
      }
    }

    withClue("table identifier specifies catalog") {
      checkTableScan(Array("testcat", "ns1", "ns2", "tbl"))
    }

    withClue("table identifier doesn't specify catalog") {
      checkTableScan(Array("tbl"))
    }

    withClue("table identifier doesn't specify catalog but default catalog is set") {
      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        checkTableScan(Array("tbl"))
      }
    }
  }

  test("Relation: table not found") {
    withClue("table identifier specifies catalog") {
      val e = intercept[AnalysisException] {
        spark.sql("SELECT * FROM testcat.tbl")
      }
      assert(e.message.contains("Table or view not found: testcat.tbl"))
    }

    withClue("table identifier doesn't specify catalog") {
      val e = intercept[AnalysisException] {
        spark.sql("SELECT * FROM tbl")
      }
      assert(e.message.contains("Table or view not found: tbl"))
    }

    withClue("table identifier doesn't specify catalog and has more than 2 name parts") {
      val e = intercept[CatalogNotFoundException] {
        spark.sql("SELECT * FROM a.b.c")
      }
      assert(e.getMessage.contains("Catalog 'a' plugin class not found"))
    }

    withClue("table identifier doesn't specify catalog but default catalog is set") {
      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        val e = intercept[AnalysisException] {
          spark.sql("SELECT * FROM tbl")
        }
        assert(e.message.contains("Table or view not found: tbl"))
      }
    }
  }

  test("Relation: name conflicts with temp view") {
    withTempView("v") {
      spark.range(10).createOrReplaceTempView("v")

      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        withTable("v") {
          spark.sql("CREATE TABLE v(i INT) USING foo")
          checkAnswer(sql("SELECT * FROM v"), spark.range(10).toDF())
        }
      }
    }
  }

  test("Relation: name conflicts with global temp view") {
    withTempView("v") {
      spark.range(10).createOrReplaceGlobalTempView("v")

      withSQLConf(SQLConf.DEFAULT_V2_CATALOG.key -> "testcat") {
        withTable("global_temp.v") {
          spark.sql("CREATE TABLE global_temp.v(i INT) USING foo")
          checkAnswer(sql("SELECT * FROM global_temp.v"), spark.range(10).toDF())
        }
      }
    }
  }

  test("Relation: SparkSession.table()") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(spark.table(s"$t1"), spark.table("source"))
    }
  }

  test("Relation: CTE") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(
        sql(s"""
          |WITH cte AS (SELECT * FROM $t1)
          |SELECT * FROM cte
        """.stripMargin),
        spark.table("source"))
    }
  }

  test("Relation: view text") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      withView("view1") { v1: String =>
        sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
        sql(s"CREATE VIEW $v1 AS SELECT * from $t1")
        checkAnswer(sql(s"TABLE $v1"), spark.table("source"))
      }
    }
  }

  test("Relation: join tables in 2 catalogs") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.v2tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      sql(s"CREATE TABLE $t2 USING foo AS SELECT id, data FROM source2")
      val df1 = spark.table("source")
      val df2 = spark.table("source2")
      val df_joined = df1.join(df2).where(df1("id") + 1 === df2("id"))
      checkAnswer(
        sql(s"""
          |SELECT *
          |FROM $t1 t1, $t2 t2
          |WHERE t1.id + 1 = t2.id
        """.stripMargin),
        df_joined)
    }
  }
}
