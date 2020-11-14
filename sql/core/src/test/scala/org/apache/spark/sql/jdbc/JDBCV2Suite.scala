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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2Suite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("CREATE SCHEMA \"test\"").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"empty_table\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"people\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('fred', 1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('mary', 2)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"employee\" (dept INTEGER, name TEXT(32), salary INTEGER," +
          " bonus INTEGER)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300)")
        .executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("simple scan") {
    checkAnswer(sql("SELECT * FROM h2.test.empty_table"), Seq())
    checkAnswer(sql("SELECT * FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
    checkAnswer(sql("SELECT name, id FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
  }

  test("scan with filter push-down") {
    val df = spark.table("h2.test.people").filter($"id" > 1)
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
    checkAnswer(df, Row("mary", 2))
  }

  test("scan with column pruning") {
    val df = spark.table("h2.test.people").select("id")
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(Seq("ID")))
    checkAnswer(df, Seq(Row(1), Row(2)))
  }

  test("scan with filter push-down and column pruning") {
    val df = spark.table("h2.test.people").filter($"id" > 1).select("name")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(Seq("NAME")))
    checkAnswer(df, Row("mary"))
  }

  test("scan with aggregate push-down") {
    val df1 = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT")
    // df1.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('MAX('SALARY), None), unresolvedalias('MIN('BONUS), None)]
    // +- 'Filter ('dept > 0)
    //    +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // max(SALARY): int, min(BONUS): int
    // Aggregate [DEPT#0], [max(SALARY#2) AS max(SALARY)#6, min(BONUS#3) AS min(BONUS)#7]
    // +- Filter (dept#0 > 0)
    //    +- SubqueryAlias h2.test.employee
    //       +- RelationV2[DEPT#0, NAME#1, SALARY#2, BONUS#3] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#0], [max(max(SALARY)#13) AS max(SALARY)#6, min(min(BONUS)#14) AS min(BONUS)#7]
    // +- RelationV2[DEPT#0, max(SALARY)#13, min(BONUS)#14] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[DEPT#0], functions=[max(max(SALARY)#13), min(min(BONUS)#14)], output=[max(SALARY)#6, min(BONUS)#7])
    // +- Exchange hashpartitioning(DEPT#0, 5), true, [id=#10]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@3d9f0a5 [DEPT#0,max(SALARY)#13,min(BONUS)#14] PushedAggregates: [*Max(SALARY,false,None), *Min(BONUS,false,None)], PushedFilters: [IsNotNull(dept), GreaterThan(dept,0)], PushedGroupby: [*DEPT], ReadSchema: struct<DEPT:int,max(SALARY):int,min(BONUS):int>// scalastyle:on line.size.limit
    //
    // df1.show
    // +-----------+----------+
    // |max(SALARY)|min(BONUS)|
    // +-----------+----------+
    // |      10000|      1000|
    // |      12000|      1200|
    // +-----------+----------+
    checkAnswer(df1, Seq(Row(10000, 1000), Row(12000, 1200)))

    val df2 = sql("select MAX(ID), MIN(ID) FROM h2.test.people where id > 0")
    // df2.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('MAX('ID), None), unresolvedalias('MIN('ID), None)]
    // +- 'Filter ('id > 0)
    //    +- 'UnresolvedRelation [h2, test, people], []
    //
    // == Analyzed Logical Plan ==
    // max(ID): int, min(ID): int
    // Aggregate [max(ID#29) AS max(ID)#32, min(ID#29) AS min(ID)#33]
    // +- Filter (id#29 > 0)
    //    +- SubqueryAlias h2.test.people
    //       +- RelationV2[NAME#28, ID#29] test.people
    //
    // == Optimized Logical Plan ==
    // Aggregate [max(max(ID)#37) AS max(ID)#32, min(min(ID)#38) AS min(ID)#33]
    // +- RelationV2[max(ID)#37, min(ID)#38] test.people
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[max(max(ID)#37), min(min(ID)#38)], output=[max(ID)#32, min(ID)#33])
    // +- Exchange SinglePartition, true, [id=#44]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@5ed31735 [max(ID)#37,min(ID)#38] PushedAggregates: [*Max(ID,false,None), *Min(ID,false,None)], PushedFilters: [IsNotNull(id), GreaterThan(id,0)], PushedGroupby: [], ReadSchema: struct<max(ID):int,min(ID):int>
    // scalastyle:on line.size.limit

    //  df2.show()
    // +-------+-------+
    // |max(ID)|min(ID)|
    // +-------+-------+
    // |      2|      1|
    // +-------+-------+
    checkAnswer(df2, Seq(Row(2, 1)))

    val df3 = sql("select AVG(ID) FROM h2.test.people where id > 0")
    checkAnswer(df3, Seq(Row(1.0)))

    val df4 = sql("select MAX(SALARY) + 1 FROM h2.test.employee")
    df4.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias(('MAX('SALARY) + 1), None)]
    // +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // (max(SALARY) + 1): int
    // Aggregate [(max(SALARY#68) + 1) AS (max(SALARY) + 1)#71]
    // +- SubqueryAlias h2.test.employee
    //    +- RelationV2[DEPT#66, NAME#67, SALARY#68, BONUS#69] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [(max((max(SALARY) + 1)#74) + 1) AS (max(SALARY) + 1)#71]
    // +- RelationV2[(max(SALARY) + 1)#74] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[max((max(SALARY) + 1)#74)], output=[(max(SALARY) + 1)#71])
    // +- Exchange SinglePartition, true, [id=#112]
    //    +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@20864cd1 [(max(SALARY) + 1)#74] PushedAggregates: [*Max(SALARY,false,None)], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<(max(SALARY) + 1):int>
    // scalastyle:on line.size.limit
    checkAnswer(df4, Seq(Row(12001)))

    // COUNT push down is not supported yet
    val df5 = sql("select COUNT(*) FROM h2.test.employee")
    // df5.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('COUNT(1), None)]
    // +- 'UnresolvedRelation [h2, test, employee], []
    //
    // == Analyzed Logical Plan ==
    // count(1): bigint
    // Aggregate [count(1) AS count(1)#87L]
    // +- SubqueryAlias h2.test.employee
    //    +- RelationV2[DEPT#82, NAME#83, SALARY#84, BONUS#85] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [count(1) AS count(1)#87L]
    // +- RelationV2[] test.employee
    //
    // == Physical Plan ==
    // *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#87L])
    // +- Exchange SinglePartition, true, [id=#149]
    //    +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#90L])
    //       +- *(1) Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@63262071 [] PushedAggregates: [], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<>
    // scalastyle:on line.size.limit
    checkAnswer(df5, Seq(Row(4)))

    val df6 = sql("select MIN(SALARY), MIN(BONUS), MIN(SALARY) * MIN(BONUS) FROM h2.test.employee")
    // df6.explain(true)
    checkAnswer(df6, Seq(Row(9000, 1000, 9000000)))
  }

  test("read/write with partition info") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      val df1 = Seq(("evan", 3), ("cathy", 4), ("alex", 5)).toDF("NAME", "ID")
      val e = intercept[IllegalArgumentException] {
        df1.write
          .option("partitionColumn", "id")
          .option("lowerBound", "0")
          .option("upperBound", "3")
          .option("numPartitions", "0")
          .insertInto("h2.test.abc")
      }.getMessage
      assert(e.contains("Invalid value `0` for parameter `numPartitions` in table writing " +
        "via JDBC. The minimum value is 1."))

      df1.write
        .option("partitionColumn", "id")
        .option("lowerBound", "0")
        .option("upperBound", "3")
        .option("numPartitions", "3")
        .insertInto("h2.test.abc")

      val df2 = spark.read
        .option("partitionColumn", "id")
        .option("lowerBound", "0")
        .option("upperBound", "3")
        .option("numPartitions", "2")
        .table("h2.test.abc")

      assert(df2.rdd.getNumPartitions === 2)
      assert(df2.count() === 5)
    }
  }

  test("show tables") {
    checkAnswer(sql("SHOW TABLES IN h2.test"),
      Seq(Row("test", "people", false), Row("test", "empty_table", false),
        Row("test", "employee", false)))
  }

  test("SQL API: create table as select") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("DataFrameWriterV2: create table as select") {
    withTable("h2.test.abc") {
      spark.table("h2.test.people").writeTo("h2.test.abc").create()
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("SQL API: replace table as select") {
    withTable("h2.test.abc") {
      intercept[CannotReplaceMissingTableException] {
        sql("REPLACE TABLE h2.test.abc AS SELECT 1 as col")
      }
      sql("CREATE OR REPLACE TABLE h2.test.abc AS SELECT 1 as col")
      checkAnswer(sql("SELECT col FROM h2.test.abc"), Row(1))
      sql("REPLACE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("DataFrameWriterV2: replace table as select") {
    withTable("h2.test.abc") {
      intercept[CannotReplaceMissingTableException] {
        sql("SELECT 1 AS col").writeTo("h2.test.abc").replace()
      }
      sql("SELECT 1 AS col").writeTo("h2.test.abc").createOrReplace()
      checkAnswer(sql("SELECT col FROM h2.test.abc"), Row(1))
      spark.table("h2.test.people").writeTo("h2.test.abc").replace()
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("SQL API: insert and overwrite") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")

      sql("INSERT INTO h2.test.abc SELECT 'lucy', 3")
      checkAnswer(
        sql("SELECT name, id FROM h2.test.abc"),
        Seq(Row("fred", 1), Row("mary", 2), Row("lucy", 3)))

      sql("INSERT OVERWRITE h2.test.abc SELECT 'bob', 4")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Row("bob", 4))
    }
  }

  test("DataFrameWriterV2: insert and overwrite") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")

      // `DataFrameWriterV2` is by-name.
      sql("SELECT 3 AS ID, 'lucy' AS NAME").writeTo("h2.test.abc").append()
      checkAnswer(
        sql("SELECT name, id FROM h2.test.abc"),
        Seq(Row("fred", 1), Row("mary", 2), Row("lucy", 3)))

      sql("SELECT 'bob' AS NAME, 4 AS ID").writeTo("h2.test.abc").overwrite(lit(true))
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Row("bob", 4))
    }
  }
}
