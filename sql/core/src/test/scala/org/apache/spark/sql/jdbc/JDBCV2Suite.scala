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
import org.apache.spark.sql.functions.{avg, lit, sum, udf}
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
    .set("spark.sql.catalog.h2.pushDownAggregate", "true")

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
        "CREATE TABLE \"test\".\"employee\" (dept INTEGER, name TEXT(32), salary NUMERIC(20, 2)," +
          " bonus DOUBLE)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (6, 'jen', 12000, 1200)")
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

  test("aggregate pushdown with alias") {
    val df1 = spark.table("h2.test.employee")
    var query1 = df1.select($"DEPT", $"SALARY".as("value"))
      .groupBy($"DEPT")
      .agg(sum($"value").as("total"))
      .filter($"total" > 1000)
    // query1.explain(true)
    checkAnswer(query1, Seq(Row(1, 19000.00), Row(2, 22000.00), Row(6, 12000)))
    val decrease = udf { (x: Double, y: Double) => x - y}
    var query2 = df1.select($"DEPT", decrease($"SALARY", $"BONUS").as("value"), $"SALARY", $"BONUS")
      .groupBy($"DEPT")
      .agg(sum($"value"), sum($"SALARY"), sum($"BONUS"))
    // query2.explain(true)
    checkAnswer(query2,
      Seq(Row(1, 16800.00, 19000.00, 2200.00), Row(2, 19500.00, 22000.00, 2500.00),
        Row(6, 10800, 12000, 1200)))

    val cols = Seq("a", "b", "c", "d")
    val df2 = sql("select * from h2.test.employee").toDF(cols: _*)
    val df3 = df2.groupBy().sum("c")
    // df3.explain(true)
    checkAnswer(df3, Seq(Row(53000.00)))

    val df4 = df2.groupBy($"a").sum("c")
    checkAnswer(df4, Seq(Row(1, 19000.00), Row(2, 22000.00), Row(6, 12000)))
  }

  test("scan with aggregate push-down") {
    val df1 = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT")
    // df1.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('MAX('SALARY), None), unresolvedalias('MIN('BONUS), None)]
    // +- 'Filter ('dept > 0)
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // max(SALARY): decimal(20,2), min(BONUS): decimal(6,2)
    // Aggregate [DEPT#253], [max(SALARY#255) AS max(SALARY)#259, min(BONUS#256) AS min(BONUS)#260]
    // +- Filter (dept#253 > 0)
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#253, NAME#254, SALARY#255, BONUS#256] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#253], [max(Max(SALARY,DecimalType(20,2))#266) AS max(SALARY)#259, min(Min(BONUS,DecimalType(6,2))#267) AS min(BONUS)#260]
    // +- RelationV2[Max(SALARY,DecimalType(20,2))#266, Min(BONUS,DecimalType(6,2))#267, DEPT#253] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[DEPT#253], functions=[max(Max(SALARY,DecimalType(20,2))#266), min(Min(BONUS,DecimalType(6,2))#267)], output=[max(SALARY)#259, min(BONUS)#260])
    // +- Exchange hashpartitioning(DEPT#253, 5), ENSURE_REQUIREMENTS, [id=#397]
    // +- HashAggregate(keys=[DEPT#253], functions=[partial_max(Max(SALARY,DecimalType(20,2))#266), partial_min(Min(BONUS,DecimalType(6,2))#267)], output=[DEPT#253, max#270, min#271])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@30437e9c [Max(SALARY,DecimalType(20,2))#266,Min(BONUS,DecimalType(6,2))#267,DEPT#253] PushedAggregates: [*Max(SALARY,DecimalType(20,2)), *Min(BONUS,DecimalType(6,2))], PushedFilters: [IsNotNull(dept), GreaterThan(dept,0)], PushedGroupby: [*DEPT], ReadSchema: struct<Max(SALARY,DecimalType(20,2)):decimal(20,2),Min(BONUS,DecimalType(6,2)):decimal(6,2),DEPT:...
    //
    // df1.show
    // +-----------+----------+
    // |max(SALARY)|min(BONUS)|
    // +-----------+----------+
    // |      10000|      1000|
    // |      12000|      1200|
    // |      12000|      1200|
    // +-----------+----------+
    checkAnswer(df1, Seq(Row(10000, 1000), Row(12000, 1200), Row(12000, 1200)))

    val df2 = sql("select MAX(ID), MIN(ID) FROM h2.test.people where id > 0")
    // df2.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('MAX('ID), None), unresolvedalias('MIN('ID), None)]
    // +- 'Filter ('id > 0)
    // +- 'UnresolvedRelation [h2, test, people], [], false
    //
    // == Analyzed Logical Plan ==
    // max(ID): int, min(ID): int
    // Aggregate [max(ID#290) AS max(ID)#293, min(ID#290) AS min(ID)#294]
    // +- Filter (id#290 > 0)
    // +- SubqueryAlias h2.test.people
    // +- RelationV2[NAME#289, ID#290] test.people
    //
    // == Optimized Logical Plan ==
    // Aggregate [max(Max(ID,IntegerType)#298) AS max(ID)#293, min(Min(ID,IntegerType)#299) AS min(ID)#294]
    // +- RelationV2[Max(ID,IntegerType)#298, Min(ID,IntegerType)#299] test.people
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[], functions=[max(Max(ID,IntegerType)#298), min(Min(ID,IntegerType)#299)], output=[max(ID)#293, min(ID)#294])
    // +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#469]
    // +- HashAggregate(keys=[], functions=[partial_max(Max(ID,IntegerType)#298), partial_min(Min(ID,IntegerType)#299)], output=[max#302, min#303])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@1368e2f7 [Max(ID,IntegerType)#298,Min(ID,IntegerType)#299] PushedAggregates: [*Max(ID,IntegerType), *Min(ID,IntegerType)], PushedFilters: [IsNotNull(id), GreaterThan(id,0)], PushedGroupby: [], ReadSchema: struct<Max(ID,IntegerType):int,Min(ID,IntegerType):int>
    // scalastyle:on line.size.limit
    //
    //  df2.show()
    // +-------+-------+
    // |max(ID)|min(ID)|
    // +-------+-------+
    // |      2|      1|
    // +-------+-------+
    checkAnswer(df2, Seq(Row(2, 1)))

    val df3 = sql("select AVG(BONUS) FROM h2.test.employee")
    // df3.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias('AVG('BONUS), None)]
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // avg(BONUS): double
    // Aggregate [avg(BONUS#69) AS avg(BONUS)#71]
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#66, NAME#67, SALARY#68, BONUS#69] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [avg(Avg(BONUS,DoubleType,false)#74) AS avg(BONUS)#71]
    // +- RelationV2[Avg(BONUS,DoubleType,false)#74] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[], functions=[avg(Avg(BONUS,DoubleType,false)#74)], output=[avg(BONUS)#71])
    // +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#143]
    // +- HashAggregate(keys=[], functions=[partial_avg(Avg(BONUS,DoubleType,false)#74)], output=[sum#77, count#78L])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@6a1d1467 [Avg(BONUS,DoubleType,false)#74] PushedAggregates: [*Avg(BONUS,DoubleType,false)], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<Avg(BONUS,DoubleType,false):double>
    // scalastyle:on line.size.limit
    // df3.show(false)
    checkAnswer(df3, Seq(Row(1180.0)))

    val df4 = sql("select MAX(SALARY) + 1 FROM h2.test.employee")
    // df4.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Project [unresolvedalias(('MAX('SALARY) + 1), None)]
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // (max(SALARY) + 1): decimal(21,2)
    // Aggregate [CheckOverflow((promote_precision(cast(max(SALARY#345) as decimal(21,2))) + promote_precision(cast(cast(1 as decimal(1,0)) as decimal(21,2)))), DecimalType(21,2), true) AS (max(SALARY) + 1)#348]
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#343, NAME#344, SALARY#345, BONUS#346] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [CheckOverflow((promote_precision(cast(max(Max(SALARY,DecimalType(20,2))#351) as decimal(21,2))) + 1.00), DecimalType(21,2), true) AS (max(SALARY) + 1)#348]
    // +- RelationV2[Max(SALARY,DecimalType(20,2))#351] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[], functions=[max(Max(SALARY,DecimalType(20,2))#351)], output=[(max(SALARY) + 1)#348])
    // +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#589]
    // +- HashAggregate(keys=[], functions=[partial_max(Max(SALARY,DecimalType(20,2))#351)], output=[max#353])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@453439e [Max(SALARY,DecimalType(20,2))#351] PushedAggregates: [*Max(SALARY,DecimalType(20,2))], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<Max(SALARY,DecimalType(20,2)):decimal(20,2)>
    // scalastyle:on line.size.limit
    checkAnswer(df4, Seq(Row(12001)))

    val df5 = sql("select MIN(SALARY), MIN(BONUS), MIN(SALARY) * MIN(BONUS) FROM h2.test.employee")
    // df5.explain(true)
    checkAnswer(df5, Seq(Row(9000, 1000, 9000000)))

    val df6 = sql("select MIN(salary), MIN(bonus), SUM(SALARY * BONUS) FROM h2.test.employee")
    // df6.explain(true)
    checkAnswer(df6, Seq(Row(9000, 1000, 62600000)))

    val df7 = sql("select BONUS, SUM(SALARY+BONUS), SALARY FROM h2.test.employee" +
      " GROUP BY SALARY, BONUS")
    // df7.explain(true)
    checkAnswer(df7, Seq(Row(1000, 11000, 10000), Row(1200, 26400, 12000),
      Row(1200, 10200, 9000), Row(1300, 11300, 10000)))

    val df8 = spark.table("h2.test.employee")
    val sub2 = udf { (x: String) => x.substring(0, 3) }
    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val df9 = df8.select($"SALARY", $"BONUS", sub2($"NAME").as("nsub2"))
      .filter("SALARY > 100")
      .filter(name($"nsub2"))
      .agg(avg($"SALARY").as("avg_salary"))
    // df9.explain(true)
    checkAnswer(df9, Seq(Row(9666.666667)))

    val df10 = sql("select SUM(SALARY+BONUS*SALARY+SALARY/BONUS), DEPT FROM h2.test.employee" +
      " GROUP BY DEPT")
    // df10.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('SUM((('SALARY + ('BONUS * 'SALARY)) + ('SALARY / 'BONUS))), None), 'DEPT]
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // sum(((SALARY + (BONUS * SALARY)) + (SALARY / BONUS))): decimal(38,9), DEPT: int
    // Aggregate [DEPT#551], [sum(CheckOverflow((promote_precision(cast(CheckOverflow((promote_precision(cast(SALARY#553 as decimal(28,4))) + promote_precision(cast(CheckOverflow((promote_precision(cast(BONUS#554 as decimal(20,2))) * promote_precision(cast(SALARY#553 as decimal(20,2)))), DecimalType(27,4), true) as decimal(28,4)))), DecimalType(28,4), true) as decimal(34,9))) + promote_precision(cast(CheckOverflow((promote_precision(cast(SALARY#553 as decimal(20,2))) / promote_precision(cast(BONUS#554 as decimal(20,2)))), DecimalType(29,9), true) as decimal(34,9)))), DecimalType(34,9), true)) AS sum(((SALARY + (BONUS * SALARY)) + (SALARY / BONUS)))#556, DEPT#551]
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#551, NAME#552, SALARY#553, BONUS#554] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#551], [sum(Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)#562) AS sum(((SALARY + (BONUS * SALARY)) + (SALARY / BONUS)))#556, DEPT#551]
    // +- RelationV2[Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)#562, DEPT#551] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[DEPT#551], functions=[sum(Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)#562)], output=[sum(((SALARY + (BONUS * SALARY)) + (SALARY / BONUS)))#556, DEPT#551])
    // +- Exchange hashpartitioning(DEPT#551, 5), ENSURE_REQUIREMENTS, [id=#917]
    // +- HashAggregate(keys=[DEPT#551], functions=[partial_sum(Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)#562)], output=[DEPT#551, sum#565, isEmpty#566])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@7692c0e9 [Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)#562,DEPT#551] PushedAggregates: [*Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false)], PushedFilters: [], PushedGroupby: [*DEPT], ReadSchema: struct<Sum(SALARY + BONUS * SALARY + SALARY / BONUS,DecimalType(38,9),false):decimal(38,9),DEPT:int>
    // scalastyle:on line.size.limit
    // df10.show(true)
    // +-----------------------------------------------------+----+
    // |sum(((SALARY + (BONUS * SALARY)) + (SALARY / BONUS)))|DEPT|
    // +-----------------------------------------------------+----+
    // |                                   20819017.500000000|   1|
    // |                                   27422017.692307692|   2|
    // |                                   14412010.000000000|   6|
    // +-----------------------------------------------------+----+
    checkAnswer(df10, Seq(Row(20819017.500000000, 1), Row(27422017.692307692, 2),
      Row(14412010.000000000, 6)))

    val df11 = sql("select COUNT(*), DEPT FROM h2.test.employee group by DEPT")
    // df11.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('COUNT(1), None), 'DEPT]
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // count(1): bigint, DEPT: int
    // Aggregate [DEPT#602], [count(1) AS count(1)#607L, DEPT#602]
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#602, NAME#603, SALARY#604, BONUS#605] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#602], [count(Count(1,LongType,false)#611L) AS count(1)#607L, DEPT#602]
    // +- RelationV2[Count(1,LongType,false)#611L, DEPT#602] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[DEPT#602], functions=[count(Count(1,LongType,false)#611L)], output=[count(1)#607L, DEPT#602])
    // +- Exchange hashpartitioning(DEPT#602, 5), ENSURE_REQUIREMENTS, [id=#1029]
    // +- HashAggregate(keys=[DEPT#602], functions=[partial_count(Count(1,LongType,false)#611L)], output=[DEPT#602, count#613L])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@5653429e [Count(1,LongType,false)#611L,DEPT#602] PushedAggregates: [*Count(1,LongType,false)], PushedFilters: [], PushedGroupby: [*DEPT], ReadSchema: struct<Count(1,LongType,false):bigint,DEPT:int>
    // scalastyle:on line.size.limit
    // df11.show(true)
    // +--------+----+
    // |count(1)|DEPT|
    // +--------+----+
    // |       2|   1|
    // |       2|   2|
    // |       1|   6|
    // +--------+----+
    checkAnswer(df11, Seq(Row(2, 1), Row(2, 2), Row(1, 6)))

    val df12 = sql("select COUNT(*) FROM h2.test.employee group by DEPT")
    // df12.explain(true)
    checkAnswer(df12, Seq(Row(2), Row(2), Row(1)))

    val df13 = sql("select COUNT(*) FROM h2.test.employee")
    // df13.explain(true)
    checkAnswer(df13, Seq(Row(5)))

    val df14 = sql("select COUNT(NAME) FROM h2.test.employee group by DEPT")
    // df14.explain(true)
    checkAnswer(df14, Seq(Row(2), Row(2), Row(1)))

    val df15 = sql("select COUNT(NAME) FROM h2.test.employee")
    // df15.explain(true)
    checkAnswer(df15, Seq(Row(5)))

    val df16 = sql("select COUNT(NAME), DEPT FROM h2.test.employee group by DEPT")
    // df16.explain(true)
    checkAnswer(df16, Seq(Row(2, 1), Row(2, 2), Row(1, 6)))

    val df17 = sql("select MAX(SALARY) FILTER (WHERE SALARY > 1000), MIN(BONUS) " +
      "FROM h2.test.employee where dept > 0 group by DEPT")
    // df17.explain(true)
    // scalastyle:off line.size.limit
    // == Parsed Logical Plan ==
    // 'Aggregate ['DEPT], [unresolvedalias('MAX('SALARY, ('SALARY > 1000)), None), unresolvedalias('MIN('BONUS), None)]
    // +- 'Filter ('dept > 0)
    // +- 'UnresolvedRelation [h2, test, employee], [], false
    //
    // == Analyzed Logical Plan ==
    // max(SALARY) FILTER (WHERE (SALARY > 1000)): decimal(20,2), min(BONUS): decimal(6,2)
    // Aggregate [DEPT#797], [max(SALARY#799) FILTER (WHERE (cast(SALARY#799 as decimal(20,2)) > cast(cast(1000 as decimal(4,0)) as decimal(20,2)))) AS max(SALARY) FILTER (WHERE (SALARY > 1000))#804, min(BONUS#800) AS min(BONUS)#802]
    // +- Filter (dept#797 > 0)
    // +- SubqueryAlias h2.test.employee
    // +- RelationV2[DEPT#797, NAME#798, SALARY#799, BONUS#800] test.employee
    //
    // == Optimized Logical Plan ==
    // Aggregate [DEPT#797], [max(Max(SALARY,DecimalType(20,2))#810) AS max(SALARY) FILTER (WHERE (SALARY > 1000))#804, min(Min(BONUS,DecimalType(6,2))#811) AS min(BONUS)#802]
    // +- RelationV2[Max(SALARY,DecimalType(20,2))#810, Min(BONUS,DecimalType(6,2))#811, DEPT#797] test.employee
    //
    // == Physical Plan ==
    //   AdaptiveSparkPlan isFinalPlan=false
    // +- HashAggregate(keys=[DEPT#797], functions=[max(Max(SALARY,DecimalType(20,2))#810), min(Min(BONUS,DecimalType(6,2))#811)], output=[max(SALARY) FILTER (WHERE (SALARY > 1000))#804, min(BONUS)#802])
    // +- Exchange hashpartitioning(DEPT#797, 5), ENSURE_REQUIREMENTS, [id=#1647]
    // +- HashAggregate(keys=[DEPT#797], functions=[partial_max(Max(SALARY,DecimalType(20,2))#810), partial_min(Min(BONUS,DecimalType(6,2))#811)], output=[DEPT#797, max#814, min#815])
    // +- Scan org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan$$anon$1@239fdf8f [Max(SALARY,DecimalType(20,2))#810,Min(BONUS,DecimalType(6,2))#811,DEPT#797] PushedAggregates: [*Max(SALARY,DecimalType(20,2)), *Min(BONUS,DecimalType(6,2))], PushedFilters: [IsNotNull(dept), GreaterThan(dept,0), *GreaterThan(SALARY,1000.00)], PushedGroupby: [*DEPT], ReadSchema: struct<Max(SALARY,DecimalType(20,2)):decimal(20,2),Min(BONUS,DecimalType(6,2)):decimal(6,2),DEPT:...
    // scalastyle:on line.size.limit
    // df17.show(true)
    // +------------------------------------------+----------+
    // |max(SALARY) FILTER (WHERE (SALARY > 1000))|min(BONUS)|
    // +------------------------------------------+----------+
    // |                                  10000.00|   1000.00|
    // |                                  12000.00|   1200.00|
    // |                                  12000.00|   1200.00|
    // +------------------------------------------+----------+
    checkAnswer(df17, Seq(Row(10000.00, 1000.00), Row(12000.00, 1200.00), Row(12000.00, 1200.00)))
  }

  test("scan with aggregate distinct push-down") {
    checkAnswer(sql("SELECT SUM(SALARY) FROM h2.test.employee"), Seq(Row(53000)))
    checkAnswer(sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee"), Seq(Row(31000)))
    checkAnswer(sql("SELECT AVG(DEPT) FROM h2.test.employee"), Seq(Row(2)))
    checkAnswer(sql("SELECT AVG(DISTINCT DEPT) FROM h2.test.employee"), Seq(Row(3)))
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
