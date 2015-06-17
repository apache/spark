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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.DefaultParserDialect
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.hive.{HiveQLDialect, HiveShim, MetastoreRelation}
import org.apache.spark.sql.parquet.ParquetRelation2
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types._

case class Nested1(f1: Nested2)
case class Nested2(f2: Nested3)
case class Nested3(f3: Int)

case class NestedArray2(b: Seq[Int])
case class NestedArray1(a: NestedArray2)

case class Order(
    id: Int,
    make: String,
    `type`: String,
    price: Int,
    pdate: String,
    customer: String,
    city: String,
    state: String,
    month: Int)

case class WindowData(
    month: Int,
    area: String,
    product: Int)
/** A SQL Dialect for testing purpose, and it can not be nested type */
class MyDialect extends DefaultParserDialect

/**
 * A collection of hive query tests where we generate the answers ourselves instead of depending on
 * Hive to generate them (in contrast to HiveQuerySuite).  Often this is because the query is
 * valid, but Hive currently cannot execute it.
 */
class SQLQuerySuite extends QueryTest {
  test("SPARK-6835: udtf in lateral view") {
    val df = Seq((1, 1)).toDF("c1", "c2")
    df.registerTempTable("table1")
    val query = sql("SELECT c1, v FROM table1 LATERAL VIEW stack(3, 1, c1 + 1, c1 + 2) d AS v")
    checkAnswer(query, Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Nil)
  }

  test("SPARK-6851: Self-joined converted parquet tables") {
    val orders = Seq(
      Order(1, "Atlas", "MTB", 234, "2015-01-07", "John D", "Pacifica", "CA", 20151),
      Order(3, "Swift", "MTB", 285, "2015-01-17", "John S", "Redwood City", "CA", 20151),
      Order(4, "Atlas", "Hybrid", 303, "2015-01-23", "Jones S", "San Mateo", "CA", 20151),
      Order(7, "Next", "MTB", 356, "2015-01-04", "Jane D", "Daly City", "CA", 20151),
      Order(10, "Next", "YFlikr", 187, "2015-01-09", "John D", "Fremont", "CA", 20151),
      Order(11, "Swift", "YFlikr", 187, "2015-01-23", "John D", "Hayward", "CA", 20151),
      Order(2, "Next", "Hybrid", 324, "2015-02-03", "Jane D", "Daly City", "CA", 20152),
      Order(5, "Next", "Street", 187, "2015-02-08", "John D", "Fremont", "CA", 20152),
      Order(6, "Atlas", "Street", 154, "2015-02-09", "John D", "Pacifica", "CA", 20152),
      Order(8, "Swift", "Hybrid", 485, "2015-02-19", "John S", "Redwood City", "CA", 20152),
      Order(9, "Atlas", "Split", 303, "2015-02-28", "Jones S", "San Mateo", "CA", 20152))

    val orderUpdates = Seq(
      Order(1, "Atlas", "MTB", 434, "2015-01-07", "John D", "Pacifica", "CA", 20151),
      Order(11, "Swift", "YFlikr", 137, "2015-01-23", "John D", "Hayward", "CA", 20151))

    orders.toDF.registerTempTable("orders1")
    orderUpdates.toDF.registerTempTable("orderupdates1")

    sql(
      """CREATE TABLE orders(
        |  id INT,
        |  make String,
        |  type String,
        |  price INT,
        |  pdate String,
        |  customer String,
        |  city String)
        |PARTITIONED BY (state STRING, month INT)
        |STORED AS PARQUET
      """.stripMargin)

    sql(
      """CREATE TABLE orderupdates(
        |  id INT,
        |  make String,
        |  type String,
        |  price INT,
        |  pdate String,
        |  customer String,
        |  city String)
        |PARTITIONED BY (state STRING, month INT)
        |STORED AS PARQUET
      """.stripMargin)

    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("INSERT INTO TABLE orders PARTITION(state, month) SELECT * FROM orders1")
    sql("INSERT INTO TABLE orderupdates PARTITION(state, month) SELECT * FROM orderupdates1")

    checkAnswer(
      sql(
        """
          |select orders.state, orders.month
          |from orders
          |join (
          |  select distinct orders.state,orders.month
          |  from orders
          |  join orderupdates
          |    on orderupdates.id = orders.id) ao
          |  on ao.state = orders.state and ao.month = orders.month
        """.stripMargin),
      (1 to 6).map(_ => Row("CA", 20151)))
  }

  test("SPARK-5371: union with null and sum") {
    val df = Seq((1, 1)).toDF("c1", "c2")
    df.registerTempTable("table1")

    val query = sql(
      """
        |SELECT
        |  MIN(c1),
        |  MIN(c2)
        |FROM (
        |  SELECT
        |    SUM(c1) c1,
        |    NULL c2
        |  FROM table1
        |  UNION ALL
        |  SELECT
        |    NULL c1,
        |    SUM(c2) c2
        |  FROM table1
        |) a
      """.stripMargin)
    checkAnswer(query, Row(1, 1) :: Nil)
  }

  test("explode nested Field") {
    Seq(NestedArray1(NestedArray2(Seq(1, 2, 3)))).toDF.registerTempTable("nestedArray")
    checkAnswer(
      sql("SELECT ints FROM nestedArray LATERAL VIEW explode(a.b) a AS ints"),
      Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("SPARK-4512 Fix attribute reference resolution error when using SORT BY") {
    checkAnswer(
      sql("SELECT * FROM (SELECT key + key AS a FROM src SORT BY value) t ORDER BY t.a"),
      sql("SELECT key + key as a FROM src ORDER BY a").collect().toSeq
    )
  }

  test("CTAS without serde") {
    def checkRelation(tableName: String, isDataSourceParquet: Boolean): Unit = {
      val relation = EliminateSubQueries(catalog.lookupRelation(Seq(tableName)))
      relation match {
        case LogicalRelation(r: ParquetRelation2) =>
          if (!isDataSourceParquet) {
            fail(
              s"${classOf[MetastoreRelation].getCanonicalName} is expected, but found " +
              s"${ParquetRelation2.getClass.getCanonicalName}.")
          }

        case r: MetastoreRelation =>
          if (isDataSourceParquet) {
            fail(
              s"${ParquetRelation2.getClass.getCanonicalName} is expected, but found " +
              s"${classOf[MetastoreRelation].getCanonicalName}.")
          }
      }
    }

    val originalConf = getConf("spark.sql.hive.convertCTAS", "false")

    setConf("spark.sql.hive.convertCTAS", "true")

    sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
    sql("CREATE TABLE IF NOT EXISTS ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
    var message = intercept[AnalysisException] {
      sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
    }.getMessage
    assert(message.contains("ctas1 already exists"))
    checkRelation("ctas1", true)
    sql("DROP TABLE ctas1")

    // Specifying database name for query can be converted to data source write path
    // is not allowed right now.
    message = intercept[AnalysisException] {
      sql("CREATE TABLE default.ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
    }.getMessage
    assert(
      message.contains("Cannot specify database name in a CTAS statement"),
      "When spark.sql.hive.convertCTAS is true, we should not allow " +
      "database name specified.")

    sql("CREATE TABLE ctas1 stored as textfile AS SELECT key k, value FROM src ORDER BY k, value")
    checkRelation("ctas1", true)
    sql("DROP TABLE ctas1")

    sql(
      "CREATE TABLE ctas1 stored as sequencefile AS SELECT key k, value FROM src ORDER BY k, value")
    checkRelation("ctas1", true)
    sql("DROP TABLE ctas1")

    sql("CREATE TABLE ctas1 stored as rcfile AS SELECT key k, value FROM src ORDER BY k, value")
    checkRelation("ctas1", false)
    sql("DROP TABLE ctas1")

    sql("CREATE TABLE ctas1 stored as orc AS SELECT key k, value FROM src ORDER BY k, value")
    checkRelation("ctas1", false)
    sql("DROP TABLE ctas1")

    sql("CREATE TABLE ctas1 stored as parquet AS SELECT key k, value FROM src ORDER BY k, value")
    checkRelation("ctas1", false)
    sql("DROP TABLE ctas1")

    setConf("spark.sql.hive.convertCTAS", originalConf)
  }

  test("SQL Dialect Switching") {
    assert(getSQLDialect().getClass === classOf[HiveQLDialect])
    setConf("spark.sql.dialect", classOf[MyDialect].getCanonicalName())
    assert(getSQLDialect().getClass === classOf[MyDialect])
    assert(sql("SELECT 1").collect() === Array(Row(1)))

    // set the dialect back to the DefaultSQLDialect
    sql("SET spark.sql.dialect=sql")
    assert(getSQLDialect().getClass === classOf[DefaultParserDialect])
    sql("SET spark.sql.dialect=hiveql")
    assert(getSQLDialect().getClass === classOf[HiveQLDialect])

    // set invalid dialect
    sql("SET spark.sql.dialect.abc=MyTestClass")
    sql("SET spark.sql.dialect=abc")
    intercept[Exception] {
      sql("SELECT 1")
    }
    // test if the dialect set back to HiveQLDialect
    getSQLDialect().getClass === classOf[HiveQLDialect]

    sql("SET spark.sql.dialect=MyTestClass")
    intercept[DialectException] {
      sql("SELECT 1")
    }
    // test if the dialect set back to HiveQLDialect
    assert(getSQLDialect().getClass === classOf[HiveQLDialect])
  }

  test("CTAS with serde") {
    sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value").collect()
    sql(
      """CREATE TABLE ctas2
        | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
        | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
        | STORED AS RCFile
        | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
        | AS
        |   SELECT key, value
        |   FROM src
        |   ORDER BY key, value""".stripMargin).collect()
    sql(
      """CREATE TABLE ctas3
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\012'
        | STORED AS textfile AS
        |   SELECT key, value
        |   FROM src
        |   ORDER BY key, value""".stripMargin).collect()

    // the table schema may like (key: integer, value: string)
    sql(
      """CREATE TABLE IF NOT EXISTS ctas4 AS
        | SELECT 1 AS key, value FROM src LIMIT 1""".stripMargin).collect()
    // do nothing cause the table ctas4 already existed.
    sql(
      """CREATE TABLE IF NOT EXISTS ctas4 AS
        | SELECT key, value FROM src ORDER BY key, value""".stripMargin).collect()

    checkAnswer(
      sql("SELECT k, value FROM ctas1 ORDER BY k, value"),
      sql("SELECT key, value FROM src ORDER BY key, value").collect().toSeq)
    checkAnswer(
      sql("SELECT key, value FROM ctas2 ORDER BY key, value"),
      sql(
        """
          SELECT key, value
          FROM src
          ORDER BY key, value""").collect().toSeq)
    checkAnswer(
      sql("SELECT key, value FROM ctas3 ORDER BY key, value"),
      sql(
        """
          SELECT key, value
          FROM src
          ORDER BY key, value""").collect().toSeq)
    intercept[AnalysisException] {
      sql(
        """CREATE TABLE ctas4 AS
          | SELECT key, value FROM src ORDER BY key, value""".stripMargin).collect()
    }
    checkAnswer(
      sql("SELECT key, value FROM ctas4 ORDER BY key, value"),
      sql("SELECT key, value FROM ctas4 LIMIT 1").collect().toSeq)

    checkExistence(sql("DESC EXTENDED ctas2"), true,
      "name:key", "type:string", "name:value", "ctas2",
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
      "serde_p1=p1", "serde_p2=p2", "tbl_p1=p11", "tbl_p2=p22", "MANAGED_TABLE"
    )

    if (HiveShim.version =="0.13.1") {
      val origUseParquetDataSource = conf.parquetUseDataSourceApi
      try {
        setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "false")
        sql(
          """CREATE TABLE ctas5
            | STORED AS parquet AS
            |   SELECT key, value
            |   FROM src
            |   ORDER BY key, value""".stripMargin).collect()

        checkExistence(sql("DESC EXTENDED ctas5"), true,
          "name:key", "type:string", "name:value", "ctas5",
          "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
          "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
          "MANAGED_TABLE"
        )

        val default = getConf("spark.sql.hive.convertMetastoreParquet", "true")
        // use the Hive SerDe for parquet tables
        sql("set spark.sql.hive.convertMetastoreParquet = false")
        checkAnswer(
          sql("SELECT key, value FROM ctas5 ORDER BY key, value"),
          sql("SELECT key, value FROM src ORDER BY key, value").collect().toSeq)
        sql(s"set spark.sql.hive.convertMetastoreParquet = $default")
      } finally {
        setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, origUseParquetDataSource.toString)
      }
    }
  }

  test("command substitution") {
    sql("set tbl=src")
    checkAnswer(
      sql("SELECT key FROM ${hiveconf:tbl} ORDER BY key, value limit 1"),
      sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)

    sql("set hive.variable.substitute=false") // disable the substitution
    sql("set tbl2=src")
    intercept[Exception] {
      sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1").collect()
    }

    sql("set hive.variable.substitute=true") // enable the substitution
    checkAnswer(
      sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1"),
      sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)
  }

  test("ordering not in select") {
    checkAnswer(
      sql("SELECT key FROM src ORDER BY value"),
      sql("SELECT key FROM (SELECT key, value FROM src ORDER BY value) a").collect().toSeq)
  }

  test("ordering not in agg") {
    checkAnswer(
      sql("SELECT key FROM src GROUP BY key, value ORDER BY value"),
      sql("""
        SELECT key
        FROM (
          SELECT key, value
          FROM src
          GROUP BY key, value
          ORDER BY value) a""").collect().toSeq)
  }

  test("double nested data") {
    sparkContext.parallelize(Nested1(Nested2(Nested3(1))) :: Nil)
      .toDF().registerTempTable("nested")
    checkAnswer(
      sql("SELECT f1.f2.f3 FROM nested"),
      Row(1))
    checkAnswer(sql("CREATE TABLE test_ctas_1234 AS SELECT * from nested"),
      Seq.empty[Row])
    checkAnswer(
      sql("SELECT * FROM test_ctas_1234"),
      sql("SELECT * FROM nested").collect().toSeq)

    intercept[AnalysisException] {
      sql("CREATE TABLE test_ctas_12345 AS SELECT * from notexists").collect()
    }
  }

  test("test CTAS") {
    checkAnswer(sql("CREATE TABLE test_ctas_123 AS SELECT key, value FROM src"), Seq.empty[Row])
    checkAnswer(
      sql("SELECT key, value FROM test_ctas_123 ORDER BY key"),
      sql("SELECT key, value FROM src ORDER BY key").collect().toSeq)
  }

  test("SPARK-4825 save join to table") {
    val testData = sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString)).toDF()
    sql("CREATE TABLE test1 (key INT, value STRING)")
    testData.write.mode(SaveMode.Append).insertInto("test1")
    sql("CREATE TABLE test2 (key INT, value STRING)")
    testData.write.mode(SaveMode.Append).insertInto("test2")
    testData.write.mode(SaveMode.Append).insertInto("test2")
    sql("CREATE TABLE test AS SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key")
    checkAnswer(
      table("test"),
      sql("SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key").collect().toSeq)
  }

  test("SPARK-3708 Backticks aren't handled correctly is aliases") {
    checkAnswer(
      sql("SELECT k FROM (SELECT `key` AS `k` FROM src) a"),
      sql("SELECT `key` FROM src").collect().toSeq)
  }

  test("SPARK-3834 Backticks not correctly handled in subquery aliases") {
    checkAnswer(
      sql("SELECT a.key FROM (SELECT key FROM src) `a`"),
      sql("SELECT `key` FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise & operator") {
    checkAnswer(
      sql("SELECT case when 1&1=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise | operator") {
    checkAnswer(
      sql("SELECT case when 1|0=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise ^ operator") {
    checkAnswer(
      sql("SELECT case when 1^0=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise ~ operator") {
    checkAnswer(
      sql("SELECT case when ~1=-2 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-4154 Query does not work if it has 'not between' in Spark SQL and HQL") {
    checkAnswer(sql("SELECT key FROM src WHERE key not between 0 and 10 order by key"),
      sql("SELECT key FROM src WHERE key between 11 and 500 order by key").collect().toSeq)
  }

  test("SPARK-2554 SumDistinct partial aggregation") {
    checkAnswer(sql("SELECT sum( distinct key) FROM src group by key order by key"),
      sql("SELECT distinct key FROM src order by key").collect().toSeq)
  }

  test("SPARK-4963 DataFrame sample on mutable row return wrong result") {
    sql("SELECT * FROM src WHERE key % 2 = 0")
      .sample(withReplacement = false, fraction = 0.3)
      .registerTempTable("sampled")
    (1 to 10).foreach { i =>
      checkAnswer(
        sql("SELECT * FROM sampled WHERE key % 2 = 1"),
        Seq.empty[Row])
    }
  }

  test("SPARK-4699 HiveContext should be case insensitive by default") {
    checkAnswer(
      sql("SELECT KEY FROM Src ORDER BY value"),
      sql("SELECT key FROM src ORDER BY value").collect().toSeq)
  }

  test("SPARK-5284 Insert into Hive throws NPE when a inner complex type field has a null value") {
    val schema = StructType(
      StructField("s",
        StructType(
          StructField("innerStruct", StructType(StructField("s1", StringType, true) :: Nil)) ::
            StructField("innerArray", ArrayType(IntegerType), true) ::
            StructField("innerMap", MapType(StringType, IntegerType)) :: Nil), true) :: Nil)
    val row = Row(Row(null, null, null))

    val rowRdd = sparkContext.parallelize(row :: Nil)

    TestHive.createDataFrame(rowRdd, schema).registerTempTable("testTable")

    sql(
      """CREATE TABLE nullValuesInInnerComplexTypes
        |  (s struct<innerStruct: struct<s1:string>,
        |            innerArray:array<int>,
        |            innerMap: map<string, int>>)
      """.stripMargin).collect()

    sql(
      """
        |INSERT OVERWRITE TABLE nullValuesInInnerComplexTypes
        |SELECT * FROM testTable
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM nullValuesInInnerComplexTypes"),
      Row(Row(null, null, null))
    )

    sql("DROP TABLE nullValuesInInnerComplexTypes")
    dropTempTable("testTable")
  }

  test("SPARK-4296 Grouping field with Hive UDF as sub expression") {
    val rdd = sparkContext.makeRDD( """{"a": "str", "b":"1", "c":"1970-01-01 00:00:00"}""" :: Nil)
    read.json(rdd).registerTempTable("data")
    checkAnswer(
      sql("SELECT concat(a, '-', b), year(c) FROM data GROUP BY concat(a, '-', b), year(c)"),
      Row("str-1", 1970))

    dropTempTable("data")

    read.json(rdd).registerTempTable("data")
    checkAnswer(sql("SELECT year(c) + 1 FROM data GROUP BY year(c) + 1"), Row(1971))

    dropTempTable("data")
  }

  test("resolve udtf in projection #1") {
    val rdd = sparkContext.makeRDD((1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    read.json(rdd).registerTempTable("data")
    val df = sql("SELECT explode(a) AS val FROM data")
    val col = df("val")
  }

  test("resolve udtf in projection #2") {
    val rdd = sparkContext.makeRDD((1 to 2).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    jsonRDD(rdd).registerTempTable("data")
    checkAnswer(sql("SELECT explode(map(1, 1)) FROM data LIMIT 1"), Row(1, 1) :: Nil)
    checkAnswer(sql("SELECT explode(map(1, 1)) as (k1, k2) FROM data LIMIT 1"), Row(1, 1) :: Nil)
    intercept[AnalysisException] {
      sql("SELECT explode(map(1, 1)) as k1 FROM data LIMIT 1")
    }

    intercept[AnalysisException] {
      sql("SELECT explode(map(1, 1)) as (k1, k2, k3) FROM data LIMIT 1")
    }
  }

  // TGF with non-TGF in project is allowed in Spark SQL, but not in Hive
  test("TGF with non-TGF in projection") {
    val rdd = sparkContext.makeRDD( """{"a": "1", "b":"1"}""" :: Nil)
    jsonRDD(rdd).registerTempTable("data")
    checkAnswer(
      sql("SELECT explode(map(a, b)) as (k1, k2), a, b FROM data"),
      Row("1", "1", "1", "1") :: Nil)
  }

  test("logical.Project should not be resolved if it contains aggregates or generators") {
    // This test is used to test the fix of SPARK-5875.
    // The original issue was that Project's resolved will be true when it contains
    // AggregateExpressions or Generators. However, in this case, the Project
    // is not in a valid state (cannot be executed). Because of this bug, the analysis rule of
    // PreInsertionCasts will actually start to work before ImplicitGenerate and then
    // generates an invalid query plan.
    val rdd = sparkContext.makeRDD((1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    read.json(rdd).registerTempTable("data")
    val originalConf = getConf("spark.sql.hive.convertCTAS", "false")
    setConf("spark.sql.hive.convertCTAS", "false")

    sql("CREATE TABLE explodeTest (key bigInt)")
    table("explodeTest").queryExecution.analyzed match {
      case metastoreRelation: MetastoreRelation => // OK
      case _ =>
        fail("To correctly test the fix of SPARK-5875, explodeTest should be a MetastoreRelation")
    }

    sql(s"INSERT OVERWRITE TABLE explodeTest SELECT explode(a) AS val FROM data")
    checkAnswer(
      sql("SELECT key from explodeTest"),
      (1 to 5).flatMap(i => Row(i) :: Row(i + 1) :: Nil)
    )

    sql("DROP TABLE explodeTest")
    dropTempTable("data")
    setConf("spark.sql.hive.convertCTAS", originalConf)
  }

  test("sanity test for SPARK-6618") {
    (1 to 100).par.map { i =>
      val tableName = s"SPARK_6618_table_$i"
      sql(s"CREATE TABLE $tableName (col1 string)")
      catalog.lookupRelation(Seq(tableName))
      table(tableName)
      tables()
      sql(s"DROP TABLE $tableName")
    }
  }

  test("SPARK-5203 union with different decimal precision") {
    Seq.empty[(Decimal, Decimal)]
      .toDF("d1", "d2")
      .select($"d1".cast(DecimalType(10, 15)).as("d"))
      .registerTempTable("dn")

    sql("select d from dn union all select d * 2 from dn")
      .queryExecution.analyzed
  }

  test("test script transform") {
    val data = (1 to 100000).map { i => (i, i, i) }
    data.toDF("d1", "d2", "d3").registerTempTable("script_trans")
    assert(100000 ===
      sql("SELECT TRANSFORM (d1, d2, d3) USING 'cat' AS (a,b,c) FROM script_trans")
      .queryExecution.toRdd.count())
  }

  test("window function: udaf with aggregate expressin") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    sparkContext.parallelize(data).toDF().registerTempTable("windowData")

    checkAnswer(
      sql(
        """
          |select area, sum(product), sum(sum(product)) over (partition by area)
          |from windowData group by month, area
        """.stripMargin),
      Seq(
        ("a", 5, 11),
        ("a", 6, 11),
        ("b", 7, 15),
        ("b", 8, 15),
        ("c", 9, 19),
        ("c", 10, 19)
      ).map(i => Row(i._1, i._2, i._3)))

    checkAnswer(
      sql(
        """
          |select area, sum(product) - 1, sum(sum(product)) over (partition by area)
          |from windowData group by month, area
        """.stripMargin),
      Seq(
        ("a", 4, 11),
        ("a", 5, 11),
        ("b", 6, 15),
        ("b", 7, 15),
        ("c", 8, 19),
        ("c", 9, 19)
      ).map(i => Row(i._1, i._2, i._3)))

    checkAnswer(
      sql(
        """
          |select area, sum(product), sum(product) / sum(sum(product)) over (partition by area)
          |from windowData group by month, area
        """.stripMargin),
      Seq(
        ("a", 5, 5d/11),
        ("a", 6, 6d/11),
        ("b", 7, 7d/15),
        ("b", 8, 8d/15),
        ("c", 10, 10d/19),
        ("c", 9, 9d/19)
      ).map(i => Row(i._1, i._2, i._3)))

    checkAnswer(
      sql(
        """
          |select area, sum(product), sum(product) / sum(sum(product) - 1) over (partition by area)
          |from windowData group by month, area
        """.stripMargin),
      Seq(
        ("a", 5, 5d/9),
        ("a", 6, 6d/9),
        ("b", 7, 7d/13),
        ("b", 8, 8d/13),
        ("c", 10, 10d/17),
        ("c", 9, 9d/17)
      ).map(i => Row(i._1, i._2, i._3)))
  }

  test("window function: partition and order expressions") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    sparkContext.parallelize(data).toDF().registerTempTable("windowData")

    checkAnswer(
      sql(
        """
          |select month, area, product, sum(product + 1) over (partition by 1 order by 2)
          |from windowData
        """.stripMargin),
      Seq(
        (1, "a", 5, 51),
        (2, "a", 6, 51),
        (3, "b", 7, 51),
        (4, "b", 8, 51),
        (5, "c", 9, 51),
        (6, "c", 10, 51)
      ).map(i => Row(i._1, i._2, i._3, i._4)))

    checkAnswer(
      sql(
        """
          |select month, area, product, sum(product)
          |over (partition by month % 2 order by 10 - product)
          |from windowData
        """.stripMargin),
      Seq(
        (1, "a", 5, 21),
        (2, "a", 6, 24),
        (3, "b", 7, 16),
        (4, "b", 8, 18),
        (5, "c", 9, 9),
        (6, "c", 10, 10)
      ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("window function: expressions in arguments of a window functions") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    sparkContext.parallelize(data).toDF().registerTempTable("windowData")

    checkAnswer(
      sql(
        """
          |select month, area, month % 2,
          |lag(product, 1 + 1, product) over (partition by month % 2 order by area)
          |from windowData
        """.stripMargin),
      Seq(
        (1, "a", 1, 5),
        (2, "a", 0, 6),
        (3, "b", 1, 7),
        (4, "b", 0, 8),
        (5, "c", 1, 5),
        (6, "c", 0, 6)
      ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("window function: multiple window expressions in a single expression") {
    val nums = sparkContext.parallelize(1 to 10).map(x => (x, x % 2)).toDF("x", "y")
    nums.registerTempTable("nums")

    val expected =
      Row(1, 1, 1, 55, 1, 57) ::
      Row(0, 2, 3, 55, 2, 60) ::
      Row(1, 3, 6, 55, 4, 65) ::
      Row(0, 4, 10, 55, 6, 71) ::
      Row(1, 5, 15, 55, 9, 79) ::
      Row(0, 6, 21, 55, 12, 88) ::
      Row(1, 7, 28, 55, 16, 99) ::
      Row(0, 8, 36, 55, 20, 111) ::
      Row(1, 9, 45, 55, 25, 125) ::
      Row(0, 10, 55, 55, 30, 140) :: Nil

    val actual = sql(
      """
        |SELECT
        |  y,
        |  x,
        |  sum(x) OVER w1 AS running_sum,
        |  sum(x) OVER w2 AS total_sum,
        |  sum(x) OVER w3 AS running_sum_per_y,
        |  ((sum(x) OVER w1) + (sum(x) OVER w2) + (sum(x) OVER w3)) as combined2
        |FROM nums
        |WINDOW w1 AS (ORDER BY x ROWS BETWEEN UnBOUNDED PRECEDiNG AND CuRRENT RoW),
        |       w2 AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOuNDED FoLLOWING),
        |       w3 AS (PARTITION BY y ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      """.stripMargin)

    checkAnswer(actual, expected)

    dropTempTable("nums")
  }

  test("test case key when") {
    (1 to 5).map(i => (i, i.toString)).toDF("k", "v").registerTempTable("t")
    checkAnswer(
      sql("SELECT CASE k WHEN 2 THEN 22 WHEN 4 THEN 44 ELSE 0 END, v FROM t"),
      Row(0, "1") :: Row(22, "2") :: Row(0, "3") :: Row(44, "4") :: Row(0, "5") :: Nil)
  }

  test("SPARK-7595: Window will cause resolve failed with self join") {
    checkAnswer(sql(
      """
        |with
        | v1 as (select key, count(value) over (partition by key) cnt_val from src),
        | v2 as (select v1.key, v1_lag.cnt_val from v1, v1 v1_lag where v1.key = v1_lag.key)
        | select * from v2 order by key limit 1
      """.stripMargin), Row(0, 3))
  }

  test("SPARK-7269 Check analysis failed in case in-sensitive") {
    Seq(1, 2, 3).map { i =>
      (i.toString, i.toString)
    }.toDF("key", "value").registerTempTable("df_analysis")
    sql("SELECT kEy from df_analysis group by key").collect()
    sql("SELECT kEy+3 from df_analysis group by key+3").collect()
    sql("SELECT kEy+3, a.kEy, A.kEy from df_analysis A group by key").collect()
    sql("SELECT cast(kEy+1 as Int) from df_analysis A group by cast(key+1 as int)").collect()
    sql("SELECT cast(kEy+1 as Int) from df_analysis A group by key+1").collect()
    sql("SELECT 2 from df_analysis A group by key+1").collect()
    intercept[AnalysisException] {
      sql("SELECT kEy+1 from df_analysis group by key+3")
    }
    intercept[AnalysisException] {
      sql("SELECT cast(key+2 as Int) from df_analysis A group by cast(key+1 as int)")
    }
  }

  // `Math.exp(1.0)` has different result for different jdk version, so not use createQueryTest
  test("udf_java_method") {
    checkAnswer(sql(
      """
        |SELECT java_method("java.lang.String", "valueOf", 1),
        |       java_method("java.lang.String", "isEmpty"),
        |       java_method("java.lang.Math", "max", 2, 3),
        |       java_method("java.lang.Math", "min", 2, 3),
        |       java_method("java.lang.Math", "round", 2.5),
        |       java_method("java.lang.Math", "exp", 1.0),
        |       java_method("java.lang.Math", "floor", 1.9)
        |FROM src tablesample (1 rows)
      """.stripMargin),
      Row(
        "1",
        "true",
        java.lang.Math.max(2, 3).toString,
        java.lang.Math.min(2, 3).toString,
        java.lang.Math.round(2.5).toString,
        java.lang.Math.exp(1.0).toString,
        java.lang.Math.floor(1.9).toString))
  }

  test("dynamic partition value test") {
    try {
      sql("set hive.exec.dynamic.partition.mode=nonstrict")
      // date
      sql("drop table if exists dynparttest1")
      sql("create table dynparttest1 (value int) partitioned by (pdate date)")
      sql(
        """
          |insert into table dynparttest1 partition(pdate)
          | select count(*), cast('2015-05-21' as date) as pdate from src
        """.stripMargin)
      checkAnswer(
        sql("select * from dynparttest1"),
        Seq(Row(500, java.sql.Date.valueOf("2015-05-21"))))

      // decimal
      sql("drop table if exists dynparttest2")
      sql("create table dynparttest2 (value int) partitioned by (pdec decimal(5, 1))")
      sql(
        """
          |insert into table dynparttest2 partition(pdec)
          | select count(*), cast('100.12' as decimal(5, 1)) as pdec from src
        """.stripMargin)
      checkAnswer(
        sql("select * from dynparttest2"),
        Seq(Row(500, new java.math.BigDecimal("100.1"))))
    } finally {
      sql("drop table if exists dynparttest1")
      sql("drop table if exists dynparttest2")
      sql("set hive.exec.dynamic.partition.mode=strict")
    }
  }

  test("Call add jar in a different thread (SPARK-8306)") {
    @volatile var error: Option[Throwable] = None
    val thread = new Thread {
      override def run() {
        // To make sure this test works, this jar should not be loaded in another place.
        TestHive.sql(
          s"ADD JAR ${TestHive.getHiveFile("hive-contrib-0.13.1.jar").getCanonicalPath()}")
        try {
          TestHive.sql(
            """
              |CREATE TEMPORARY FUNCTION example_max
              |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
            """.stripMargin)
        } catch {
          case throwable: Throwable =>
            error = Some(throwable)
        }
      }
    }
    thread.start()
    thread.join()
    error match {
      case Some(throwable) =>
        fail("CREATE TEMPORARY FUNCTION should not fail.", throwable)
      case None => // OK
    }
  }
}
