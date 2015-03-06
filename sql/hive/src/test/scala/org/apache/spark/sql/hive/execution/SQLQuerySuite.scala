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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.hive.{MetastoreRelation, HiveShim}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.parquet.ParquetRelation2
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SQLConf}

case class Nested1(f1: Nested2)
case class Nested2(f2: Nested3)
case class Nested3(f3: Int)

case class NestedArray2(b: Seq[Int])
case class NestedArray1(a: NestedArray2)

/**
 * A collection of hive query tests where we generate the answers ourselves instead of depending on
 * Hive to generate them (in contrast to HiveQuerySuite).  Often this is because the query is
 * valid, but Hive currently cannot execute it.
 */
class SQLQuerySuite extends QueryTest {

  test("explode nested Field") {
    Seq(NestedArray1(NestedArray2(Seq(1,2,3)))).toDF.registerTempTable("nestedArray")
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
    assert(message.contains("Table ctas1 already exists"))
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
    intercept[org.apache.hadoop.hive.metastore.api.AlreadyExistsException] {
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
      "serde_p1=p1", "serde_p2=p2", "tbl_p1=p11", "tbl_p2=p22","MANAGED_TABLE"
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
    testData.insertInto("test1")
    sql("CREATE TABLE test2 (key INT, value STRING)")
    testData.insertInto("test2")
    testData.insertInto("test2")
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
    jsonRDD(rdd).registerTempTable("data")
    checkAnswer(
      sql("SELECT concat(a, '-', b), year(c) FROM data GROUP BY concat(a, '-', b), year(c)"),
      Row("str-1", 1970))

    dropTempTable("data")

    jsonRDD(rdd).registerTempTable("data")
    checkAnswer(sql("SELECT year(c) + 1 FROM data GROUP BY year(c) + 1"), Row(1971))

    dropTempTable("data")
  }

  test("logical.Project should not be resolved if it contains aggregates or generators") {
    // This test is used to test the fix of SPARK-5875.
    // The original issue was that Project's resolved will be true when it contains
    // AggregateExpressions or Generators. However, in this case, the Project
    // is not in a valid state (cannot be executed). Because of this bug, the analysis rule of
    // PreInsertionCasts will actually start to work before ImplicitGenerate and then
    // generates an invalid query plan.
    val rdd = sparkContext.makeRDD((1 to 5).map(i => s"""{"a":[$i, ${i+1}]}"""))
    jsonRDD(rdd).registerTempTable("data")
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
}
