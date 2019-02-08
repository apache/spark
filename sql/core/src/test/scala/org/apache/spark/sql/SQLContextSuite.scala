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

package org.apache.spark.sql

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

class SQLContextSuite extends SparkFunSuite with SharedSparkContext {

  object DummyRule extends Rule[LogicalPlan] {
    def apply(p: LogicalPlan): LogicalPlan = p
  }

  test("Sessions of SQLContext") {
    val sqlContext = SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
    val session1 = sqlContext.newSession()
    val session2 = sqlContext.newSession()

    // all have the default configurations
    val key = SQLConf.SHUFFLE_PARTITIONS.key
    assert(session1.getConf(key) === session2.getConf(key))
    session1.setConf(key, "1")
    session2.setConf(key, "2")
    assert(session1.getConf(key) === "1")
    assert(session2.getConf(key) === "2")

    // temporary table should not be shared
    val df = session1.range(10)
    df.createOrReplaceTempView("test1")
    assert(session1.tableNames().contains("test1"))
    assert(!session2.tableNames().contains("test1"))

    // UDF should not be shared
    def myadd(a: Int, b: Int): Int = a + b
    session1.udf.register[Int, Int, Int]("myadd", myadd)
    session1.sql("select myadd(1, 2)").explain()
    intercept[AnalysisException] {
      session2.sql("select myadd(1, 2)").explain()
    }
  }

  test("Catalyst optimization passes are modifiable at runtime") {
    val sqlContext = SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
    sqlContext.experimental.extraOptimizations = Seq(DummyRule)
    assert(sqlContext.sessionState.optimizer.batches.flatMap(_.rules).contains(DummyRule))
  }

  test("get all tables") {
    val sqlContext = SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")
    assert(
      sqlContext.tables().filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    assert(
      sqlContext.sql("SHOW tables").filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    sqlContext.sessionState.catalog.dropTable(
      TableIdentifier("listtablessuitetable"), ignoreIfNotExists = true, purge = false)
    assert(sqlContext.tables().filter("tableName = 'listtablessuitetable'").count() === 0)
  }

  test("getting all tables with a database name has no impact on returned table names") {
    val sqlContext = SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")
    assert(
      sqlContext.tables("default").filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    assert(
      sqlContext.sql("show TABLES in default").filter("tableName = 'listtablessuitetable'")
        .collect().toSeq == Row("", "listtablessuitetable", true) :: Nil)

    sqlContext.sessionState.catalog.dropTable(
      TableIdentifier("listtablessuitetable"), ignoreIfNotExists = true, purge = false)
    assert(sqlContext.tables().filter("tableName = 'listtablessuitetable'").count() === 0)
  }

  test("query the returned DataFrame of tables") {
    val sqlContext = SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")

    val expectedSchema = StructType(
      StructField("database", StringType, false) ::
        StructField("tableName", StringType, false) ::
        StructField("isTemporary", BooleanType, false) :: Nil)

    Seq(sqlContext.tables(), sqlContext.sql("SHOW TABLes")).foreach {
      tableDF =>
        assert(expectedSchema === tableDF.schema)

        tableDF.createOrReplaceTempView("tables")
        assert(
          sqlContext.sql(
            "SELECT isTemporary, tableName from tables WHERE tableName = 'listtablessuitetable'")
            .collect().toSeq == Row(true, "listtablessuitetable") :: Nil)
        assert(
          sqlContext.tables().filter("tableName = 'tables'").select("tableName", "isTemporary")
            .collect().toSeq == Row("tables", true) :: Nil)
        sqlContext.dropTempTable("tables")
    }
  }

}
