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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, TableScan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class SaveIntoDataSourceCommandSuite extends QueryTest with SharedSparkSession {

  test("simpleString is redacted") {
    val URL = "connection.url"
    val PASS = "mypassword"
    val DRIVER = "mydriver"

    val dataSource = DataSource(
      sparkSession = spark,
      className = "jdbc",
      partitionColumns = Nil,
      options = Map("password" -> PASS, "url" -> URL, "driver" -> DRIVER))

    val logicalPlanString = dataSource
      .planForWriting(SaveMode.ErrorIfExists, spark.range(1).logicalPlan)
      .treeString(true)

    assert(!logicalPlanString.contains(URL))
    assert(!logicalPlanString.contains(PASS))
    assert(logicalPlanString.contains(DRIVER))
  }

  test("SPARK-39952: SaveIntoDataSourceCommand should recache result relation") {
    val provider = classOf[FakeV1DataSource].getName

    def saveIntoDataSource(data: Int): Unit = {
      spark.range(data)
        .write
        .mode("append")
        .format(provider)
        .save()
    }

    def loadData: DataFrame = {
      spark.read
        .format(provider)
        .load()
    }

    saveIntoDataSource(1)
    val cached = loadData.cache()
    checkAnswer(cached, Row(0))

    saveIntoDataSource(2)
    checkAnswer(loadData, Row(0) :: Row(1) :: Nil)

    FakeV1DataSource.data = null
  }

  test("Data type support") {

    val dataSource = DataSource(
      sparkSession = spark,
      className = "jdbc",
      partitionColumns = Nil,
      options = Map())

    val df = spark.range(1).selectExpr(
        "cast('a' as binary) a", "true b", "cast(1 as byte) c", "1.23 d", "'abc'",
        "'abc' COLLATE UTF8_LCASE")
    dataSource.planForWriting(SaveMode.ErrorIfExists, df.logicalPlan)

    // Variant and Interval types are disallowed by default.
    val unsupportedTypes = Seq(
        ("parse_json('1') col", "VARIANT"),
        ("array(parse_json('1')) col", "ARRAY<VARIANT>"),
        ("struct(1, parse_json('1')) col", "STRUCT<col1: INT NOT NULL, col2: VARIANT NOT NULL>"),
        ("map(1, parse_json('1')) col", "MAP<INT, VARIANT>"),
        ("INTERVAL '1' MONTH col", "INTERVAL MONTH"),
        ("make_ym_interval(1, 2) col", "INTERVAL YEAR TO MONTH"),
        ("make_dt_interval(1, 2, 3, 4) col", "INTERVAL DAY TO SECOND"))

    unsupportedTypes.foreach { testCase =>
      val df = spark.range(1).selectExpr(testCase._1)
      checkError(
        exception = intercept[AnalysisException] {
          dataSource.planForWriting(SaveMode.ErrorIfExists, df.logicalPlan)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map("columnName" -> "`col`", "columnType" -> s"\"${testCase._2}\"",
          "format" -> ".*JdbcRelationProvider.*"),
        matchPVals = true
      )
    }
  }
}

object FakeV1DataSource {
  var data: RDD[Row] = _
}

class FakeV1DataSource extends RelationProvider with CreatableRelationProvider {
  override def createRelation(
     sqlContext: SQLContext,
     parameters: Map[String, String]): BaseRelation = {
    FakeRelation()
  }

  override def createRelation(
     sqlContext: SQLContext,
     mode: SaveMode,
     parameters: Map[String, String],
     data: DataFrame): BaseRelation = {
    FakeV1DataSource.data = data.rdd
    FakeRelation()
  }
}

case class FakeRelation() extends BaseRelation with TableScan {
  override def sqlContext: SQLContext = SparkSession.getActiveSession.get.sqlContext
  override def schema: StructType = StructType(Seq(StructField("id", LongType)))
  override def buildScan(): RDD[Row] = FakeV1DataSource.data
}
