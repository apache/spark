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

package org.apache.spark.sql.sources

import org.apache.spark.sql._

class DefaultSource extends SimpleScanSource

class SimpleScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleScan(parameters("from").toInt, parameters("to").toInt)(sqlContext)
  }
}

case class SimpleScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends TableScan {

  override def schema =
    StructType(StructField("i", IntegerType, nullable = false) :: Nil)

  override def buildScan() = sqlContext.sparkContext.parallelize(from to to).map(Row(_))
}

class TableScanSuite extends DataSourceTest {
  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTen
        |USING org.apache.spark.sql.sources.SimpleScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  sqlTest(
    "SELECT * FROM oneToTen",
    (1 to 10).map(Row(_)).toSeq)

  sqlTest(
    "SELECT i FROM oneToTen",
    (1 to 10).map(Row(_)).toSeq)

  sqlTest(
    "SELECT i FROM oneToTen WHERE i < 5",
    (1 to 4).map(Row(_)).toSeq)

  sqlTest(
    "SELECT i * 2 FROM oneToTen",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT a.i, b.i FROM oneToTen a JOIN oneToTen b ON a.i = b.i + 1",
    (2 to 10).map(i => Row(i, i - 1)).toSeq)


  test("Caching")  {
    // Cached Query Execution
    cacheTable("oneToTen")
    assertCached(sql("SELECT * FROM oneToTen"))
    checkAnswer(
      sql("SELECT * FROM oneToTen"),
      (1 to 10).map(Row(_)).toSeq)

    assertCached(sql("SELECT i FROM oneToTen"))
    checkAnswer(
      sql("SELECT i FROM oneToTen"),
      (1 to 10).map(Row(_)).toSeq)

    assertCached(sql("SELECT i FROM oneToTen WHERE i < 5"))
    checkAnswer(
      sql("SELECT i FROM oneToTen WHERE i < 5"),
      (1 to 4).map(Row(_)).toSeq)

    assertCached(sql("SELECT i * 2 FROM oneToTen"))
    checkAnswer(
      sql("SELECT i * 2 FROM oneToTen"),
      (1 to 10).map(i => Row(i * 2)).toSeq)

    assertCached(sql("SELECT a.i, b.i FROM oneToTen a JOIN oneToTen b ON a.i = b.i + 1"), 2)
    checkAnswer(
      sql("SELECT a.i, b.i FROM oneToTen a JOIN oneToTen b ON a.i = b.i + 1"),
      (2 to 10).map(i => Row(i, i - 1)).toSeq)

    // Verify uncaching
    uncacheTable("oneToTen")
    assertCached(sql("SELECT * FROM oneToTen"), 0)
  }

  test("defaultSource") {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenDef
        |USING org.apache.spark.sql.sources
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM oneToTenDef"),
      (1 to 10).map(Row(_)).toSeq)
  }
}
