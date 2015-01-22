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

import scala.language.existentials

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class PrunedScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimplePrunedScan(parameters("from").toInt, parameters("to").toInt)(sqlContext)
  }
}

case class SimplePrunedScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends PrunedScan {

  override def schema =
    StructType(
      StructField("a", IntegerType, nullable = false) ::
      StructField("b", IntegerType, nullable = false) :: Nil)

  override def buildScan(requiredColumns: Array[String]) = {
    val rowBuilders = requiredColumns.map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i * 2)
    }

    sqlContext.sparkContext.parallelize(from to to).map(i =>
      Row.fromSeq(rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)))
  }
}

class PrunedScanSuite extends DataSourceTest {
  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenPruned
        |USING org.apache.spark.sql.sources.PrunedScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  sqlTest(
    "SELECT * FROM oneToTenPruned",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenPruned",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT b, a FROM oneToTenPruned",
    (1 to 10).map(i => Row(i * 2, i)).toSeq)

  sqlTest(
    "SELECT a FROM oneToTenPruned",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT a, a FROM oneToTenPruned",
    (1 to 10).map(i => Row(i, i)).toSeq)

  sqlTest(
    "SELECT b FROM oneToTenPruned",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT a * 2 FROM oneToTenPruned",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT A AS b FROM oneToTenPruned",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT x.b, y.a FROM oneToTenPruned x JOIN oneToTenPruned y ON x.a = y.b",
    (1 to 5).map(i => Row(i * 4, i)).toSeq)

  sqlTest(
    "SELECT x.a, y.b FROM oneToTenPruned x JOIN oneToTenPruned y ON x.a = y.b",
    (2 to 10 by 2).map(i => Row(i, i)).toSeq)

  testPruning("SELECT * FROM oneToTenPruned", "a", "b")
  testPruning("SELECT a, b FROM oneToTenPruned", "a", "b")
  testPruning("SELECT b, a FROM oneToTenPruned", "b", "a")
  testPruning("SELECT b, b FROM oneToTenPruned", "b")
  testPruning("SELECT a FROM oneToTenPruned", "a")
  testPruning("SELECT b FROM oneToTenPruned", "b")

  def testPruning(sqlString: String, expectedColumns: String*): Unit = {
    test(s"Columns output ${expectedColumns.mkString(",")}: $sqlString") {
      val queryExecution = sql(sqlString).queryExecution
      val rawPlan = queryExecution.executedPlan.collect {
        case p: execution.PhysicalRDD => p
      } match {
        case Seq(p) => p
        case _ => fail(s"More than one PhysicalRDD found\n$queryExecution")
      }
      val rawColumns = rawPlan.output.map(_.name)
      val rawOutput = rawPlan.execute().first()

      if (rawColumns != expectedColumns) {
        fail(
          s"Wrong column names. Got $rawColumns, Expected $expectedColumns\n" +
          s"Filters pushed: ${FiltersPushed.list.mkString(",")}\n" +
            queryExecution)
      }

      if (rawOutput.size != expectedColumns.size) {
        fail(s"Wrong output row. Got $rawOutput\n$queryExecution")
      }
    }
  }

}

