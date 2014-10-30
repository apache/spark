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

import org.apache.spark.sql.catalyst.expressions.{Row => _, _}
import org.apache.spark.sql._

class FilteredScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleFilteredScan(parameters("from").toInt, parameters("to").toInt)(sqlContext)
  }
}

case class SimpleFilteredScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends BaseRelation with FilteredScan {

  override def schema =
    StructType(
      StructField("a", IntegerType, nullable = false) ::
      StructField("b", IntegerType, nullable = false) :: Nil)

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]) = {
    val rowBuilders = requiredColumns.map(_.name).map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i * 2)
    }

    val filter = filters.collect {
      case Seq(EqualTo(a: AttributeReference, l: Literal)) if a.name == "a" =>
        (i: Int) => i == l.value
      case Seq(EqualTo(l: Literal, a: AttributeReference)) if a.name == "a" =>
        (i: Int) => i == l.value
    }.headOption.getOrElse((_: Int) => true)

    sqlContext.sparkContext.parallelize(from to to).filter(filter).map(i =>
      Row.fromSeq(rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)))
  }
}

class FilteredScanSuite extends DataSourceTest {

  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenFiltered
        |USING org.apache.spark.sql.sources.FilteredScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  sqlTest(
    "SELECT * FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT b, a FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2, i)).toSeq)

  sqlTest(
    "SELECT a FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT a * 2 FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT A AS b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT x.b, y.a FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
    (1 to 5).map(i => Row(i * 4, i)).toSeq)

  sqlTest(
    "SELECT x.a, y.b FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
    (2 to 10 by 2).map(i => Row(i, i)).toSeq)

  sqlTest(
    "SELECT * FROM oneToTenFiltered WHERE a = 1",
    Seq(1).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT * FROM oneToTenFiltered WHERE b = 2",
    Seq(1).map(i => Row(i, i * 2)).toSeq)
}

