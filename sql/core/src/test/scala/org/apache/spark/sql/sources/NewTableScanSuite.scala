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
import java.sql.{Timestamp, Date}
import org.apache.spark.sql.execution.RDDConversions

case class PrimaryData(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    decimalField: BigDecimal,
    date: Date,
    timestampField: Timestamp)

class AllDataTypesScanSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: Option[StructType] = None): BaseRelation = {
    AllDataTypesScan(parameters("from").toInt, parameters("TO").toInt, schema)(sqlContext)
  }
}

case class AllDataTypesScan(
    from: Int,
    to: Int,
    userSpecifiedSchema: Option[StructType])(@transient val sqlContext: SQLContext)
  extends TableScan {

  override def schema = userSpecifiedSchema.get

  override def buildScan() = {
    val rdd = sqlContext.sparkContext.parallelize(from to to).map { i =>
      PrimaryData(
        i.toString,
        i,
        i.toLong,
        i.toFloat,
        i.toDouble,
        i.toShort,
        i.toByte,
        true,
        BigDecimal(i),
        new Date(12345),
        new Timestamp(12345))
    }

    RDDConversions.productToRowRdd(rdd, schema)
  }

}

class NewTableScanSuite extends DataSourceTest {
  import caseInsensisitiveContext._

  var records = (1 to 10).map { i =>
      Row(
        i.toString,
        i,
        i.toLong,
        i.toFloat,
        i.toDouble,
        i.toShort,
        i.toByte,
        true,
        BigDecimal(i),
        new Date(12345),
        new Timestamp(12345))
    }.toSeq

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTen(stringField stRIng, intField iNt, longField Bigint,
        |floatField flOat, doubleField doubLE, shortField smaLlint, byteField tinyint,
        |booleanField boolean, decimalField decimal(10,2), dateField dAte, timestampField tiMestamp)
        |USING org.apache.spark.sql.sources.AllDataTypesScanSource
        |OPTIONS (
        |  From '1',
        |  To '10'
        |)
      """.stripMargin)
  }

  sqlTest(
    "SELECT * FROM oneToTen",
    records)

  sqlTest(
    "SELECT stringField FROM oneToTen",
    (1 to 10).map(i =>Row(i.toString)).toSeq)

  sqlTest(
    "SELECT intField FROM oneToTen WHERE intField < 5",
    (1 to 4).map(Row(_)).toSeq)

  sqlTest(
    "SELECT longField * 2 FROM oneToTen",
    (1 to 10).map(i => Row(i * 2.toLong)).toSeq)

  sqlTest(
    """SELECT a.floatField, b.floatField FROM oneToTen a JOIN oneToTen b
      |ON a.floatField = b.floatField + 1""".stripMargin,
    (2 to 10).map(i => Row(i.toFloat, i - 1.toFloat)).toSeq)

  sqlTest(
    "SELECT distinct(a.dateField) FROM oneToTen a",
    Some(new Date(12345)).map(Row(_)).toSeq)

  sqlTest(
    "SELECT distinct(a.timestampField) FROM oneToTen a",
    Some(new Timestamp(12345)).map(Row(_)).toSeq)

}
