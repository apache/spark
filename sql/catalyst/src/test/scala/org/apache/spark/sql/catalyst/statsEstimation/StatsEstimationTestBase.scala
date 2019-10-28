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

package org.apache.spark.sql.catalyst.statsEstimation

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}


trait StatsEstimationTestBase extends SparkFunSuite {

  var originalValue: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Enable stats estimation based on CBO.
    originalValue = SQLConf.get.getConf(SQLConf.CBO_ENABLED)
    SQLConf.get.setConf(SQLConf.CBO_ENABLED, true)
  }

  override def afterAll(): Unit = {
    SQLConf.get.setConf(SQLConf.CBO_ENABLED, originalValue)
    super.afterAll()
  }

  def getColSize(attribute: Attribute, colStat: ColumnStat): Long = attribute.dataType match {
    // For UTF8String: base + offset + numBytes
    case StringType => colStat.avgLen.getOrElse(attribute.dataType.defaultSize.toLong) + 8 + 4
    case _ => colStat.avgLen.getOrElse(attribute.dataType.defaultSize)
  }

  def attr(colName: String): AttributeReference = AttributeReference(colName, IntegerType)()

  /** Convert (column name, column stat) pairs to an AttributeMap based on plan output. */
  def toAttributeMap(colStats: Seq[(String, ColumnStat)], plan: LogicalPlan)
    : AttributeMap[ColumnStat] = {
    val nameToAttr: Map[String, Attribute] = plan.output.map(a => (a.name, a)).toMap
    AttributeMap(colStats.map(kv => nameToAttr(kv._1) -> kv._2))
  }

  /** Get a test ColumnStat with given distinctCount and nullCount */
  def rangeColumnStat(distinctCount: Int, nullCount: Int): ColumnStat =
    ColumnStat(distinctCount = Some(distinctCount),
      min = Some(1), max = Some(distinctCount),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
}

/**
 * This class is used for unit-testing. It's a logical plan whose output and stats are passed in.
 */
case class StatsTestPlan(
    outputList: Seq[Attribute],
    rowCount: BigInt,
    attributeStats: AttributeMap[ColumnStat],
    size: Option[BigInt] = None) extends LeafNode {
  override def output: Seq[Attribute] = outputList
  override def computeStats(): Statistics = Statistics(
    // If sizeInBytes is useless in testing, we just use a fake value
    sizeInBytes = size.getOrElse(Int.MaxValue),
    rowCount = Some(rowCount),
    attributeStats = attributeStats)
}
