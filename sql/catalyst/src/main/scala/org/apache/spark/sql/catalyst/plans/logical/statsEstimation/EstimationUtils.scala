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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.types.StringType


object EstimationUtils {

  /** Check if each plan has rowCount in its statistics. */
  def rowCountsExist(conf: CatalystConf, plans: LogicalPlan*): Boolean =
    plans.forall(_.stats(conf).rowCount.isDefined)

  /** Get column stats for output attributes. */
  def getOutputMap(inputMap: AttributeMap[ColumnStat], output: Seq[Attribute])
    : AttributeMap[ColumnStat] = {
    AttributeMap(output.flatMap(a => inputMap.get(a).map(a -> _)))
  }

  def getOutputSize(
      attributes: Seq[Attribute],
      outputRowCount: BigInt,
      attrStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): BigInt = {
    // We assign a generic overhead for a Row object, the actual overhead is different for different
    // Row format.
    val sizePerRow = 8 + attributes.map { attr =>
      if (attrStats.contains(attr)) {
        attr.dataType match {
          case StringType =>
            // UTF8String: base + offset + numBytes
            attrStats(attr).avgLen + 8 + 4
          case _ =>
            attrStats(attr).avgLen
        }
      } else {
        attr.dataType.defaultSize
      }
    }.sum

    // Output size can't be zero, or sizeInBytes of BinaryNode will also be zero
    // (simple computation of statistics returns product of children).
    if (outputRowCount > 0) outputRowCount * sizePerRow else 1
  }
}
