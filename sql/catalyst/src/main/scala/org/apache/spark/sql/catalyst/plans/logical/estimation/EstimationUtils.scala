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

package org.apache.spark.sql.catalyst.plans.logical.estimation

import scala.math.BigDecimal.RoundingMode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.types.StringType


object EstimationUtils extends Logging {

  def ceil(bigDecimal: BigDecimal): BigInt = bigDecimal.setScale(0, RoundingMode.CEILING).toBigInt()

  def getRowSize(attributes: Seq[Attribute], attrStats: AttributeMap[ColumnStat]): Long = {
    // We assign a generic overhead for a Row object, the actual overhead is different for different
    // Row format.
    8 + attributes.map { attr =>
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
  }
}

/** Attribute Reference extractor */
object ExtractAttr {
  def unapply(exp: Expression): Option[AttributeReference] = exp match {
    case ar: AttributeReference => Some(ar)
    case _ => None
  }
}
