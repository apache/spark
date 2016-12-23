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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.types.StringType

object EstimationUtils extends Logging {

  def ceil(bigDecimal: BigDecimal): BigInt = bigDecimal.setScale(0, RoundingMode.CEILING).toBigInt()

  def getRowSize(attributes: Seq[Attribute], colStats: Map[String, ColumnStat]): Long = {
    attributes.map { attr =>
      if (colStats.contains(attr.name)) {
        attr.dataType match {
          case StringType =>
            // base + offset + numBytes
            colStats(attr.name).avgLen + 8 + 4
          case _ =>
            colStats(attr.name).avgLen
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
    case Cast(ar: AttributeReference, dataType) => Some(ar)
    case _ => None
  }
}
