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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, analysis}
import org.apache.spark.sql.types.{StructField, StructType}

object LocalRelation {
  def apply(output: Attribute*): LocalRelation = new LocalRelation(output)

  def apply(output1: StructField, output: StructField*): LocalRelation = {
    new LocalRelation(StructType(output1 +: output).toAttributes)
  }

  def fromExternalRows(output: Seq[Attribute], data: Seq[Row]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }

  def fromProduct(output: Seq[Attribute], data: Seq[Product]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }
}

case class LocalRelation(output: Seq[Attribute], data: Seq[InternalRow] = Nil)
  extends LeafNode with analysis.MultiInstanceRelation {

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    LocalRelation(output.map(_.newInstance()), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = {
    EliminateSubQueries(plan) match {
      case LocalRelation(otherOutput, otherData) =>
        otherOutput.map(_.dataType) == output.map(_.dataType) && otherData == data
      case _ => false
    }
  }

  override lazy val statistics =
    Statistics(sizeInBytes = output.map(_.dataType.defaultSize).sum * data.length)
}
