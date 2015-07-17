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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow, UnsafeRowConverter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.unsafe.PlatformDependent


sealed trait RowFormat
case object UnsafeRowFormat extends RowFormat
case object SafeRowFormat extends RowFormat


/**
 * :: DeveloperApi ::
 * Converts Java-object-based rows into [[UnsafeRow]]s.
 */
@DeveloperApi
case class ToUnsafeRow(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def doExecute(): RDD[UnsafeRow] = {
    // TODO: this will change after SPARK-9022 / #7437
    child.execute().mapPartitions { iter =>
      val toUnsafeConverter = new UnsafeRowConverter(child.output.map(_.dataType).toArray)
      val unsafeRow = new UnsafeRow()
      var buffer = new Array[Byte](64)
      val numFields = child.output.size
      def convert(row: InternalRow): UnsafeRow = {
        val sizeRequirement = toUnsafeConverter.getSizeRequirement(row)
        if (sizeRequirement > buffer.length) {
          buffer = new Array[Byte](sizeRequirement)
        }
        // TODO: how do we want to handle object pools here?
        toUnsafeConverter.writeRow(
          row, buffer, PlatformDependent.BYTE_ARRAY_OFFSET, sizeRequirement, null)
        unsafeRow.pointTo(
          buffer, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, sizeRequirement, null)
        unsafeRow
      }
      iter.map(convert)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Converts [[UnsafeRow]]s back into Java-object-based rows.
 */
@DeveloperApi
case class FromUnsafeRow(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def doExecute(): RDD[InternalRow] = {
    // TODO: this will change after SPARK-9022 / #7437
    child.execute().mapPartitions { iter =>
      val proj = newMutableProjection(null, child.output)()
      iter.map(proj)
    }
  }
}

private[sql] object EnsureRowFormats extends Rule[SparkPlan] {

  private def meetsRequirements(operator: åSparkPlan): Boolean = {
    operator.children.flatMap(_.)
  }

  override def apply(operator: SparkPlan): SparkPlan = {

  }
}