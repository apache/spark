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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
 * Rewrites [[PythonAggregatorUDAF]] logical operator using a combination of
 * MapInArrow, Aggregate, and FlatMapGroupsInArrow operators.
 *
 * This implements a three-phase aggregation pattern:
 * 1. Partial aggregation (MapInArrow): Applies reduce() on each partition, outputs
 *    (random_key, buffer) pairs
 * 2. Intermediate merge (FlatMapGroupsInArrow): Groups by random key, applies merge()
 * 3. Final merge (FlatMapGroupsInArrow): Groups by actual group keys (or single key),
 *    applies merge() + finish()
 *
 * The key insight is that we must create NEW PythonUDF expressions for each phase,
 * using the correct intermediate attribute references. We extract the PythonFunction
 * from the original UDF expressions and create new PythonUDFs with the correct children.
 */
object RewritePythonAggregatorUDAF extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
    case udaf @ PythonAggregatorUDAF(
        groupingAttributes,
        partialReduceUDF,
        mergeUDF,
        finalMergeUDF,
        resultAttribute,
        child) =>

      // Extract PythonFunction from each UDF expression
      val reduceFunc = extractPythonFunction(partialReduceUDF)
      val mergeFunc = extractPythonFunction(mergeUDF)
      val finalFunc = extractPythonFunction(finalMergeUDF)

      // Step 1: MapInArrow for partial aggregation
      // The reduce UDF takes the child's output and produces (key, buffer)
      val mapInArrowOutput = Seq(
        AttributeReference("key", LongType, nullable = false)(),
        AttributeReference("buffer", BinaryType, nullable = true)()
      )

      // Use the original UDF's children (columns specified by Python in correct order)
      // This preserves the ordering: [grouping_cols..., value_col]
      val originalReduceChildren = partialReduceUDF.asInstanceOf[PythonUDF].children
      val reduceUDFWithCorrectChildren = createPythonUDF(
        reduceFunc,
        partialReduceUDF,
        originalReduceChildren,  // Preserve original column ordering from Python
        StructType(mapInArrowOutput.map(a => StructField(a.name, a.dataType, a.nullable)))
      )

      val mapInArrow = MapInArrow(
        reduceUDFWithCorrectChildren,
        mapInArrowOutput,
        child,
        isBarrier = false,
        profile = None
      )

      // Get actual output attributes from mapInArrow
      val keyAttr = mapInArrow.output.head
      val bufferAttr = mapInArrow.output(1)

      // Step 2: Group by random key and apply merge via FlatMapGroupsInArrow
      val mergeOutputAttrs = Seq(AttributeReference("buffer", BinaryType, nullable = true)())

      // Create merge UDF that takes (key, buffer) and produces (buffer)
      val mergeUDFWithCorrectChildren = createPythonUDF(
        mergeFunc,
        mergeUDF,
        Seq(keyAttr, bufferAttr),  // Reference the mapInArrow output
        StructType(mergeOutputAttrs.map(a => StructField(a.name, a.dataType, a.nullable)))
      )

      val flatMapMerge = FlatMapGroupsInArrow(
        Seq(keyAttr),
        mergeUDFWithCorrectChildren,
        mergeOutputAttrs,
        mapInArrow
      )
      val mergedBufferAttr = flatMapMerge.output.head

      // Step 3: Add a constant key for final grouping
      val finalKeyAlias = Alias(Literal(0L), "final_key")()
      val projectWithFinalKey = Project(
        Seq(finalKeyAlias, mergedBufferAttr),
        flatMapMerge
      )
      val finalKeyAttr = projectWithFinalKey.output.head
      val finalBufferAttr = projectWithFinalKey.output(1)

      // Step 4: Group by final key and apply final merge + finish
      val finalOutput = if (groupingAttributes.nonEmpty) {
        groupingAttributes.map(_.toAttribute) :+ resultAttribute
      } else {
        Seq(resultAttribute)
      }

      // Create final UDF that takes (final_key, buffer) and produces the result
      val finalUDFWithCorrectChildren = createPythonUDF(
        finalFunc,
        finalMergeUDF,
        Seq(finalKeyAttr, finalBufferAttr),  // Reference the project output
        StructType(finalOutput.map(a => StructField(a.name, a.dataType, a.nullable)))
      )

      val flatMapFinal = FlatMapGroupsInArrow(
        Seq(finalKeyAttr),
        finalUDFWithCorrectChildren,
        finalOutput,
        projectWithFinalKey
      )

      // Step 5: Project out the final key if it was just for grouping
      val result = if (groupingAttributes.isEmpty) {
        Project(Seq(resultAttribute), flatMapFinal)
      } else {
        Project(finalOutput, flatMapFinal)
      }

      val attrMapping = udaf.output.zip(result.output)
      result -> attrMapping
  }

  /**
   * Extract PythonFunction from a PythonUDF expression.
   */
  private def extractPythonFunction(expr: Expression): PythonFunction = {
    expr match {
      case udf: PythonUDF => udf.func
      case other =>
        throw new IllegalArgumentException(
          s"Expected PythonUDF but got ${other.getClass.getSimpleName}")
    }
  }

  /**
   * Create a new PythonUDF with the given function but different children (attribute references).
   */
  private def createPythonUDF(
      func: PythonFunction,
      originalUDF: Expression,
      newChildren: Seq[Expression],
      returnType: DataType): PythonUDF = {
    originalUDF match {
      case udf: PythonUDF =>
        PythonUDF(
          name = udf.name,
          func = func,
          dataType = returnType,
          children = newChildren,
          evalType = udf.evalType,
          udfDeterministic = udf.udfDeterministic
        )
      case other =>
        throw new IllegalArgumentException(
          s"Expected PythonUDF but got ${other.getClass.getSimpleName}")
    }
  }
}
