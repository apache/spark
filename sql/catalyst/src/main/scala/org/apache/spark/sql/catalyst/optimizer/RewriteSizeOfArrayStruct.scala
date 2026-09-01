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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, MapKeys, MapValues, Size}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * Computing the length of an array of structs (`size(arr)` / `array_size(arr)`) only requires the
 * array's structural information (offsets / repetition levels), not the values of the element
 * struct's fields. However, when `size` is applied to a whole `ARRAY<STRUCT<...>>` column, the
 * column is referenced as a whole, which prevents [[NestedColumnAliasing]] (and the subsequent
 * schema pruning at the file format reader) from pruning the unused nested fields. As a result,
 * all nested fields are read from Parquet/ORC, causing large and unnecessary I/O (SPARK-58735).
 *
 * This rule rewrites `size(arr)` into `size(arr.<field>)`, picking a single (cheapest) field of the
 * element struct. Because extracting a field from an array of structs preserves the array's length
 * and null-ness, the result of `size` is unchanged, while the extra [[GetArrayStructFields]] lets
 * the existing nested column pruning read only that one field.
 *
 * Example:
 * {{{
 *   size(events)  =>  size(events.<smallest_field>)
 * }}}
 */
object RewriteSizeOfArrayStruct extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.nestedSchemaPruningEnabled) {
      plan
    } else {
      plan.transformAllExpressions {
        case s @ Size(child, _) if canRewrite(child) =>
          val array = child.dataType.asInstanceOf[ArrayType]
          val struct = array.elementType.asInstanceOf[StructType]
          // Pick the smallest field by default size, mirroring [[GenerateOptimization]]. Extracting
          // any field preserves the array length, so the result of `size` is unchanged.
          val (field, ordinal) =
            struct.fields.zipWithIndex.minBy { case (f, _) => f.dataType.defaultSize }
          val extractor = GetArrayStructFields(
            child, field, ordinal, struct.length, array.containsNull || field.nullable)
          s.withNewChildren(Seq(extractor))
      }
    }
  }

  /**
   * We only rewrite when the child is an array of a struct with more than one field (with a single
   * field there is nothing to prune) that is rooted at a column reference (so nested column pruning
   * can actually prune it), and is not already a field extraction on an array of structs (which
   * keeps this rule idempotent).
   */
  private def canRewrite(child: Expression): Boolean = {
    !child.isInstanceOf[GetArrayStructFields] && isColumnReference(child) && (child.dataType match {
      case ArrayType(st: StructType, _) => st.length > 1
      case _ => false
    })
  }

  /**
   * Returns true if the expression is built solely from a base column reference and value
   * extractors, i.e. it reads from a scan column that nested column pruning can prune.
   */
  private def isColumnReference(e: Expression): Boolean = e match {
    case _: AttributeReference => true
    case g: GetStructField => isColumnReference(g.child)
    case g: GetArrayStructFields => isColumnReference(g.child)
    case g: GetArrayItem => isColumnReference(g.child)
    case g: GetMapValue => isColumnReference(g.child)
    case m: MapValues => isColumnReference(m.child)
    case m: MapKeys => isColumnReference(m.child)
    case _ => false
  }
}
