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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Generate, LeafNode, LogicalPlan, MergeRows}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.StructType

/**
 * Updates nullability of Attributes in a resolved LogicalPlan by using the nullability of
 * corresponding Attributes of its children output Attributes. This step is needed because
 * users can use a resolved AttributeReference in the Dataset API and outer joins
 * can change the nullability of an AttributeReference. Without this rule, a nullable column's
 * nullable field can be actually set as non-nullable, which cause illegal optimization
 * (e.g., NULL propagation) and wrong answers.
 * See SPARK-13484 and SPARK-13801 for the concrete queries of this case.
 */
object UpdateAttributeNullability extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    AlwaysProcess.fn, ruleId) {
    // Skip unresolved nodes.
    case p if !p.resolved => p
    // Skip leaf node, as it has no child and no need to update nullability.
    case p: LeafNode => p
    case p: LogicalPlan if p.childrenResolved =>
      val nullabilities = p.children.flatMap(c => c.output).groupBy(_.exprId).map {
        // If there are multiple Attributes having the same ExprId, we need to resolve
        // the conflict of nullable field. We do not really expect this to happen.
        case (exprId, attributes) => exprId -> attributes.exists(_.nullable)
      }
      // The nested nullability of the data types, e.g., struct field nullability, can become
      // wider as well, for instance, when the output of an Expand is widened below, so widen it
      // accordingly. Do not narrow it, as the references to the always nullable output of a
      // stateful streaming operator would then differ in data type from the references in the
      // operator, e.g., once CollapseProject merges them.
      val dataTypes = p.children.flatMap(_.output).map(attr => attr.exprId -> attr.dataType).toMap
      // For an Attribute used by the current LogicalPlan, if it is from its children,
      // we fix the nullable field by using the nullability setting of the corresponding
      // output Attribute from the children.
      val newPlan = p.transformExpressions {
        case attr: Attribute if nullabilities.contains(attr.exprId) =>
          val newAttr = attr.withNullability(nullabilities(attr.exprId))
          val dataType = dataTypes(attr.exprId)
          if (attr.dataType != dataType &&
              DataTypeUtils.equalsIgnoreNullability(attr.dataType, dataType)) {
            newAttr.withDataType(StructType.unionLikeMerge(attr.dataType, dataType))
          } else {
            newAttr
          }
      }
      newPlan match {
        case m: MergeRows => widenOutputNullability(m)
        case e: Expand => widenOutputNullability(e, p.asInstanceOf[Expand])
        case g: Generate => widenOutputNullability(g, p.asInstanceOf[Generate])
        case _ => newPlan
      }
  }

  // MergeRows derives the nullability of its new output attributes from the outputs of its
  // instructions when MERGE is rewritten. The outputs may become nullable later, for instance,
  // when HandleNullInputsForUDF makes a Scala UDF return null for null inputs, so widen the
  // nullability of the output attributes accordingly, keeping their expression IDs.
  private def widenOutputNullability(m: MergeRows): MergeRows = {
    val outputs = m.outputs.map(_.toIndexedSeq)
    val newOutput = m.output.zipWithIndex.map { case (attr, index) =>
      attr.withNullability(attr.nullable || outputs.exists(output => output(index).nullable))
    }
    if (newOutput == m.output) m else m.copy(output = newOutput)
  }

  // Expand derives the nullability of its new output attributes from its projections when it is
  // built, for instance, for grouping sets or UNPIVOT. The projections may become nullable later
  // when this rule updates the attributes they reference, e.g., a column of a Dataset that an
  // outer join makes nullable, so widen the nullability of the output attributes accordingly,
  // keeping their expression IDs. Likewise, merge the nested nullability that the projections
  // gain, e.g., the nullability of a struct field built from such a column, into the data types
  // of the output attributes. Only the projections that this rule changes are merged, so that
  // UNPIVOT keeps taking the nested nullability of the first value with
  // `spark.sql.legacy.unpivot.useFirstValueNestedNullability`.
  private def widenOutputNullability(e: Expand, oldExpand: Expand): Expand = {
    val projections = e.projections.map(_.toIndexedSeq)
    val oldProjections = oldExpand.projections.map(_.toIndexedSeq)
    val newOutput = e.output.zipWithIndex.map { case (attr, index) =>
      val changedDataTypes = projections.zip(oldProjections).collect {
        case (projection, oldProjection)
            if projection(index).dataType != oldProjection(index).dataType =>
          projection(index).dataType
      }
      attr.withNullability(
        attr.nullable || projections.exists(projection => projection(index).nullable))
        .withDataType(changedDataTypes.foldLeft(attr.dataType)(StructType.unionLikeMerge))
    }
    if (newOutput == e.output) e else e.copy(output = newOutput)
  }

  // Generate derives the nullability of its generator output attributes from the element schema
  // of its generator when it is built. The element schema may become nullable later when this
  // rule updates the attributes the generator references, e.g., a column of a Dataset that an
  // outer join makes nullable, so widen the nullability of the generator output attributes
  // accordingly, keeping their expression IDs. Likewise, merge the nested nullability that the
  // element schema gains, e.g., the nullability of a struct field built from such a column, into
  // the data types of the output attributes. Only the fields that this rule changes are
  // considered, as some generators declare all fields nullable while their output attributes
  // keep a narrower nullability, e.g., `ReplicateRows` built for EXCEPT ALL.
  private def widenOutputNullability(g: Generate, oldGenerate: Generate): Generate = {
    val fields = g.generator.elementSchema.fields
    val oldFields = oldGenerate.generator.elementSchema.fields
    val newOutput = g.generatorOutput.zipWithIndex.map { case (attr, index) =>
      val (field, oldField) = (fields(index), oldFields(index))
      val newAttr = attr.withNullability(attr.nullable || (field.nullable && !oldField.nullable))
      if (field.dataType != oldField.dataType) {
        newAttr.withDataType(StructType.unionLikeMerge(attr.dataType, field.dataType))
      } else {
        newAttr
      }
    }
    if (newOutput == g.generatorOutput) g else g.copy(generatorOutput = newOutput)
  }
}
