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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, ArrayTransform, CreateArray, CreateMap, CreateNamedStruct, CreateNamedStructUnsafe, CreateStruct, EqualTo, ExpectsInputTypes, Expression, GetStructField, LambdaFunction, NamedLambdaVariable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * We need to take care of special floating numbers (NaN and -0.0) in several places:
 *   1. When compare values, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   2. In GROUP BY, different NaNs should belong to the same group, -0.0 and 0.0 should belong
 *      to the same group.
 *   3. In join keys, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   4. In window partition keys, different NaNs should be treated as same, `-0.0` and `0.0`
 *      should be treated as same.
 *
 * Case 1 is fine, as we handle NaN and -0.0 well during comparison. For complex types, we
 * recursively compare the fields/elements, so it's also fine.
 *
 * Case 2, 3 and 4 are problematic, as they compare `UnsafeRow` binary directly, and different
 * NaNs have different binary representation, and the same thing happens for -0.0 and 0.0.
 *
 * This rule normalizes NaN and -0.0 in Window partition keys, Join keys and Aggregate grouping
 * expressions.
 *
 * Note that, this rule should be an analyzer rule, as it must be applied to make the query result
 * corrected. Currently it's executed as an optimizer rule, because the optimizer may create new
 * joins(for subquery) and reorder joins(may change the join condition), and this rule needs to be
 * executed at the end.
 */
object NormalizeFloatingNumbers extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as we only need to run this rule once.
    case _: Subquery => plan

    case _ => plan transform {
      case w: Window if w.partitionSpec.exists(p => needNormalize(p.dataType)) =>
        w.copy(partitionSpec = w.partitionSpec.map(normalize))

      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _)
        if leftKeys.exists(k => needNormalize(k.dataType)) =>
        val newLeftJoinKeys = leftKeys.map(normalize)
        val newRightJoinKeys = rightKeys.map(normalize)
        val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
          case (l, r) => EqualTo(l, r)
        } ++ condition
        j.copy(condition = Some(newConditions.reduce(And)))

      // TODO: ideally Aggregate should also be handled here, but its grouping expressions are
      // mixed in its aggregate expressions. It's unreliable to change the grouping expressions
      // here. For now we normalize grouping expressions in `AggUtils` during planning.
    }
  }

  private def needNormalize(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case StructType(fields) => fields.exists(f => needNormalize(f.dataType))
    case ArrayType(et, _) => needNormalize(et)
    // We don't need to handle MapType here, as it's not comparable.
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if expr.dataType == FloatType || expr.dataType == DoubleType =>
      NormalizeNaNAndZero(expr)

    case CreateNamedStruct(children) =>
      CreateNamedStruct(children.map(normalize))

    case CreateNamedStructUnsafe(children) =>
      CreateNamedStructUnsafe(children.map(normalize))

    case CreateArray(children) =>
      CreateArray(children.map(normalize))

    case CreateMap(children) =>
      CreateMap(children.map(normalize))

    case a: Alias if needNormalize(a.dataType) =>
      a.withNewChildren(Seq(normalize(a.child)))

    case _ if expr.dataType.isInstanceOf[StructType] && needNormalize(expr.dataType) =>
      val fields = expr.dataType.asInstanceOf[StructType].fields.indices.map { i =>
        normalize(GetStructField(expr, i))
      }
      CreateStruct(fields)

    case _ if expr.dataType.isInstanceOf[ArrayType] && needNormalize(expr.dataType) =>
      val ArrayType(et, containsNull) = expr.dataType
      val lv = NamedLambdaVariable("arg", et, containsNull)
      val function = normalize(lv)
      ArrayTransform(expr, LambdaFunction(function, Seq(lv)))

    case _ => expr
  }
}

case class NormalizeNaNAndZero(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(FloatType, DoubleType))

  private lazy val normalizer: Any => Any = child.dataType match {
    case FloatType => (input: Any) => {
      val f = input.asInstanceOf[Float]
      if (f.isNaN) {
        Float.NaN
      } else if (f == -0.0f) {
        0.0f
      } else {
        f
      }
    }

    case DoubleType => (input: Any) => {
      val d = input.asInstanceOf[Double]
      if (d.isNaN) {
        Double.NaN
      } else if (d == -0.0d) {
        0.0d
      } else {
        d
      }
    }
  }

  override def nullSafeEval(input: Any): Any = {
    normalizer(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val codeToNormalize = child.dataType match {
      case FloatType => (f: String) => {
        s"""
           |if (Float.isNaN($f)) {
           |  ${ev.value} = Float.NaN;
           |} else if ($f == -0.0f) {
           |  ${ev.value} = 0.0f;
           |} else {
           |  ${ev.value} = $f;
           |}
         """.stripMargin
      }

      case DoubleType => (d: String) => {
        s"""
           |if (Double.isNaN($d)) {
           |  ${ev.value} = Double.NaN;
           |} else if ($d == -0.0d) {
           |  ${ev.value} = 0.0d;
           |} else {
           |  ${ev.value} = $d;
           |}
         """.stripMargin
      }
    }

    nullSafeCodeGen(ctx, ev, codeToNormalize)
  }
}
