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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, ArrayTransform, CaseWhen, Coalesce, CreateArray, CreateMap, CreateNamedStruct, EqualTo, ExpectsInputTypes, Expression, GetStructField, If, IsNull, KnownFloatingPointNormalized, LambdaFunction, Literal, NamedLambdaVariable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * We need to take care of special floating numbers (NaN and -0.0) in several places:
 *   1. When compare values, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   2. In aggregate grouping keys, different NaNs should belong to the same group, -0.0 and 0.0
 *      should belong to the same group.
 *   3. In join keys, different NaNs should be treated as same, `-0.0` and `0.0` should be
 *      treated as same.
 *   4. In window partition keys, different NaNs should belong to the same partition, -0.0 and 0.0
 *      should belong to the same partition.
 *
 * Case 1 is fine, as we handle NaN and -0.0 well during comparison. For complex types, we
 * recursively compare the fields/elements, so it's also fine.
 *
 * Case 2, 3 and 4 are problematic, as Spark SQL turns grouping/join/window partition keys into
 * binary `UnsafeRow` and compare the binary data directly. Different NaNs have different binary
 * representation, and the same thing happens for -0.0 and 0.0.
 *
 * This rule normalizes NaN and -0.0 in window partition keys, join keys and aggregate grouping
 * keys.
 *
 * Ideally we should do the normalization in the physical operators that compare the
 * binary `UnsafeRow` directly. We don't need this normalization if the Spark SQL execution engine
 * is not optimized to run on binary data. This rule is created to simplify the implementation, so
 * that we have a single place to do normalization, which is more maintainable.
 *
 * Note that, this rule must be executed at the end of optimizer, because the optimizer may create
 * new joins(the subquery rewrite) and new join conditions(the join reorder).
 */
object NormalizeFloatingNumbers extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _ => plan transform {
      case w: Window if w.partitionSpec.exists(p => needNormalize(p)) =>
        // Although the `windowExpressions` may refer to `partitionSpec` expressions, we don't need
        // to normalize the `windowExpressions`, as they are executed per input row and should take
        // the input row as it is.
        w.copy(partitionSpec = w.partitionSpec.map(normalize))

      // Only hash join and sort merge join need the normalization. Here we catch all Joins with
      // join keys, assuming Joins with join keys are always planned as hash join or sort merge
      // join. It's very unlikely that we will break this assumption in the near future.
      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _)
          // The analyzer guarantees left and right joins keys are of the same data type. Here we
          // only need to check join keys of one side.
          if leftKeys.exists(k => needNormalize(k)) =>
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

  /**
   * Short circuit if the underlying expression is already normalized
   */
  private def needNormalize(expr: Expression): Boolean = expr match {
    case KnownFloatingPointNormalized(_) => false
    case _ => needNormalize(expr.dataType)
  }

  private def needNormalize(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case StructType(fields) => fields.exists(f => needNormalize(f.dataType))
    case ArrayType(et, _) => needNormalize(et)
    // Currently MapType is not comparable and analyzer should fail earlier if this case happens.
    case _: MapType =>
      throw new IllegalStateException("grouping/join/window partition keys cannot be map type.")
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr) => expr

    case a: Alias =>
      a.withNewChildren(Seq(normalize(a.child)))

    case CreateNamedStruct(children) =>
      CreateNamedStruct(children.map(normalize))

    case CreateArray(children, useStringTypeWhenEmpty) =>
      CreateArray(children.map(normalize), useStringTypeWhenEmpty)

    case CreateMap(children, useStringTypeWhenEmpty) =>
      CreateMap(children.map(normalize), useStringTypeWhenEmpty)

    case _ if expr.dataType == FloatType || expr.dataType == DoubleType =>
      KnownFloatingPointNormalized(NormalizeNaNAndZero(expr))

    case If(cond, trueValue, falseValue) =>
      If(cond, normalize(trueValue), normalize(falseValue))

    case CaseWhen(branches, elseVale) =>
      CaseWhen(branches.map(br => (br._1, normalize(br._2))), elseVale.map(normalize))

    case Coalesce(children) =>
      Coalesce(children.map(normalize))

    case _ if expr.dataType.isInstanceOf[StructType] =>
      val fields = expr.dataType.asInstanceOf[StructType].fieldNames.zipWithIndex.map {
        case (name, i) => Seq(Literal(name), normalize(GetStructField(expr, i)))
      }
      val struct = CreateNamedStruct(fields.flatten.toSeq)
      KnownFloatingPointNormalized(If(IsNull(expr), Literal(null, struct.dataType), struct))

    case _ if expr.dataType.isInstanceOf[ArrayType] =>
      val ArrayType(et, containsNull) = expr.dataType
      val lv = NamedLambdaVariable("arg", et, containsNull)
      val function = normalize(lv)
      KnownFloatingPointNormalized(ArrayTransform(expr, LambdaFunction(function, Seq(lv))))

    case _ => throw new IllegalStateException(s"fail to normalize $expr")
  }

  val FLOAT_NORMALIZER: Any => Any = (input: Any) => {
    val f = input.asInstanceOf[Float]
    if (f.isNaN) {
      Float.NaN
    } else if (f == -0.0f) {
      0.0f
    } else {
      f
    }
  }

  val DOUBLE_NORMALIZER: Any => Any = (input: Any) => {
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

case class NormalizeNaNAndZero(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(FloatType, DoubleType))

  private lazy val normalizer: Any => Any = child.dataType match {
    case FloatType => NormalizeFloatingNumbers.FLOAT_NORMALIZER
    case DoubleType => NormalizeFloatingNumbers.DOUBLE_NORMALIZER
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
