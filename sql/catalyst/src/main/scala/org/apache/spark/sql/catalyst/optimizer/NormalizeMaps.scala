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

import scala.math.Ordering

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator._
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.types._

/**
 * When comparing two maps, we have to make sure two maps having the same key value pairs but
 * with different key ordering are equal (e.g., Map('a' -> 1, 'b' -> 2) should equal to
 * Map('b' -> 2, 'a' -> 1). To make sure the assumption holds,
 * this rule inserts a [[SortMapKeys]] expression to sort map entries by keys.
 *
 * NOTE: this rule must be executed at the end of the optimizer because it may create
 * new joins (the subquery rewrite) and new join conditions (the join reorder).
 */
object NormalizeMaps extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    // The analyzer guarantees left and right types are the same, so
    // we only need to check a type in one side.
    case cmp @ BinaryComparison(left, right) if needNormalize(left) =>
      cmp.withNewChildren(SortMapKeys(left) :: SortMapKeys(right) :: Nil)

    case In(value, list) if needNormalize(value) =>
      In(SortMapKeys(value), list.map(SortMapKeys))

    case in @ InSet(value, list) if needNormalize(value) =>
      val newHset = list.map(c => SortMapKeys(Literal(c, in.child.dataType)).eval())
      InSet(SortMapKeys(value), newHset)

    case sort: SortOrder if needNormalize(sort.child) =>
      sort.copy(child = SortMapKeys(sort.child))
  }.transform {
    case w: Window if w.partitionSpec.exists(p => needNormalize(p)) =>
      w.copy(partitionSpec = w.partitionSpec.map(normalize))

    // TODO: `NormalizeMaps` has the same restriction with `NormalizeFloatingNumbers`;
    // ideally Aggregate should also be handled here, but its grouping expressions are
    // mixed in its aggregate expressions. It's unreliable to change the grouping expressions
    // here. For now we normalize grouping expressions in `AggUtils` during planning.
  }

  private def needNormalize(expr: Expression): Boolean = expr match {
    case SortMapKeys(_) => false
    case _ => needNormalize(expr.dataType)
  }

  private def needNormalize(dt: DataType): Boolean = dt match {
    case StructType(fields) => fields.exists(f => needNormalize(f.dataType))
    case ArrayType(et, _) => needNormalize(et)
    case _: MapType => true
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr) => expr
    case _ => SortMapKeys(expr)
  }
}

/**
 * This expression sorts all maps in an expression's result. This expression enables the use of
 * maps in comparisons and equality operations.
 */
case class SortMapKeys(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(ArrayType, MapType, StructType))

  override def dataType: DataType = child.dataType

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  private def createFuncToSortRecursively(dt: DataType): Any => Any = dt match {
    case m @ MapType(keyType, valueType, _) =>
      val sf = createFuncToSortRecursively(valueType)
      val keyOrdering = new Ordering[(Any, Any)] {
        val ordering = TypeUtils.getInterpretedOrdering(keyType)
        override def compare(x: (Any, Any), y: (Any, Any)): Int = ordering.compare(x._1, y._1)

      }
      (data: Any) => {
        val input = data.asInstanceOf[MapData]
        val length = input.numElements()
        val keys = input.keyArray()
        val values = input.valueArray()
        val buffer = Array.ofDim[(Any, Any)](length)
        var i = 0
        while (i < length) {
          // Map keys cannot contain map types (See `TypeUtils.checkForMapKeyType`),
          // so we recursively sort values only.
          val k = keys.get(i, m.keyType)
          val v = if (!values.isNullAt(i)) {
            sf(values.get(i, m.valueType))
          } else {
            null
          }
          buffer(i) = k -> v
          i += 1
        }

        java.util.Arrays.sort(buffer, keyOrdering)

        ArrayBasedMapData(buffer.toIterator, length, identity, identity)
      }

    case ArrayType(dt, _) =>
      val sf = createFuncToSortRecursively(dt)
      (data: Any) => {
        val input = data.asInstanceOf[ArrayData]
        val length = input.numElements()
        val output = Array.ofDim[Any](length)
        var i = 0
        while (i < length) {
          if (!input.isNullAt(i)) {
            output(i) = sf(input.get(i, dt))
          } else {
            output(i) = null
          }
          i += 1
        }
        new GenericArrayData(output)
      }

    case StructType(fields) =>
      val fs = fields.map { field =>
        val sf = createFuncToSortRecursively(field.dataType)
        (input: InternalRow, i: Int) => {
          sf(input.get(i, field.dataType))
        }
      }
      val length = fields.length
      (data: Any) => {
        val input = data.asInstanceOf[InternalRow]
        val output = Array.ofDim[Any](length)
        var i = 0
        while (i < length) {
          if (!input.isNullAt(i)) {
            output(i) = fs(i)(input, i)
          } else {
            output(i) = null
          }
          i += 1
        }
        new GenericInternalRow(output)
      }

    case _ =>
      identity
  }

  @transient private[this] lazy val sortFunc = {
    createFuncToSortRecursively(dataType)
  }

  override def nullSafeEval(input: Any): Any = sortFunc(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // TODO: we should code generate this
    val tf = ctx.addReferenceObj("sortFunc", sortFunc, classOf[Any => Any].getCanonicalName)
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = (${javaType(dataType)})$tf.apply($eval);"
    })
  }
}
