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

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{getValue, javaType}
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, MapData, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, DataType, MapType}

/**
 * When comparing two maps, we have to make sure two maps have the same key value pairs but
 * with different key ordering are equal.
 * For example, Map('a' -> 1, 'b' -> 2) equals to Map('b' -> 2, 'a' -> 1).
 *
 * We have to specially handle this in grouping/join/window because Spark SQL turns
 * grouping/join/window partition keys into binary `UnsafeRow` and compare the
 * binary data directly instead of using MapType's ordering. So in these cases, we have
 * to insert an expression to sort map entries by key.
 *
 * Note that, this rule must be executed at the end of optimizer, because the optimizer may create
 * new joins(the subquery rewrite) and new join conditions(the join reorder).
 */
object NormalizeMapType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case w: Window if w.partitionSpec.exists(p => needNormalize(p)) =>
      w.copy(partitionSpec = w.partitionSpec.map(normalize))

    case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _)
      // The analyzer guarantees left and right joins keys are of the same data type.
      if leftKeys.exists(k => needNormalize(k)) =>
      val newLeftJoinKeys = leftKeys.map(normalize)
      val newRightJoinKeys = rightKeys.map(normalize)
      val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
        case (l, r) => EqualTo(l, r)
      } ++ condition
      j.copy(condition = Some(newConditions.reduce(And)))
  }

  private def needNormalize(expr: Expression): Boolean = expr match {
    case SortMapKey(_) => false
    case e if e.dataType.isInstanceOf[MapType] => true
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr) => expr
    case e if e.dataType.isInstanceOf[MapType] =>
      SortMapKey(e)
  }
}

case class SortMapKey(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  private lazy val MapType(keyType, valueType, valueContainsNull) = dataType.asInstanceOf[MapType]
  private lazy val keyOrdering: Ordering[Any] = TypeUtils.getInterpretedOrdering(keyType)
  private lazy val mapBuilder = new ArrayBasedMapBuilder(keyType, valueType)

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = child.dataType

  override def nullSafeEval(input: Any): Any = {
    val childMap = input.asInstanceOf[MapData]
    val keys = childMap.keyArray()
    val values = childMap.valueArray()
    val sortedKeyIndex = (0 until childMap.numElements()).toArray.sorted(new Ordering[Int] {
      override def compare(a: Int, b: Int): Int = {
        keyOrdering.compare(keys.get(a, keyType), keys.get(b, keyType))
      }
    })

    var i = 0
    while (i < childMap.numElements()) {
      val index = sortedKeyIndex(i)
      mapBuilder.put(
        keys.get(index, keyType),
        if (values.isNullAt(index)) null else values.get(index, valueType))

      i += 1
    }

    mapBuilder.build()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val initIndexArrayFunc = ctx.freshName("initIndexArray")
    val numElements = ctx.freshName("numElements")
    val sortedKeyIndex = ctx.freshName("sortedKeyIndex")
    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    val idx = ctx.freshName("idx")
    val index = ctx.freshName("index")
    val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
    ctx.addNewFunction(initIndexArrayFunc,
      s"""
         |private Integer[] $initIndexArrayFunc(int n) {
         |  Integer[] arr = new Integer[n];
         |  for (int i = 0; i < n; i++) {
         |    arr[i] = i;
         |  }
         |  return arr;
         |}""".stripMargin)

    val codeToNormalize = (f: String) => {
      s"""
         |int $numElements = $f.numElements();
         |Integer[] $sortedKeyIndex = $initIndexArrayFunc($numElements);
         |final ArrayData $keyArray = $f.keyArray();
         |final ArrayData $valueArray = $f.valueArray();
         |java.util.Arrays.sort($sortedKeyIndex, new java.util.Comparator<Integer>() {
         |   @Override
         |   public int compare(Object a, Object b) {
         |     int indexA = ((Integer)a).intValue();
         |     int indexB = ((Integer)b).intValue();
         |     ${javaType(keyType)} keyA = ${getValue(keyArray, keyType, "indexA")};
         |     ${javaType(keyType)} keyB = ${getValue(keyArray, keyType, "indexB")};
         |     return ${ctx.genComp(keyType, "keyA", "keyB")};
         |   }
         |});
         |
         |for (int $idx = 0; $idx < $numElements; $idx++) {
         |  Integer $index = $sortedKeyIndex[$idx];
         |  $builderTerm.put(
         |    ${getValue(keyArray, keyType, index)},
         |    $valueArray.isNullAt($index) ? null : ${getValue(valueArray, valueType, index)});
         |}
         |
         |${ev.value} = $builderTerm.build();
         |""".stripMargin
    }

    nullSafeCodeGen(ctx, ev, codeToNormalize)
  }
}
