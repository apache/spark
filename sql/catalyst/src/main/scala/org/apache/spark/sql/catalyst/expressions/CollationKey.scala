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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.internal.types.{AbstractArrayType, StringTypeAnyCollation}
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

case class CollationKey(expr: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringTypeAnyCollation, AbstractArrayType(StringTypeAnyCollation)))
  override def dataType: DataType = expr.dataType

  final lazy val collationId: Int = dataType match {
    case _: StringType =>
      dataType.asInstanceOf[StringType].collationId
    case ArrayType(_: StringType, _) =>
      val arr = dataType.asInstanceOf[ArrayType]
      arr.elementType.asInstanceOf[StringType].collationId
  }

  override def nullSafeEval(input: Any): Any = dataType match {
    case _: StringType =>
      getCollationKey(input.asInstanceOf[UTF8String])
    case ArrayType(_: StringType, _) =>
      input match {
        case arr: Array[UTF8String] =>
          arr.map(getCollationKey)
        case arr: GenericArrayData =>
          val result = new Array[UTF8String](arr.numElements())
          for (i <- 0 until arr.numElements()) {
            result(i) = getCollationKey(arr.getUTF8String(i))
          }
          new GenericArrayData(result)
        case _ =>
          None
      }
  }

  def getCollationKey(str: UTF8String): UTF8String = {
    val collation = CollationFactory.fetchCollation(collationId)
    if (collation.supportsBinaryEquality) {
      str
    } else if (collation.supportsLowercaseEquality) {
      UTF8String.fromString(str.toString.toLowerCase(Locale.ROOT))
    } else {
      val collationKey = collation.collator.getCollationKey(str.toString)
      UTF8String.fromBytes(collationKey.toByteArray)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case _: StringType =>
      val collation = CollationFactory.fetchCollation(collationId)
      if (collation.supportsBinaryEquality) {
        defineCodeGen(ctx, ev, c => s"$c")
      } else if (collation.supportsLowercaseEquality) {
        defineCodeGen(ctx, ev, c => s"$c.toLowerCase()")
      } else {
        defineCodeGen(ctx, ev, c => s"UTF8String.fromBytes(CollationFactory.fetchCollation" +
          s"($collationId).collator.getCollationKey($c.toString()).toByteArray())")
      }
    case ArrayType(_: StringType, _) =>
      val expr = ctx.addReferenceObj("this", this)
      val arrData = ctx.freshName("arrData")
      val arrLength = ctx.freshName("arrLength")
      val arrResult = ctx.freshName("arrResult")
      val arrIndex = ctx.freshName("arrIndex")
      nullSafeCodeGen(ctx, ev, eval => {
        s"""
           |if ($eval instanceof ArrayData) {
           |  ArrayData $arrData = (ArrayData)$eval;
           |  int $arrLength = $arrData.numElements();
           |  UTF8String[] $arrResult = new UTF8String[$arrLength];
           |  for (int $arrIndex = 0; $arrIndex < $arrLength; $arrIndex++) {
           |    $arrResult[$arrIndex] = $expr.getCollationKey($arrData.getUTF8String($arrIndex));
           |  }
           |  ${ev.value} = new GenericArrayData($arrResult);
           |} else {
           |  ${ev.value} = null;
           |}
      """.stripMargin
      })
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(expr = newChild)
  }

  override def child: Expression = expr
}
