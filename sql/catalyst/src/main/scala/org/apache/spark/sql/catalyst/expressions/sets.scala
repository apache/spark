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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashSet

/** The data type for expressions returning an OpenHashSet as the result. */
private[sql] class OpenHashSetUDT(
    val elementType: DataType) extends UserDefinedType[OpenHashSet[Any]] {

  override def sqlType: DataType = ArrayType(elementType)

  /** Since we are using OpenHashSet internally, usually it will not be called. */
  override def serialize(obj: Any): Seq[Any] = {
    obj.asInstanceOf[OpenHashSet[Any]].iterator.toSeq
  }

  /** Since we are using OpenHashSet internally, usually it will not be called. */
  override def deserialize(datum: Any): OpenHashSet[Any] = {
    val iterator = datum.asInstanceOf[Seq[Any]].iterator
    val set = new OpenHashSet[Any]
    while(iterator.hasNext) {
      set.add(iterator.next())
    }

    set
  }

  override def userClass: Class[OpenHashSet[Any]] = classOf[OpenHashSet[Any]]

  private[spark] override def asNullable: OpenHashSetUDT = this
}

/**
 * Creates a new set of the specified type
 */
case class NewSet(elementType: DataType) extends LeafExpression with CodegenFallback {

  override def nullable: Boolean = false

  override def dataType: OpenHashSetUDT = new OpenHashSetUDT(elementType)

  override def eval(input: InternalRow): Any = {
    new OpenHashSet[Any]()
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    elementType match {
      case IntegerType | LongType =>
        ev.isNull = "false"
        s"""
          ${ctx.javaType(dataType)} ${ev.primitive} = new ${ctx.javaType(dataType)}();
        """
      case _ => super.genCode(ctx, ev)
    }
  }

  override def toString: String = s"new Set($dataType)"
}

/**
 * Adds an item to a set.
 * For performance, this expression mutates its input during evaluation.
 * Note: this expression is internal and created only by the GeneratedAggregate,
 * we don't need to do type check for it.
 */
case class AddItemToSet(item: Expression, set: Expression)
  extends Expression with CodegenFallback {

  override def children: Seq[Expression] = item :: set :: Nil

  override def nullable: Boolean = set.nullable

  override def dataType: DataType = set.dataType

  override def eval(input: InternalRow): Any = {
    val itemEval = item.eval(input)
    val setEval = set.eval(input).asInstanceOf[OpenHashSet[Any]]

    if (itemEval != null) {
      if (setEval != null) {
        setEval.add(itemEval)
        setEval
      } else {
        null
      }
    } else {
      setEval
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val elementType = set.dataType.asInstanceOf[OpenHashSetUDT].elementType
    elementType match {
      case IntegerType | LongType =>
        val itemEval = item.gen(ctx)
        val setEval = set.gen(ctx)
        val htype = ctx.javaType(dataType)

        ev.isNull = "false"
        ev.primitive = setEval.primitive
        itemEval.code + setEval.code +  s"""
          if (!${itemEval.isNull} && !${setEval.isNull}) {
           (($htype)${setEval.primitive}).add(${itemEval.primitive});
          }
         """
      case _ => super.genCode(ctx, ev)
    }
  }

  override def toString: String = s"$set += $item"
}

/**
 * Combines the elements of two sets.
 * For performance, this expression mutates its left input set during evaluation.
 * Note: this expression is internal and created only by the GeneratedAggregate,
 * we don't need to do type check for it.
 */
case class CombineSets(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def nullable: Boolean = left.nullable
  override def dataType: DataType = left.dataType

  override def eval(input: InternalRow): Any = {
    val leftEval = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if(leftEval != null) {
      val rightEval = right.eval(input).asInstanceOf[OpenHashSet[Any]]
      if (rightEval != null) {
        val iterator = rightEval.iterator
        while(iterator.hasNext) {
          val rightValue = iterator.next()
          leftEval.add(rightValue)
        }
      }
      leftEval
    } else {
      null
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val elementType = left.dataType.asInstanceOf[OpenHashSetUDT].elementType
    elementType match {
      case IntegerType | LongType =>
        val leftEval = left.gen(ctx)
        val rightEval = right.gen(ctx)
        val htype = ctx.javaType(dataType)

        ev.isNull = leftEval.isNull
        ev.primitive = leftEval.primitive
        leftEval.code + rightEval.code + s"""
          if (!${leftEval.isNull} && !${rightEval.isNull}) {
            ${leftEval.primitive}.union((${htype})${rightEval.primitive});
          }
        """
      case _ => super.genCode(ctx, ev)
    }
  }
}

/**
 * Returns the number of elements in the input set.
 * Note: this expression is internal and created only by the GeneratedAggregate,
 * we don't need to do type check for it.
 */
case class CountSet(child: Expression) extends UnaryExpression with CodegenFallback {

  override def dataType: DataType = LongType

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[OpenHashSet[Any]].size.toLong

  override def toString: String = s"$child.count()"
}
