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

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.collection.OpenHashSet

/**
 * Creates a new set of the specified type
 */
case class NewSet(elementType: DataType) extends LeafExpression {
  type EvaluatedType = Any

  def nullable = false

  // We are currently only using these Expressions internally for aggregation.  However, if we ever
  // expose these to users we'll want to create a proper type instead of hijacking ArrayType.
  def dataType = ArrayType(elementType)

  def eval(input: Row): Any = {
    new OpenHashSet[Any]()
  }

  override def toString = s"new Set($dataType)"
}

/**
 * Adds an item to a set.
 * For performance, this expression mutates its input during evaluation.
 */
case class AddItemToSet(item: Expression, set: Expression) extends Expression {
  type EvaluatedType = Any

  def children = item :: set :: Nil

  def nullable = set.nullable

  def dataType = set.dataType
  def eval(input: Row): Any = {
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

  override def toString = s"$set += $item"
}

/**
 * Combines the elements of two sets.
 * For performance, this expression mutates its left input set during evaluation.
 */
case class CombineSets(left: Expression, right: Expression) extends BinaryExpression {
  type EvaluatedType = Any

  def nullable = left.nullable || right.nullable

  def dataType = left.dataType

  def symbol = "++="

  def eval(input: Row): Any = {
    val leftEval = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if(leftEval != null) {
      val rightEval = right.eval(input).asInstanceOf[OpenHashSet[Any]]
      if (rightEval != null) {
        val iterator = rightEval.iterator
        while(iterator.hasNext) {
          val rightValue = iterator.next()
          leftEval.add(rightValue)
        }
        leftEval
      } else {
        null
      }
    } else {
      null
    }
  }
}

/**
 * Returns the number of elements in the input set.
 */
case class CountSet(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  def nullable = child.nullable

  def dataType = LongType

  def eval(input: Row): Any = {
    val childEval = child.eval(input).asInstanceOf[OpenHashSet[Any]]
    if (childEval != null) {
      childEval.size.toLong
    }
  }

  override def toString = s"$child.count()"
}
