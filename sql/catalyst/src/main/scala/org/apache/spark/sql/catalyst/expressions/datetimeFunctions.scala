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

import org.apache.spark.sql.types._

case class DateAdd(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def children: Seq[Expression] = left :: right :: Nil
  override def expectedChildTypes: Seq[DataType] = DateType :: IntegerType :: Nil

  override def dataType: DataType = DateType

  override def toString: String = s"DateAdd($left, $right)"

  override def eval(input: Row): Any = {
    val startDate = left.eval(input)
    val days = right.eval(input)
    if (startDate == null || days == null) {
      null
    } else {
      startDate.asInstanceOf[Int] + days.asInstanceOf[Int]
    }
  }
}

case class DateSub(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = DateType :: IntegerType :: Nil

  override def dataType: DataType = DateType

  override def toString: String = s"DateSub($left, $right)"

  override def eval(input: Row): Any = {
    val startDate = left.eval(input)
    val days = right.eval(input)
    if (startDate == null || days == null) {
      null
    } else {
      startDate.asInstanceOf[Int] - days.asInstanceOf[Int]
    }
  }
}

case class DateDiff(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = DateType :: DateType :: Nil

  override def dataType: DataType = IntegerType

  override def toString: String = s"DateDiff($left, $right)"

  override def eval(input: Row): Any = {
    val startDate = left.eval(input)
    val endDate = right.eval(input)
    if (startDate == null || endDate == null) {
      null
    } else {
      startDate.asInstanceOf[Int] - endDate.asInstanceOf[Int]
    }
  }
}
