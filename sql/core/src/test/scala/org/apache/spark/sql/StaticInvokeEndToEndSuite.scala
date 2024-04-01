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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, RuntimeReplaceable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType}

class StaticInvokeEndToEndSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("the method declare throw one exception") {
    val df = Seq(("1", "spark")).toDF("key", "value").toDF()
    val actualDF = df.select(Column(ParseInt1(Column("key").expr)))
    val plan = actualDF.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    checkAnswer(actualDF, Seq(Row(1)))
  }

  test("the method declare throw two exception") {
    val df = Seq(("1", "spark")).toDF("key", "value").toDF()
    val actualDF = df.select(Column(ParseInt2(Column("key").expr)))
    val plan = actualDF.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    checkAnswer(actualDF, Seq(Row(1)))
  }
}

case class ParseInt1(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[StaticInvokeMethod],
    IntegerType,
    "parseInt1",
    Seq(child),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = IntegerType

  override def prettyName: String = "parse_int1"

  override protected def withNewChildInternal(newChild: Expression): ParseInt1 =
    copy(child = newChild)
}

case class ParseInt2(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[StaticInvokeMethod],
    IntegerType,
    "parseInt2",
    Seq(child),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = IntegerType

  override def prettyName: String = "parse_int2"

  override protected def withNewChildInternal(newChild: Expression): ParseInt2 =
    copy(child = newChild)
}
