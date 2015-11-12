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

package test.org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, GenericInternalRow, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, Strategy, QueryTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.unsafe.types.UTF8String

case class FastOperator(output: Seq[Attribute]) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val str = Literal("so fast").value
    val row = new GenericInternalRow(Array[Any](str))
    sparkContext.parallelize(Seq(row))
  }

  override def children: Seq[SparkPlan] = Nil
}

object TestStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(Seq(attr), _) if attr.name == "a" =>
      FastOperator(attr.toAttribute :: Nil) :: Nil
    case _ => Nil
  }
}

class ExtraStrategiesSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("insert an extraStrategy") {
    try {
      sqlContext.experimental.extraStrategies = TestStrategy :: Nil

      val df = sparkContext.parallelize(Seq(("so slow", 1))).toDF("a", "b")
      checkAnswer(
        df.select("a"),
        Row("so fast"))

      checkAnswer(
        df.select("a", "b"),
        Row("so slow", 1))
    } finally {
      sqlContext.experimental.extraStrategies = Nil
    }
  }
}
