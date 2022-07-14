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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession

case class FastOperator(output: Seq[Attribute]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val str = Literal("so fast").value
    val row = new GenericInternalRow(Array[Any](str))
    val unsafeProj = UnsafeProjection.create(schema)
    val unsafeRow = unsafeProj(row).copy()
    sparkContext.parallelize(Seq(unsafeRow))
  }

  override def producedAttributes: AttributeSet = outputSet
}

object TestStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(Seq(attr), _) if attr.name == "a" =>
      FastOperator(attr.toAttribute :: Nil) :: Nil
    case _ => Nil
  }
}

class ExtraStrategiesSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("insert an extraStrategy") {
    try {
      spark.experimental.extraStrategies = TestStrategy :: Nil

      val df = sparkContext.parallelize(Seq(("so slow", 1))).toDF("a", "b")
      checkAnswer(
        df.select("a"),
        Row("so fast"))

      checkAnswer(
        df.select("a", "b"),
        Row("so slow", 1))
    } finally {
      spark.experimental.extraStrategies = Nil
    }
  }
}
