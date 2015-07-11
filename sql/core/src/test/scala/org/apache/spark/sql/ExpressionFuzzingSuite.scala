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

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.NullType
import org.clapper.classutil.ClassFinder

case object DummyAnalyzer extends RuleExecutor[LogicalPlan] {
  override protected val batches: Seq[Batch] = Seq(
    Batch("analysis", FixedPoint(100), HiveTypeCoercion.typeCoercionRules: _*)
  )
}


case class DummyPlan(expr: Expression) extends LogicalPlan {
  override def output: Seq[Attribute] = Seq.empty

  /** Returns all of the expressions present in this query plan operator. */
  override def expressions: Seq[Expression] = Seq(expr)

  override def children: Seq[LogicalPlan] = Seq.empty
}


class ExpressionFuzzingSuite extends SparkFunSuite {
  lazy val expressionClasses: Seq[Class[_]] = {
    val finder = ClassFinder(
      System.getProperty("java.class.path").split(':').map(new File(_)) .filter(_.exists))
    val classes = finder.getClasses().toIterator
    ClassFinder.concreteSubclasses(classOf[Expression].getName, classes)
      .map(c => Class.forName(c.name)).toSeq
  }

  expressionClasses.foreach(println)

  for (c <- expressionClasses) {
    val singleExprConstructor = c.getConstructors.filter { c =>
      c.getParameterTypes.toSeq == Seq(classOf[Expression])
    }
    singleExprConstructor.foreach { cons =>
      test(s"${c.getName}") {
        val expr: Expression = cons.newInstance(Literal(null)).asInstanceOf[Expression]
        val coercedExpr: Expression = {
          val dummyPlan = DummyPlan(expr)
          DummyAnalyzer.execute(dummyPlan).asInstanceOf[DummyPlan].expr
        }
        if (expr.checkInputDataTypes().isSuccess) {
          val row: InternalRow = new GenericInternalRow(Array[Any](null))
          val gened = GenerateMutableProjection.generate(Seq(coercedExpr), Seq(AttributeReference("f", NullType)()))
          gened()
          //        expr.eval(row)
        } else {
          println(s"Input types check failed for $c")
        }
      }
    }
  }
}
