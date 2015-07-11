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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateProjection, GenerateMutableProjection}
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
    val exprOnlyConstructor = c.getConstructors.filter { c =>
      c.getParameterTypes.toSet == Set(classOf[Expression])
    }.sortBy(_.getParameterTypes.length * -1).headOption
    exprOnlyConstructor.foreach { cons =>
      val numChildren = cons.getParameterTypes.length
      test(s"${c.getName}") {
        val expr: Expression =
          cons.newInstance(Seq.fill(numChildren)(Literal.create(null, NullType)): _*).asInstanceOf[Expression]
        val coercedExpr: Expression = {
          val dummyPlan = DummyPlan(expr)
          DummyAnalyzer.execute(dummyPlan).asInstanceOf[DummyPlan].expr
          expr
        }
        println(s"Before coercion: ${expr.children.map(_.dataType)}")
        println(s"After coercion: ${coercedExpr.children.map(_.dataType)}")
        assume(coercedExpr.checkInputDataTypes().isSuccess, coercedExpr.checkInputDataTypes().toString)
        val row: InternalRow = new GenericInternalRow(Array.fill[Any](numChildren)(null))
        val inputSchema = coercedExpr.children.map(c => AttributeReference("f", c.dataType)())
        val gened = GenerateProjection.generate(Seq(coercedExpr), inputSchema)
        // TODO: mutable projections
        //gened().apply(row)
        coercedExpr.eval(row)
      }
    }
  }
}
