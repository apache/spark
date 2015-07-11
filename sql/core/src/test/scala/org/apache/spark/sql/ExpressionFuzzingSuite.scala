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

import org.clapper.classutil.ClassFinder

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateProjection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.NullType

/**
 * This test suite implements fuzz tests for expression code generation. It uses reflection to
 * automatically discover all [[Expression]]s, then instantiates these expressions with random
 * children/inputs. If the resulting expression passes the type checker after type coercion is
 * performed then we attempt to compile the expression and compare its output to output generated
 * by the interpreted expression.
 */
class ExpressionFuzzingSuite extends SparkFunSuite {

  /**
   * All subclasses of [[Expression]].
   */
  lazy val expressionSubclasses: Seq[Class[Expression]] = {
    val classpathEntries: Seq[File] = System.getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter(_.contains("spark"))
      .map(new File(_))
      .filter(_.exists()).toSeq
    val allClasses = ClassFinder(classpathEntries).getClasses()
    assert(allClasses.nonEmpty, "Could not find Spark classes on classpath.")
    ClassFinder.concreteSubclasses(classOf[Expression].getName, allClasses)
      .map(c => Class.forName(c.name).asInstanceOf[Class[Expression]]).toSeq
  }

  def coerceTypes(expression: Expression): Expression = {
    val dummyPlan: LogicalPlan = DummyPlan(expression)
    DummyAnalyzer.execute(dummyPlan).asInstanceOf[DummyPlan].expression
  }

  for (c <- expressionSubclasses) {
    val exprOnlyConstructor = c.getConstructors.filter { c =>
      c.getParameterTypes.toSet == Set(classOf[Expression])
    }.sortBy(_.getParameterTypes.length * -1).headOption
    exprOnlyConstructor.foreach { cons =>
      val numChildren = cons.getParameterTypes.length
      test(s"${c.getName}") {
        val expr: Expression =
          cons.newInstance(Seq.fill(numChildren)(Literal.create(null, NullType)): _*).asInstanceOf[Expression]
        val coercedExpr: Expression = coerceTypes(expr)
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

private case object DummyAnalyzer extends RuleExecutor[LogicalPlan] {
  override protected val batches: Seq[Batch] = Seq(
    Batch("analysis", FixedPoint(100), HiveTypeCoercion.typeCoercionRules: _*)
  )
}

private case class DummyPlan(expression: Expression) extends LogicalPlan {
  override def expressions: Seq[Expression] = Seq(expression)
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}
