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
import java.lang.reflect.Constructor

import org.clapper.classutil.ClassFinder

import org.apache.spark.{Logging, SparkFunSuite}
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
class ExpressionFuzzingSuite extends SparkFunSuite with Logging {

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

  /**
   * Given an expression class, find the constructor which accepts only expressions. If there are
   * multiple such constructors, pick the one with the most parameters.
   * @return The matching constructor, or None if no appropriate constructor could be found.
   */
  def getBestConstructor(expressionClass: Class[Expression]): Option[Constructor[Expression]] = {
    val allConstructors = expressionClass.getConstructors ++ expressionClass.getDeclaredConstructors
    allConstructors
      .map(_.asInstanceOf[Constructor[Expression]])
      .filter(_.getParameterTypes.toSet == Set(classOf[Expression]))
      .sortBy(_.getParameterTypes.length * -1)
      .headOption
  }

  def testExpression(expressionClass: Class[Expression]): Unit = {
    // Eventually, we should add support for testing multiple constructors. For now, though, we
    // only test the "best" one:
    val constructor: Constructor[Expression] = {
      val maybeBestConstructor = getBestConstructor(expressionClass)
      assume(maybeBestConstructor.isDefined, "Could not find an Expression-only constructor")
      maybeBestConstructor.get
    }
    val numChildren: Int = constructor.getParameterTypes.length
    // Eventually, we should test with multiple types of child expressions. For now, though, we
    // construct null literals for all child expressions and leave it up to the type coercion rules
    // to cast them to the appropriate types.
    val expression: Expression = {
      val childExpressions: Seq[Expression] = Seq.fill(numChildren)(Literal.create(null, NullType))
      coerceTypes(constructor.newInstance(childExpressions: _*))
    }
    logInfo(s"After type coercion, expression is $expression")
    // Make sure that the resulting expression passes type checks.
    val typecheckResult = expression.checkInputDataTypes()
    assume(typecheckResult.isSuccess, s"Type checks failed: $typecheckResult")
    // Attempt to generate code for this expression by using it to generate a projection.
    val inputSchema = expression.children.map(c => AttributeReference("f", c.dataType)())
    val generatedProjection = GenerateProjection.generate(Seq(expression), inputSchema)
    val interpretedProjection = new InterpretedProjection(Seq(expression), inputSchema)
    // Check that the answers agree for an input row consisting entirely of nulls, since the
    // implicit type casts should make this safe
    val inputRow = InternalRow.apply(Seq.fill(numChildren)(null))
    val generatedResult = generatedProjection.apply(inputRow)
    val interpretedResult = interpretedProjection.apply(inputRow)
    assert(generatedResult === interpretedResult)
  }

  // Run the actual tests
  expressionSubclasses.foreach { expressionClass =>
    test(s"${expressionClass.getName}") {
      testExpression(expressionClass)
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
