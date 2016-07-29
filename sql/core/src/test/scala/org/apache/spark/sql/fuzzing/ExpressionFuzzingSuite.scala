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

package org.apache.spark.sql.fuzzing

import java.io.File
import java.lang.reflect.Constructor

import scala.util.{Random, Try}

import org.clapper.classutil.ClassFinder

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BinaryType, DataType, DataTypeTestUtils, DecimalType}
import org.apache.spark.util.Utils

/**
 * This test suite implements fuzz tests for expression code generation. It uses reflection to
 * automatically discover all [[Expression]]s, then instantiates these expressions with random
 * children/inputs. If the resulting expression passes the type checker after type coercion is
 * performed then we attempt to compile the expression and compare its output to output generated
 * by the interpreted expression.
 */
class ExpressionFuzzingSuite extends SparkFunSuite with Logging {

  val NUM_TRIALS_PER_EXPRESSION: Int = 100

  /**
   * All evaluable subclasses of [[Expression]].
   */
  lazy val evaluableExpressionClasses: Seq[Class[Expression]] = {
    val classpathEntries: Seq[File] = System.getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter(_.contains("spark"))
      .map(new File(_))
      .filter(_.exists()).toSeq
    val allClasses = ClassFinder(classpathEntries).getClasses().toIterator
    assert(allClasses.nonEmpty, "Could not find Spark classes on classpath.")
    ClassFinder.concreteSubclasses(classOf[Expression].getName, allClasses)
      .map(c => Utils.classForName(c.name).asInstanceOf[Class[Expression]]).toSeq
      // We should only test evalulable expressions:
      .filterNot(c => classOf[Unevaluable].isAssignableFrom(c))
      // These expressions currently OOM because we try to pass in massive numeric literals:
      .filterNot(_ == classOf[FormatNumber])
      .filterNot(_ == classOf[StringSpace])
      .filterNot(_ == classOf[StringLPad])
      .filterNot(_ == classOf[StringRPad])
      .filterNot(_ == classOf[BRound])
      .filterNot(_ == classOf[Round])
  }

  def coerceTypes(expression: Expression): Expression = {
    val dummyPlan: LogicalPlan = DummyPlan(expression)
    DummyAnalyzer.execute(dummyPlan).asInstanceOf[DummyPlan].expression
  }

  /**
   * Given an expression class, find the constructor which accepts only expressions. If there are
   * multiple such constructors, pick the one with the most parameters.
   *
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

  def getRandomLiteral: Literal = {
    val allTypes = DataTypeTestUtils.atomicTypes
      .filterNot(_.isInstanceOf[DecimalType]) // casts can lead to OOM
      .filterNot(_.isInstanceOf[BinaryType]) // leads to spurious errors in string reverse
    val dataTypesWithGenerators: Map[DataType, () => Any] = allTypes.map { dt =>
      (dt, RandomDataGenerator.forType(dt, nullable = true))
    }.filter(_._2.isDefined).toMap.mapValues(_.get)
    val (dt, generator) =
      dataTypesWithGenerators.toSeq(Random.nextInt(dataTypesWithGenerators.size))
    Literal.create(generator(), dt)
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
    // Construct random literals for all child expressions and leave it up to the type coercion
    // rules to cast them to the appropriate types. Skip
    for (_ <- 1 to NUM_TRIALS_PER_EXPRESSION) {
      val expression: Expression = {
        val childExpressions: Seq[Expression] = Seq.fill(numChildren)(getRandomLiteral)
        coerceTypes(constructor.newInstance(childExpressions: _*))
      }
      logInfo(s"After type coercion, expression is $expression")
      // Make sure that the resulting expression passes type checks.
      require(expression.childrenResolved)
      val typecheckResult = expression.checkInputDataTypes()
      if (typecheckResult.isFailure) {
        logDebug(s"Type checks failed: $typecheckResult")
      } else {
        withClue(s"$expression") {
          val inputRow = InternalRow.apply() // Can be empty since we're only using literals
          val inputSchema = expression.children.map(c => AttributeReference("f", c.dataType)())

          val interpretedProjection = new InterpretedProjection(Seq(expression), inputSchema)
          val interpretedResult = interpretedProjection.apply(inputRow)

          val maybeGenProjection =
            Try(GenerateSafeProjection.generate(Seq(expression), inputSchema))
          if (maybeGenProjection.isFailure) {
            //scalastyle:off
            println(
              s"Code generation for expression $expression failed with inputSchema $inputSchema")
          }
          maybeGenProjection.foreach { generatedProjection =>
            val generatedResult = generatedProjection.apply(inputRow)
            assert(generatedResult === interpretedResult)
          }
        }
      }
    }
  }

  // Run the actual tests
  evaluableExpressionClasses.sortBy(_.getName).foreach { expressionClass =>
    test(s"${expressionClass.getName}") {
      testExpression(expressionClass)
    }
  }
}

private case object DummyAnalyzer extends RuleExecutor[LogicalPlan] {
  override protected val batches: Seq[Batch] = Seq(
    Batch("analysis", FixedPoint(100), TypeCoercion.typeCoercionRules: _*)
  )
}

private case class DummyPlan(expression: Expression) extends LogicalPlan {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}
