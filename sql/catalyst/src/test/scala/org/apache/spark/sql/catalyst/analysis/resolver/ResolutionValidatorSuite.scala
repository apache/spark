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

package org.apache.spark.sql.catalyst.analysis.resolver

import scala.collection.immutable
import scala.reflect.runtime.universe.typeOf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  Alias,
  AttributeReference,
  Cast,
  GreaterThan,
  Literal,
  NamedExpression,
  TimeAdd
}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.{
  BooleanType,
  DayTimeIntervalType,
  DecimalType,
  IntegerType,
  StringType,
  TimestampType
}

class ResolutionValidatorSuite extends SparkFunSuite with SQLConfHelper {
  private val resolveMethodNamesToIgnore = Seq(
    // [[Resolver]] accepts [[UnresolvedInlineTable]], [[ResolvedInlineTable]] and
    // [[LocalRelation]], but produces only [[ResolvedInlineTable]] and [[LocalRelation]], so
    // we omit one of them here.
    // See [[Resolver.resolveInlineTable]] scaladoc for more info.
    "resolveResolvedInlineTable"
  )

  private val colInteger = AttributeReference(name = "colInteger", dataType = IntegerType)()
  private val colBoolean = AttributeReference(name = "colBoolean", dataType = BooleanType)()
  private val colTimestamp = AttributeReference(name = "colTimestamp", dataType = TimestampType)()

  test("All resolve* methods must have validate* counterparts") {
    val actualMethodNames = typeOf[ResolutionValidator].decls
      .collect {
        case decl if decl.isMethod => decl.name.toString
      }
      .filter(name => {
        name.startsWith("validate")
      })
    val actualMethodNamesSet = immutable.HashSet(actualMethodNames.toSeq: _*)

    val resolveMethodNamesToIgnoreSet = immutable.HashSet(resolveMethodNamesToIgnore: _*)

    typeOf[Resolver].decls
      .collect {
        case decl if decl.isMethod => decl.name.toString
      }
      .filter(name => {
        name.startsWith("resolve") && !resolveMethodNamesToIgnoreSet.contains(name)
      })
      .map(name => {
        "validate" + name.stripPrefix("resolve")
      })
      .foreach(name => {
        assert(actualMethodNamesSet.contains(name), name)
      })
  }

  test("Project") {
    validate(
      Project(
        projectList = Seq(colInteger, colBoolean, colInteger),
        child = LocalRelation(output = Seq(colInteger, colBoolean))
      )
    )
    validate(
      Project(
        projectList = Seq(colInteger),
        child = LocalRelation(output = colBoolean)
      ),
      error = Some("Project list contains nonexisting attribute")
    )
  }

  test("Filter") {
    validate(
      Project(
        projectList = Seq(colBoolean),
        child = Filter(
          condition = colBoolean,
          child = LocalRelation(output = colBoolean)
        )
      )
    )
    validate(
      Project(
        projectList = Seq(colInteger),
        child = Filter(
          condition = colInteger,
          child = LocalRelation(output = colInteger)
        )
      ),
      error = Some("Non-boolean condition")
    )
    validate(
      Project(
        projectList = Seq(colBoolean),
        child = Filter(
          condition = AttributeReference(name = "colBooleanOther", dataType = BooleanType)(),
          child = LocalRelation(output = colBoolean)
        )
      ),
      error = Some("Condition references nonexisting attribute")
    )
  }

  test("Predicate") {
    validate(
      Project(
        projectList = Seq(colInteger),
        child = Filter(
          condition = GreaterThan(colInteger, colInteger),
          child = LocalRelation(output = colInteger)
        )
      )
    )
    validate(
      Project(
        projectList = Seq(colInteger),
        child = Filter(
          condition = GreaterThan(colInteger, colBoolean),
          child = LocalRelation(output = Seq(colInteger, colBoolean))
        )
      ),
      error = Some("Input data types mismatch")
    )
  }

  test("BinaryExpression") {
    validate(
      Project(
        projectList = Seq(
          Alias(
            child = Add(
              left = Literal(5),
              right = Literal(1)
            ),
            "Add"
          )(NamedExpression.newExprId)
        ),
        child = LocalRelation(output = colInteger)
      )
    )
    validate(
      Project(
        projectList = Seq(
          Alias(
            child = Add(
              left = Literal(5),
              right = Literal("1")
            ),
            "AddWrongInputTypes"
          )(NamedExpression.newExprId)
        ),
        child = LocalRelation(output = colInteger)
      ),
      error = Some("checkInputDataTypes mismatch")
    )
    validate(
      Project(
        projectList = Seq(
          Alias(
            child = TimeAdd(
              start = Cast(
                child = Literal("2024-10-01"),
                dataType = TimestampType,
                timeZoneId = Option(conf.sessionLocalTimeZone)
              ),
              interval = Cast(
                child = Literal(1),
                dataType = DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY),
                timeZoneId = Option(conf.sessionLocalTimeZone)
              )
            ),
            "AddNoTimezone"
          )(NamedExpression.newExprId)
        ),
        child = LocalRelation(output = colInteger)
      ),
      error = Some("TimezoneId is not set for TimeAdd")
    )
  }

  test("TimeZoneAwareExpression") {
    validate(
      Project(
        projectList = Seq(
          Alias(
            Cast(
              child = colInteger,
              dataType = DecimalType.USER_DEFAULT,
              timeZoneId = Option(conf.sessionLocalTimeZone)
            ),
            "withTimezone"
          )(NamedExpression.newExprId)
        ),
        child = LocalRelation(output = colInteger)
      )
    )
    validate(
      Project(
        projectList = Seq(
          Alias(
            Cast(
              child = colTimestamp,
              dataType = StringType
            ),
            "withoutTimezone"
          )(NamedExpression.newExprId)
        ),
        child = LocalRelation(output = colTimestamp)
      ),
      error = Some("TimezoneId is not set")
    )
  }

  def validate(plan: LogicalPlan, error: Option[String] = None): Unit = {
    def errorWrapper(error: String)(body: => Unit): Unit = {
      withClue(error) {
        intercept[Throwable] {
          body
        }
      }
    }

    def noopWrapper(body: => Unit) = {
      body
    }

    val wrapper = error
      .map(error => { errorWrapper(error) _ })
      .getOrElse { noopWrapper _ }

    val validator = new ResolutionValidator
    wrapper {
      validator.validatePlan(plan)
    }
  }
}
