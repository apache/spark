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

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  ArraysZip,
  AttributeReference,
  BinaryExpression,
  Exists,
  Expression,
  InSubquery,
  ListQuery,
  Literal,
  NamedExpression,
  OuterReference,
  Predicate,
  ScalarSubquery,
  TimeZoneAwareExpression
}
import org.apache.spark.sql.types.BooleanType

/**
 * The [[ExpressionResolutionValidator]] performs the validation work on the expression tree for the
 * [[ResolutionValidator]]. These two components work together recursively validating the
 * logical plan. You can find more info in the [[ResolutionValidator]] scaladoc.
 */
class ExpressionResolutionValidator(resolutionValidator: ResolutionValidator) {
  private val attributeScopeStack = resolutionValidator.getAttributeScopeStack

  /**
   * The flag to indicate if the validation process traverses the outer reference subtree.
   */
  private var inOuterReferenceSubtree = false

  /**
   * Validate resolved expression tree. The principle is the same as
   * [[ResolutionValidator.validate]].
   */
  def validate(expression: Expression): Unit = {
    expression match {
      case attributeReference: AttributeReference =>
        validateAttributeReference(attributeReference)
      case alias: Alias =>
        validateAlias(alias)
      case binaryExpression: BinaryExpression =>
        validateBinaryExpression(binaryExpression)
      case literal: Literal =>
        validateLiteral(literal)
      case scalarSubquery: ScalarSubquery =>
        validateScalarSubquery(scalarSubquery)
      case inSubquery: InSubquery =>
        validateInSubquery(inSubquery)
      case listQuery: ListQuery =>
        validateListQuery(listQuery)
      case exists: Exists =>
        validateExists(exists)
      case predicate: Predicate =>
        validatePredicate(predicate)
      case arraysZip: ArraysZip =>
        validateArraysZip(arraysZip)
      case timezoneExpression: TimeZoneAwareExpression =>
        validateTimezoneExpression(timezoneExpression)
      case expression: Expression =>
        validateExpression(expression)
    }
  }

  def validateProjectList(projectList: Seq[NamedExpression]): Unit = {
    projectList.foreach {
      case attributeReference: AttributeReference =>
        validateAttributeReference(attributeReference)
      case alias: Alias =>
        validateAlias(alias)
      case outerReference: OuterReference =>
        validateOuterReference(outerReference)
    }
  }

  private def validatePredicate(predicate: Predicate) = {
    predicate.children.foreach(validate)
    assert(
      predicate.dataType == BooleanType,
      s"Output type of a predicate must be a boolean, but got: ${predicate.dataType.typeName}"
    )
    validateInputDataTypes(predicate)
  }

  private def validateAttributeReference(attributeReference: AttributeReference): Unit = {
    assert(
      attributeScopeStack.contains(attributeReference, isOuterReference = inOuterReferenceSubtree),
      s"Attribute $attributeReference is missing from attribute scope stack: $attributeScopeStack"
    )
  }

  private def validateAlias(alias: Alias): Unit = {
    validate(alias.child)
  }

  private def validateOuterReference(outerReference: OuterReference): Unit = {
    inOuterReferenceSubtree = true
    try {
      validate(outerReference.e)
    } finally {
      inOuterReferenceSubtree = false
    }
  }

  private def validateBinaryExpression(binaryExpression: BinaryExpression): Unit = {
    validate(binaryExpression.left)
    validate(binaryExpression.right)
    validateInputDataTypes(binaryExpression)

    binaryExpression match {
      case timezoneExpression: TimeZoneAwareExpression =>
        assert(timezoneExpression.timeZoneId.nonEmpty, "Timezone expression must have a timezone")
      case _ =>
    }
  }

  private def validateLiteral(literal: Literal): Unit = {}

  private def validateScalarSubquery(scalarSubquery: ScalarSubquery): Unit = {
    attributeScopeStack.withNewScope(isSubqueryRoot = true) {
      resolutionValidator.validate(scalarSubquery.plan)
    }

    for (outerAttribute <- scalarSubquery.outerAttrs) {
      validate(outerAttribute)
    }

    assert(
      scalarSubquery.plan.output.size == 1,
      s"Scalar subquery returns more than one column: ${scalarSubquery.plan.output}"
    )
  }

  private def validateInSubquery(inSubquery: InSubquery): Unit = {
    for (value <- inSubquery.values) {
      validate(value)
    }

    validate(inSubquery.query)
  }

  private def validateListQuery(listQuery: ListQuery): Unit = {
    attributeScopeStack.withNewScope(isSubqueryRoot = true) {
      resolutionValidator.validate(listQuery.plan)
    }

    for (outerAttribute <- listQuery.outerAttrs) {
      validate(outerAttribute)
    }
  }

  private def validateExists(exists: Exists): Unit = {
    attributeScopeStack.withNewScope(isSubqueryRoot = true) {
      resolutionValidator.validate(exists.plan)
    }

    for (outerAttribute <- exists.outerAttrs) {
      validate(outerAttribute)
    }
  }

  private def validateArraysZip(arraysZip: ArraysZip): Unit = {
    arraysZip.children.foreach(validate)
    arraysZip.names.foreach(validate)
    validateInputDataTypes(arraysZip)
  }

  private def validateTimezoneExpression(timezoneExpression: TimeZoneAwareExpression): Unit = {
    timezoneExpression.children.foreach(validate)
    assert(timezoneExpression.timeZoneId.nonEmpty, "Timezone expression must have a timezone")
  }

  private def validateExpression(expression: Expression): Unit = {
    expression.children.foreach(validate)
    validateInputDataTypes(expression)
  }

  private def validateInputDataTypes(expression: Expression): Unit = {
    assert(
      expression.checkInputDataTypes().isSuccess,
      s"Input types of ${expression.getClass.getName} must be valid, but got: " +
      expression.children.map(_.dataType.typeName).mkString(", ")
    )
  }
}
