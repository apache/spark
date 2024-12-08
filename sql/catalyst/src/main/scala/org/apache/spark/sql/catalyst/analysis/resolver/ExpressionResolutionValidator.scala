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
  AttributeReference,
  BinaryExpression,
  CreateNamedStruct,
  Expression,
  ExtractANSIIntervalDays,
  GetStructField,
  Literal,
  NamedExpression,
  Predicate,
  RuntimeReplaceable,
  StringRPad,
  TimeZoneAwareExpression,
  UnaryMinus
}
import org.apache.spark.sql.types.BooleanType

/**
 * The [[ExpressionResolutionValidator]] performs the validation work on the expression tree for the
 * [[ResolutionValidator]]. These two components work together recursively validating the
 * logical plan. You can find more info in the [[ResolutionValidator]] scaladoc.
 */
class ExpressionResolutionValidator(resolutionValidator: ResolutionValidator) {

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
      case extractANSIIntervalDay: ExtractANSIIntervalDays =>
        validateExtractANSIIntervalDays(extractANSIIntervalDay)
      case literal: Literal =>
        validateLiteral(literal)
      case predicate: Predicate =>
        validatePredicate(predicate)
      case stringRPad: StringRPad =>
        validateStringRPad(stringRPad)
      case unaryMinus: UnaryMinus =>
        validateUnaryMinus(unaryMinus)
      case getStructField: GetStructField =>
        validateGetStructField(getStructField)
      case createNamedStruct: CreateNamedStruct =>
        validateCreateNamedStruct(createNamedStruct)
      case runtimeReplaceable: RuntimeReplaceable =>
        validateRuntimeReplaceable(runtimeReplaceable)
      case timezoneExpression: TimeZoneAwareExpression =>
        validateTimezoneExpression(timezoneExpression)
    }
  }

  def validateProjectList(projectList: Seq[NamedExpression]): Unit = {
    projectList.foreach(expression => {
      expression match {
        case attributeReference: AttributeReference =>
          validateAttributeReference(attributeReference)
        case alias: Alias =>
          validateAlias(alias)
      }
    })
  }

  private def validatePredicate(predicate: Predicate) = {
    predicate.children.foreach(validate)
    assert(
      predicate.dataType == BooleanType,
      s"Output type of a predicate must be a boolean, but got: ${predicate.dataType.typeName}"
    )
    assert(
      predicate.checkInputDataTypes().isSuccess,
      s"Input types of a predicate must be valid, but got: " +
      s"${predicate.children.map(_.dataType.typeName).mkString(", ")}"
    )
  }

  private def validateStringRPad(stringRPad: StringRPad) = {
    validate(stringRPad.first)
    validate(stringRPad.second)
    validate(stringRPad.third)
    assert(
      stringRPad.checkInputDataTypes().isSuccess,
      s"Input types of rpad must be valid, but got: " +
      s"${stringRPad.children.map(_.dataType.typeName).mkString(", ")}"
    )
  }

  private def validateAttributeReference(attributeReference: AttributeReference): Unit = {
    assert(
      resolutionValidator.attributeScopeStack.top.contains(attributeReference),
      s"Attribute $attributeReference is missing from attribute scope: " +
      s"${resolutionValidator.attributeScopeStack.top}"
    )
  }

  private def validateAlias(alias: Alias): Unit = {
    validate(alias.child)
  }

  private def validateBinaryExpression(binaryExpression: BinaryExpression): Unit = {
    validate(binaryExpression.left)
    validate(binaryExpression.right)
    assert(binaryExpression.checkInputDataTypes().isSuccess)

    binaryExpression match {
      case timezoneExpression: TimeZoneAwareExpression =>
        assert(timezoneExpression.timeZoneId.nonEmpty)
      case _ =>
    }
  }

  private def validateExtractANSIIntervalDays(
      extractANSIIntervalDays: ExtractANSIIntervalDays): Unit = {
    validate(extractANSIIntervalDays.child)
  }

  private def validateLiteral(literal: Literal): Unit = {}

  private def validateUnaryMinus(unaryMinus: UnaryMinus): Unit = {
    validate(unaryMinus.child)
    assert(unaryMinus.checkInputDataTypes().isSuccess)
  }

  private def validateRuntimeReplaceable(runtimeReplaceable: RuntimeReplaceable): Unit = {
    runtimeReplaceable.children.foreach(validate)
  }

  private def validateGetStructField(getStructField: GetStructField): Unit = {
    validate(getStructField.child)
  }

  private def validateCreateNamedStruct(createNamedStruct: CreateNamedStruct): Unit = {
    createNamedStruct.children.foreach(validate)
    assert(
      createNamedStruct.checkInputDataTypes().isSuccess,
      s"Input types of CreateNamedStruct must be valid, but got: " +
      s"${createNamedStruct.children.map(_.dataType.typeName).mkString(", ")}"
    )
  }

  private def validateTimezoneExpression(timezoneExpression: TimeZoneAwareExpression): Unit = {
    timezoneExpression.children.foreach(validate)
    assert(timezoneExpression.timeZoneId.nonEmpty, "Timezone expression must have a timezone")
  }
}
