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
  ArrayDistinct,
  ArrayInsert,
  ArrayJoin,
  ArrayMax,
  ArrayMin,
  ArraysZip,
  AttributeReference,
  BinaryExpression,
  ConditionalExpression,
  CreateArray,
  CreateMap,
  CreateNamedStruct,
  Expression,
  ExtractANSIIntervalDays,
  GetArrayStructFields,
  GetMapValue,
  GetStructField,
  Literal,
  MapConcat,
  MapContainsKey,
  MapEntries,
  MapFromEntries,
  MapKeys,
  MapValues,
  NamedExpression,
  Predicate,
  RuntimeReplaceable,
  StringRPad,
  StringToMap,
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
      case getMapValue: GetMapValue =>
        validateGetMapValue(getMapValue)
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
      case getArrayStructFields: GetArrayStructFields =>
        validateGetArrayStructFields(getArrayStructFields)
      case createMap: CreateMap =>
        validateCreateMap(createMap)
      case stringToMap: StringToMap =>
        validateStringToMap(stringToMap)
      case mapContainsKey: MapContainsKey =>
        validateMapContainsKey(mapContainsKey)
      case mapConcat: MapConcat =>
        validateMapConcat(mapConcat)
      case mapKeys: MapKeys =>
        validateMapKeys(mapKeys)
      case mapValues: MapValues =>
        validateMapValues(mapValues)
      case mapEntries: MapEntries =>
        validateMapEntries(mapEntries)
      case mapFromEntries: MapFromEntries =>
        validateMapFromEntries(mapFromEntries)
      case createArray: CreateArray =>
        validateCreateArray(createArray)
      case arrayDistinct: ArrayDistinct =>
        validateArrayDistinct(arrayDistinct)
      case arrayInsert: ArrayInsert =>
        validateArrayInsert(arrayInsert)
      case arrayJoin: ArrayJoin =>
        validateArrayJoin(arrayJoin)
      case arrayMax: ArrayMax =>
        validateArrayMax(arrayMax)
      case arrayMin: ArrayMin =>
        validateArrayMin(arrayMin)
      case arraysZip: ArraysZip =>
        validateArraysZip(arraysZip)
      case conditionalExpression: ConditionalExpression =>
        validateConditionalExpression(conditionalExpression)
      case runtimeReplaceable: RuntimeReplaceable =>
        validateRuntimeReplaceable(runtimeReplaceable)
      case timezoneExpression: TimeZoneAwareExpression =>
        validateTimezoneExpression(timezoneExpression)
    }
  }

  def validateProjectList(projectList: Seq[NamedExpression]): Unit = {
    projectList.foreach {
      case attributeReference: AttributeReference =>
        validateAttributeReference(attributeReference)
      case alias: Alias =>
        validateAlias(alias)
    }
  }

  private def validatePredicate(predicate: Predicate) = {
    predicate.children.foreach(validate)
    assert(
      predicate.dataType == BooleanType,
      s"Output type of a predicate must be a boolean, but got: ${predicate.dataType.typeName}"
    )
    assert(
      predicate.checkInputDataTypes().isSuccess,
      "Input types of a predicate must be valid, but got: " +
      predicate.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateStringRPad(stringRPad: StringRPad) = {
    validate(stringRPad.first)
    validate(stringRPad.second)
    validate(stringRPad.third)
    assert(
      stringRPad.checkInputDataTypes().isSuccess,
      "Input types of rpad must be valid, but got: " +
      stringRPad.children.map(_.dataType.typeName).mkString(", ")
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
    assert(
      binaryExpression.checkInputDataTypes().isSuccess,
      "Input types of a binary expression must be valid, but got: " +
      binaryExpression.children.map(_.dataType.typeName).mkString(", ")
    )

    binaryExpression match {
      case timezoneExpression: TimeZoneAwareExpression =>
        assert(timezoneExpression.timeZoneId.nonEmpty, "Timezone expression must have a timezone")
      case _ =>
    }
  }

  private def validateConditionalExpression(conditionalExpression: ConditionalExpression): Unit =
    conditionalExpression.children.foreach(validate)

  private def validateExtractANSIIntervalDays(
      extractANSIIntervalDays: ExtractANSIIntervalDays): Unit = {
    validate(extractANSIIntervalDays.child)
  }

  private def validateLiteral(literal: Literal): Unit = {}

  private def validateUnaryMinus(unaryMinus: UnaryMinus): Unit = {
    validate(unaryMinus.child)
    assert(
      unaryMinus.checkInputDataTypes().isSuccess,
      "Input types of a unary minus must be valid, but got: " +
      unaryMinus.child.dataType.typeName.mkString(", ")
    )
  }

  private def validateGetStructField(getStructField: GetStructField): Unit = {
    validate(getStructField.child)
  }

  private def validateCreateNamedStruct(createNamedStruct: CreateNamedStruct): Unit = {
    createNamedStruct.children.foreach(validate)
    assert(
      createNamedStruct.checkInputDataTypes().isSuccess,
      "Input types of CreateNamedStruct must be valid, but got: " +
      createNamedStruct.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateGetArrayStructFields(getArrayStructFields: GetArrayStructFields): Unit = {
    validate(getArrayStructFields.child)
  }

  private def validateGetMapValue(getMapValue: GetMapValue): Unit = {
    validate(getMapValue.child)
    validate(getMapValue.key)
    assert(
      getMapValue.checkInputDataTypes().isSuccess,
      "Input types of GetMapValue must be valid, but got: " +
      getMapValue.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateCreateMap(createMap: CreateMap): Unit = {
    createMap.children.foreach(validate)
    assert(
      createMap.checkInputDataTypes().isSuccess,
      "Input types of CreateMap must be valid, but got: " +
      createMap.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateStringToMap(stringToMap: StringToMap): Unit = {
    validate(stringToMap.text)
    validate(stringToMap.pairDelim)
    validate(stringToMap.keyValueDelim)
  }

  private def validateMapContainsKey(mapContainsKey: MapContainsKey): Unit = {
    validate(mapContainsKey.left)
    validate(mapContainsKey.right)
    assert(
      mapContainsKey.checkInputDataTypes().isSuccess,
      "Input types of MapContainsKey must be valid, but got: " +
      mapContainsKey.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateMapConcat(mapConcat: MapConcat): Unit = {
    mapConcat.children.foreach(validate)
    assert(
      mapConcat.checkInputDataTypes().isSuccess,
      "Input types of MapConcat must be valid, but got: " +
      mapConcat.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateMapKeys(mapKeys: MapKeys): Unit = {
    validate(mapKeys.child)
  }

  private def validateMapValues(mapValues: MapValues): Unit = {
    validate(mapValues.child)
  }

  private def validateMapEntries(mapEntries: MapEntries): Unit = {
    validate(mapEntries.child)
  }

  private def validateMapFromEntries(mapFromEntries: MapFromEntries): Unit = {
    mapFromEntries.children.foreach(validate)
    assert(
      mapFromEntries.checkInputDataTypes().isSuccess,
      "Input types of MapFromEntries must be valid, but got: " +
      mapFromEntries.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateCreateArray(createArray: CreateArray): Unit = {
    createArray.children.foreach(validate)
    assert(
      createArray.checkInputDataTypes().isSuccess,
      "Input types of CreateArray must be valid, but got: " +
      createArray.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateArrayDistinct(arrayDistinct: ArrayDistinct): Unit = {
    validate(arrayDistinct.child)
    assert(
      arrayDistinct.checkInputDataTypes().isSuccess,
      "Input types of ArrayDistinct must be valid, but got: " +
      arrayDistinct.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateArrayInsert(arrayInsert: ArrayInsert): Unit = {
    validate(arrayInsert.srcArrayExpr)
    validate(arrayInsert.posExpr)
    validate(arrayInsert.itemExpr)
    assert(
      arrayInsert.checkInputDataTypes().isSuccess,
      "Input types of ArrayInsert must be valid, but got: " +
      arrayInsert.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateArrayJoin(arrayJoin: ArrayJoin): Unit = {
    validate(arrayJoin.array)
    validate(arrayJoin.delimiter)
    if (arrayJoin.nullReplacement.isDefined) {
      validate(arrayJoin.nullReplacement.get)
    }
  }

  private def validateArrayMax(arrayMax: ArrayMax): Unit = {
    validate(arrayMax.child)
    assert(
      arrayMax.checkInputDataTypes().isSuccess,
      "Input types of ArrayMax must be valid, but got: " +
      arrayMax.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateArrayMin(arrayMin: ArrayMin): Unit = {
    validate(arrayMin.child)
    assert(
      arrayMin.checkInputDataTypes().isSuccess,
      "Input types of ArrayMin must be valid, but got: " +
      arrayMin.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateArraysZip(arraysZip: ArraysZip): Unit = {
    arraysZip.children.foreach(validate)
    arraysZip.names.foreach(validate)
    assert(
      arraysZip.checkInputDataTypes().isSuccess,
      "Input types of ArraysZip must be valid, but got: " +
      arraysZip.children.map(_.dataType.typeName).mkString(", ")
    )
  }

  private def validateRuntimeReplaceable(runtimeReplaceable: RuntimeReplaceable): Unit = {
    runtimeReplaceable.children.foreach(validate)
  }

  private def validateTimezoneExpression(timezoneExpression: TimeZoneAwareExpression): Unit = {
    timezoneExpression.children.foreach(validate)
    assert(timezoneExpression.timeZoneId.nonEmpty, "Timezone expression must have a timezone")
  }
}
