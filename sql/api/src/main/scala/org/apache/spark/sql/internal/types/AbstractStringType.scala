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

package org.apache.spark.sql.internal.types

import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}

/**
 * AbstractStringType is an abstract class for StringType with collation support.
 */
abstract class AbstractStringType(supportsTrimCollation: Boolean = false)
    extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType = SqlApiConf.get.defaultStringType
  override private[sql] def simpleString: String = "string"

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other match {
      case st: StringType =>
        canUseTrimCollation(st) && acceptsStringType(st)
      case _ =>
        false
    }
  }
  def acceptsStringType(other: StringType): Boolean

  private[sql] def canUseTrimCollation(other: StringType): Boolean =
    supportsTrimCollation || !other.usesTrimCollation
}

/**
 * Used for expressions supporting only binary collation.
 */
case class StringTypeBinary(supportsTrimCollation: Boolean = false)
    extends AbstractStringType(supportsTrimCollation) {

  override def acceptsStringType(other: StringType): Boolean =
    other.supportsBinaryEquality
}

object StringTypeBinary extends StringTypeBinary(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeBinary = {
    new StringTypeBinary(supportsTrimCollation)
  }
}

/**
 * Used for expressions supporting only binary and lowercase collation.
 */
case class StringTypeBinaryLcase(supportsTrimCollation: Boolean = false)
    extends AbstractStringType(supportsTrimCollation) {

  override def acceptsStringType(other: StringType): Boolean =
    other.supportsBinaryEquality || other.isUTF8LcaseCollation
}

object StringTypeBinaryLcase extends StringTypeBinaryLcase(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeBinaryLcase = {
    new StringTypeBinaryLcase(supportsTrimCollation)
  }
}

/**
 * Used for expressions supporting collation types with optional
 * case, accent, and trim sensitivity specifiers.
 *
 * Case and accent sensitivity specifiers are supported by default.
 */
case class StringTypeWithCollation(
    supportsTrimCollation: Boolean = false,
    supportsCaseSpecifier: Boolean = true,
    supportsAccentSpecifier: Boolean = true)
    extends AbstractStringType(supportsTrimCollation) {

  override def acceptsStringType(other: StringType): Boolean = {
    (supportsCaseSpecifier || !other.isCaseInsensitive) &&
      (supportsAccentSpecifier || !other.isAccentInsensitive)
  }
}

object StringTypeWithCollation extends StringTypeWithCollation(false, true, true) {
  def apply(
      supportsTrimCollation: Boolean = false,
      supportsCaseSpecifier: Boolean = true,
      supportsAccentSpecifier: Boolean = true): StringTypeWithCollation = {
    new StringTypeWithCollation(
      supportsTrimCollation, supportsCaseSpecifier, supportsAccentSpecifier)
  }
}

/**
 * Used for expressions supporting all possible collation types except
 * those that are case-sensitive but accent insensitive (CS_AI).
 */
case class StringTypeNonCSAICollation(supportsTrimCollation: Boolean = false)
    extends AbstractStringType(supportsTrimCollation) {

  override def acceptsStringType(other: StringType): Boolean =
    other.isCaseInsensitive || !other.isAccentInsensitive
}

object StringTypeNonCSAICollation extends StringTypeNonCSAICollation(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeNonCSAICollation = {
    new StringTypeNonCSAICollation(supportsTrimCollation)
  }
}
