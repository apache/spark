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
abstract class AbstractStringType(private[sql] val supportsTrimCollation: Boolean = false)
    extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType = SqlApiConf.get.defaultStringType
  override private[sql] def simpleString: String = "string"
  private[sql] def canUseTrimCollation(other: DataType): Boolean =
    supportsTrimCollation || !other.asInstanceOf[StringType].usesTrimCollation
}

/**
 * Use StringTypeBinary for expressions supporting only binary collation.
 */
case class StringTypeBinary(override val supportsTrimCollation: Boolean = false)
  extends AbstractStringType(supportsTrimCollation) {
    override private[sql] def acceptsType(other: DataType): Boolean =
      other.isInstanceOf[StringType] && other.asInstanceOf[StringType].supportsBinaryEquality &&
        canUseTrimCollation(other)
 }

object StringTypeBinary extends StringTypeBinary(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeBinary = {
    new StringTypeBinary(supportsTrimCollation)
  }
}
/**
 * Use StringTypeBinaryLcase for expressions supporting only binary and lowercase collation.
 */
case class StringTypeBinaryLcase(override val supportsTrimCollation: Boolean = false)
  extends AbstractStringType(supportsTrimCollation) {
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[StringType] && (other.asInstanceOf[StringType].supportsBinaryEquality ||
      other.asInstanceOf[StringType].isUTF8LcaseCollation) && canUseTrimCollation(other)
}

object StringTypeBinaryLcase extends StringTypeBinaryLcase(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeBinaryLcase = {
    new StringTypeBinaryLcase(supportsTrimCollation)
  }
}
/**
 * Use StringTypeWithCaseAccentSensitivity for expressions supporting all collation types (binary
 * and ICU) but limited to using case and accent sensitivity specifiers.
 */
case class StringTypeWithCaseAccentSensitivity(override val supportsTrimCollation: Boolean = false)
  extends AbstractStringType(supportsTrimCollation) {
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[StringType] && canUseTrimCollation(other)
}

object StringTypeWithCaseAccentSensitivity extends StringTypeWithCaseAccentSensitivity(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeWithCaseAccentSensitivity = {
    new StringTypeWithCaseAccentSensitivity(supportsTrimCollation)
  }
}

/**
 * Use StringTypeNonCSAICollation for expressions supporting all possible collation types except
 * CS_AI collation types.
 */
case class StringTypeNonCSAICollation (override val supportsTrimCollation: Boolean = false)
  extends AbstractStringType(supportsTrimCollation) {
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[StringType] && other.asInstanceOf[StringType].isNonCSAI &&
      canUseTrimCollation(other)
}

object StringTypeNonCSAICollation extends StringTypeNonCSAICollation(false) {
  def apply(supportsTrimCollation: Boolean): StringTypeNonCSAICollation = {
    new StringTypeNonCSAICollation(supportsTrimCollation)
  }
}
