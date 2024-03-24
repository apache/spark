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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}

object CollationTypeConstraints {

  def checkCollationCompatibility(collationId: Int, dataTypes: Seq[DataType]): TypeCheckResult = {
    val collationName = CollationFactory.fetchCollation(collationId).collationName
    // Additional check needed for collation compatibility
    dataTypes.collectFirst {
      case stringType: StringType if stringType.collationId != collationId =>
        val collation = CollationFactory.fetchCollation(stringType.collationId)
        DataTypeMismatch(
          errorSubClass = "COLLATION_MISMATCH",
          messageParameters = Map(
            "collationNameLeft" -> collationName,
            "collationNameRight" -> collation.collationName
          )
        )
    } getOrElse TypeCheckResult.TypeCheckSuccess
  }

}

/**
 * StringTypeCollated is an abstract class for StringType with collation support.
 */
abstract class StringTypeCollated extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType = StringType
}

/**
 * Use StringTypeBinary for expressions supporting only binary collation.
 */
case object StringTypeBinary extends StringTypeCollated {
  override private[sql] def simpleString: String = "string_binary"
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[StringType] && other.asInstanceOf[StringType].isBinaryCollation
}

/**
 * Use StringTypeBinaryLcase for expressions supporting only binary and lowercase collation.
 */
case object StringTypeBinaryLcase extends StringTypeCollated {
  override private[sql] def simpleString: String = "string_binary_lcase"
  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[StringType] && (other.asInstanceOf[StringType].isBinaryCollation ||
      other.asInstanceOf[StringType].isLowercaseCollation)
}

/**
 * Use StringTypeAnyCollation for expressions supporting all possible collation types.
 */
case object StringTypeAnyCollation extends StringTypeCollated {
  override private[sql] def simpleString: String = "string_any_collation"
  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[StringType]
}
