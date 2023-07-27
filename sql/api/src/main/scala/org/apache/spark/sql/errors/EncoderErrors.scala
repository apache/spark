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
package org.apache.spark.sql.errors

import org.apache.spark.{SparkBuildInfo, SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.types.UserDefinedType

object EncoderErrors extends DataTypeErrorsBase {
  def userDefinedTypeNotAnnotatedAndRegisteredError(udt: UserDefinedType[_]): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2155",
      messageParameters = Map(
        "userClass" -> udt.userClass.getName),
      cause = null)
  }

  def cannotFindEncoderForTypeError(typeName: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "ENCODER_NOT_FOUND",
      messageParameters = Map(
        "typeName" -> typeName,
        "docroot" -> SparkBuildInfo.spark_doc_root))
  }

  def cannotHaveCircularReferencesInBeanClassError(
      clazz: Class[_]): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2138",
      messageParameters = Map("clazz" -> clazz.toString()))
  }

  def cannotFindConstructorForTypeError(tpe: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2144",
      messageParameters = Map(
        "tpe" -> tpe))
  }

  def cannotHaveCircularReferencesInClassError(t: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2139",
      messageParameters = Map("t" -> t))
  }

  def cannotUseInvalidJavaIdentifierAsFieldNameError(
      fieldName: String, walkedTypePath: WalkedTypePath): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2140",
      messageParameters = Map(
        "fieldName" -> fieldName,
        "walkedTypePath" -> walkedTypePath.toString()))
  }

  def primaryConstructorNotFoundError(cls: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2021",
      messageParameters = Map("cls" -> cls.toString()))
  }
}
