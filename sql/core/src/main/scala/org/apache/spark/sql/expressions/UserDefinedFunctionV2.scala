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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.JavaUDF
import org.apache.spark.sql.types.DataType

@InterfaceStability.Stable
class UserDefinedFunctionV2 private[sql] (
    f: AnyRef,
    dataType: DataType,
    inputTypes: Seq[DataType],
    nameOption: Option[String] = None,
    nullable: Boolean = true,
    deterministic: Boolean = true) {

  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    Column(JavaUDF(
      f,
      dataType,
      exprs.map(_.expr),
      inputTypes,
      nameOption,
      nullable,
      deterministic))
  }
}

object UserDefinedFunctionV2 {

  // java api, 0 args
  def udfv2(f: UDF0[_], returnType: DataType, nameOption: Option[String], nullable: Boolean,
    deterministic: Boolean): UserDefinedFunctionV2 = {
    val func = f.asInstanceOf[UDF0[Any]].call()
    new UserDefinedFunctionV2(() => func, returnType, Seq.empty[DataType],
      nameOption, nullable, deterministic)
  }

  // java api, 1 args
  def udfv2(f: UDF1[_, _], returnType: DataType, inputType: DataType, nameOption: Option[String],
    nullable: Boolean, deterministic: Boolean): UserDefinedFunctionV2 = {
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    new UserDefinedFunctionV2(func, returnType, Seq(inputType),
      nameOption, nullable, deterministic)
  }

  // java api, 2 args
  def udfv2(f: UDF2[_, _, _], returnType: DataType, input1Type: DataType, input2Type: DataType,
    nameOption: Option[String], nullable: Boolean, deterministic: Boolean)
    : UserDefinedFunctionV2 = {
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    new UserDefinedFunctionV2(func, returnType, Seq(input1Type, input2Type),
      nameOption, nullable, deterministic)
  }

  // Discussion: Whether we need to automatically extract type from scala function.
  // scala api, 0 args
  def udfv2(f: Function0[_], returnType: DataType, nameOption: Option[String], nullable: Boolean,
    deterministic: Boolean): UserDefinedFunctionV2 = {
    new UserDefinedFunctionV2(f, returnType, Seq.empty[DataType],
      nameOption, nullable, deterministic)
  }

  // scala api, 1 args
  def udfv2(f: Function1[_, _], returnType: DataType, inputType: DataType,
    nameOption: Option[String], nullable: Boolean, deterministic: Boolean)
    : UserDefinedFunctionV2 = {
    new UserDefinedFunctionV2(f, returnType, Seq(inputType),
      nameOption, nullable, deterministic)
  }

  // scala api, 2 args
  def udfv2(f: Function2[_, _, _], returnType: DataType, input1Type: DataType, input2Type: DataType,
    nameOption: Option[String], nullable: Boolean, deterministic: Boolean)
    : UserDefinedFunctionV2 = {
    new UserDefinedFunctionV2(f, returnType, Seq(input1Type, input2Type),
      nameOption, nullable, deterministic)
  }
}