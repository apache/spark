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
package org.apache.spark.sql.protobuf

import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, StringLiteral}

/**
 * Register a couple Protobuf functions in the internal function registry.
 *
 * This is a bit of a hack because we use the [[SparkSessionExtensions]] mechanism to register
 * functions in a global registry. The use of a Scala object makes sure we only do this once.
 */
object InternalFunctionRegistration {
  def apply(): Unit = ()

  private def registerFunction(name: String)(builder: Seq[Expression] => Expression): Unit = {
    FunctionRegistry.internal.createOrReplaceTempFunction(name, builder, "internal")
  }

  registerFunction("from_protobuf") {
    case Seq(input, StringLiteral(messageName), descriptor, options) =>
      ProtobufDataToCatalyst(
        input,
        messageName,
        Some(descriptor).asInstanceOf[Option[Array[Byte]]],
        options.asInstanceOf[Map[String, String]])
  }
}

class InternalFunctionRegistration extends SparkSessionExtensionsProvider {
  override def apply(e: SparkSessionExtensions): Unit = InternalFunctionRegistration()
}
