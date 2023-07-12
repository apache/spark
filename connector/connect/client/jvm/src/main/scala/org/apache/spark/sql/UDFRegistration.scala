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

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.{ScalarUserDefinedFunction, UserDefinedFunction}

/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this:
 *
 * {{{
 *   spark.udf
 * }}}
 *
 * @since 3.5.0
 */
class UDFRegistration(session: SparkSession) extends Logging {

  /**
   * Registers a user-defined function (UDF), for a UDF that's already defined using the Dataset
   * API (i.e. of type UserDefinedFunction). To change a UDF to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`. To change a UDF to nonNullable, call the API
   * `UserDefinedFunction.asNonNullable()`.
   *
   * Example:
   * {{{
   *   val foo = udf(() => Math.random())
   *   spark.udf.register("random", foo.asNondeterministic())
   *
   *   val bar = udf(() => "bar")
   *   spark.udf.register("stringLit", bar.asNonNullable())
   * }}}
   *
   * @param name
   *   the name of the UDF.
   * @param udf
   *   the UDF needs to be registered.
   * @return
   *   the registered UDF.
   *
   * @since 3.5.0
   */
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction = {
    udf.withName(name) match {
      case scalarUdf: ScalarUserDefinedFunction =>
        session.registerUdf(scalarUdf.toProto)
        scalarUdf
      case other =>
        throw new UnsupportedOperationException(
          s"Registering a UDF of type " +
            s"${other.getClass.getSimpleName} is currently unsupported.")
    }
  }
}
