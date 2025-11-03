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
package org.apache.spark.sql.classic

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UnboundRowEncoder
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expressions.SparkUserDefinedFunction

private[sql] object UserDefinedFunctionUtils {
  /**
   * Convert a UDF into an (executable) ScalaUDF expressions.
   *
   * This function should be moved to ScalaUDF when we move SparkUserDefinedFunction to sql/api.
   */
  def toScalaUDF(udf: SparkUserDefinedFunction, children: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      udf.f,
      udf.dataType,
      children,
      udf.inputEncoders.map(_.collect {
        // At some point it would be nice if were to support this.
        case e if e != UnboundRowEncoder => encoderFor(e)
      }),
      udf.outputEncoder.map(encoderFor(_)),
      udfName = udf.givenName,
      nullable = udf.nullable,
      udfDeterministic = udf.deterministic)
  }
}
