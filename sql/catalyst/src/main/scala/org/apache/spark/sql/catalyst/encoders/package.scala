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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.errors.QueryExecutionErrors

package object encoders {
  /**
   * Returns an internal encoder object that can be used to serialize / deserialize JVM objects
   * into Spark SQL rows.  The implicit encoder should always be unresolved (i.e. have no attribute
   * references from a specific schema.)  This requirement allows us to preserve whether a given
   * object type is being bound by name or by ordinal when doing resolution.
   */
  def encoderFor[A : Encoder]: ExpressionEncoder[A] = implicitly[Encoder[A]] match {
    case e: ExpressionEncoder[A] =>
      e.assertUnresolved()
      e
    case a: AgnosticEncoder[A] => ExpressionEncoder(a)
    case other => throw QueryExecutionErrors.invalidExpressionEncoderError(other.getClass.getName)
  }
}
