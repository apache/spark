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

import org.apache.spark.sql.catalyst.expressions.{Cast, CurrentDate, Expression, MakeTimestampNTZ}
import org.apache.spark.sql.types.{TimestampNTZType, TimeType}

/**
 * Rewrites Cast from TIME -> TIMESTAMP_NTZ to MakeTimestampNTZ(CurrentDate(), TIME)
 *
 * Example: CAST(TIME '15:30:00' AS TIMESTAMP_NTZ) => MakeTimestampNTZ(CurrentDate(),
 * Literal(15:30:00))
 */
object RewriteTimeCastToTimestampNTZ {
  def rewrite(expr: Expression): Expression = expr match {
    case c @ Cast(child, TimestampNTZType, _, _)
        if child.resolved && child.dataType.isInstanceOf[TimeType] =>
      MakeTimestampNTZ(CurrentDate(), child)
    case other =>
      other
  }
}
