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

package org.apache.spark.sql.types.ops

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.util.TimeFormatter
import org.apache.spark.sql.types.TimeType

class TimeTypeApiOps(t: TimeType) extends TypeApiOps with EncodeTypeOps with FormatTypeOps {
  private lazy val fracFormatter = TimeFormatter.getFractionFormatter()

  override def getEncoder: AgnosticEncoder[_] = LocalTimeEncoder

  override def format(v: Any): String = fracFormatter.format(v.asInstanceOf[Long])
  override def toSQLValue(v: Any): String = s"TIME '${format(v)}'"
}
