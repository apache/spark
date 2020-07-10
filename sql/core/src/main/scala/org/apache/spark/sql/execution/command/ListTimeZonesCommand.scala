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

package org.apache.spark.sql.execution.command

import java.util.TimeZone

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

/**
 * This command is for retrieving all the supported region-based Zone IDs supported.
 * Command that runs
 * {{{
 *   SET TIME ZONE ALL;
 * }}}
 */
case object ListTimeZonesCommand extends RunnableCommand {
  override val output: Seq[Attribute] =
    Seq(AttributeReference("id", StringType, nullable = false)(),
      AttributeReference("name", StringType, nullable = false)(),
      AttributeReference("offset", IntegerType, nullable = false)(),
      AttributeReference("useDaylight", BooleanType, nullable = false)())
  override def run(sparkSession: SparkSession): Seq[Row] = {
    TimeZone.getAvailableIDs().map { zid =>
      val zone = TimeZone.getTimeZone(zid)
      val zoneName = zone.getDisplayName(TimestampFormatter.defaultLocale)
      val rawOffset = zone.getRawOffset
      val useDaylight = zone.useDaylightTime()
      Row(zid, zoneName, rawOffset, useDaylight)
    }
  }
}
