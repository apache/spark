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

package org.apache.spark.sql.hive.orc

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.orc.{OrcFilters => BuiltinOrcFilters}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[orc] object HiveOrcFilters extends Logging {
  def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument] = {
    if (HiveUtils.isHive2) {
      BuiltinOrcFilters.createFilter(schema, filters).asInstanceOf[Option[SearchArgument]]
    } else {
      OrcFilters.createFilter(schema, filters)
    }
  }
}
