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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PreparedExternalLookupJoin}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategies, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.python.PythonExternalLookupReaderFactory
import org.apache.spark.sql.internal.SQLConf

/** Plans the external lookup joins that [[PrepareExternalLookupJoin]] prepared. */
object ExternalLookupJoinStrategy extends SparkStrategy with QueryErrorsBase {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case lookup: PreparedExternalLookupJoin =>
      val realTimeMode = SparkStrategies.isRealTimeMode(lookup.child)
      // The lookups of Python data sources run in real-time mode only once it is turned on.
      if (realTimeMode && lookup.readerFactory.isInstanceOf[PythonExternalLookupReaderFactory] &&
          !SQLConf.get.pythonExternalLookupRealTimeModeEnabled) {
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "PYTHON_REAL_TIME_MODE",
          Map(
            "table" -> toSQLFreeFormId(lookup.tableName),
            "config" -> toSQLConf(SQLConf.PYTHON_EXTERNAL_LOOKUP_REAL_TIME_MODE_ENABLED.key)))
      }
      ExternalLookupJoinExec(
        lookup.tableName,
        lookup.lookupParameters,
        lookup.lookupInput,
        lookup.lookupOutput,
        lookup.lookupTransforms,
        lookup.lookupKeys,
        lookup.lookupKeyComparisons,
        lookup.joinType,
        lookup.residualCondition,
        lookup.scan,
        lookup.readerFactory,
        lookup.maxParameterRowsPerRequest,
        lookup.cacheOptions,
        realTimeMode,
        planLater(lookup.child)) :: Nil
    case _ => Nil
  }
}
