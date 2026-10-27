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

package org.apache.spark.sql.execution.planmerging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * Collects the DSv2 scans of a plan for the scan-merging suites in this package. Shared so that a
 * change to how merged scans are collected reaches every suite that measures merging.
 */
private[planmerging] trait V2ScanMergingTestHelper {

  protected def v2Scans(df: DataFrame): Seq[DataSourceV2ScanRelation] =
    df.queryExecution.optimizedPlan.collectWithSubqueries {
      case s: DataSourceV2ScanRelation => s
    }

  /**
   * A merged subquery is referenced once per original subquery, so the logical plan duplicates it
   * (physical planning reuses it). Dedupe by canonical form: one distinct scan is consistent with a
   * merge and more than one means it was declined. Scans that read the same columns and carry the
   * same filters canonicalize equal either way, so use subquery counts for those.
   */
  protected def distinctScans(df: DataFrame): Int = v2Scans(df).map(_.canonicalized).distinct.length
}
