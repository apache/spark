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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.pipelines.common.DatasetType

object GraphElementTypeUtils {

  /**
   * Helper function to obtain the DatasetType. This function should be called with all
   * the flows that are writing into a table and all the flows should be resolved.
   */
  def getDatasetTypeForMaterializedViewOrStreamingTable(
      flowsToTable: Seq[ResolvedFlow]): DatasetType = {
    val isStreamingTable = flowsToTable.exists(f => f.df.isStreaming || f.once)
    if (isStreamingTable) {
      DatasetType.STREAMING_TABLE
    } else {
      DatasetType.MATERIALIZED_VIEW
    }
  }
}
