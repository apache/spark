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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileDataSourceV2, FileTable}

/**
 * Replace the V2 data source of table in [[InsertIntoTable]] to V1 [[FileFormat]].
 * E.g, with temporary view `t` using [[FileDataSourceV2]], inserting into  view `t` fails
 * since there is no correspoding physical plan.
 * This is a temporary hack for making current data source V2 work. It should be removed
 * when write path of file data source v2 is finished.
 */
class FallBackFileDataSourceToV1(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(d @
        DataSourceV2Relation(source: FileDataSourceV2, table: FileTable, _, _, _), _, _, _, _) =>
      val v1FileFormat = source.fallBackFileFormat.getConstructor().newInstance()
      val relation = HadoopFsRelation(table.getFileIndex, table.getFileIndex.partitionSchema,
        table.schema(), None, v1FileFormat, d.options)(sparkSession)
      i.copy(table = LogicalRelation(relation))
  }
}
