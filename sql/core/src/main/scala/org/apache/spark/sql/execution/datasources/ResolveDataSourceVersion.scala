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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{CONTINUOUS_READ, MICRO_BATCH_READ}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object ResolveDataSourceVersion extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r@VersionUnresolvedRelation(v1DataSource, _, _) =>
      val sparkSession = r.sparkSession
      val ds = DataSource.lookupDataSource(r.source, sparkSession.sqlContext.conf).
        getConstructor().newInstance() // TableProvider or Source

      val v1Relation = ds match {
        case _: StreamSourceProvider => Some(StreamingRelation(v1DataSource))
        case _ => None
      }
      ds match {
        // file source v2 does not support streaming yet.
        case provider: TableProvider if !provider.isInstanceOf[FileDataSourceV2] =>
          DataSourceV2Utils.loadV2StreamingSource(
            sparkSession, provider, r.userSpecifiedSchema, r.options, r.source, v1Relation)
            .getOrElse(StreamingRelation(v1DataSource))

        // fallback to v1
        case _ => StreamingRelation(v1DataSource)
      }
  }
}
