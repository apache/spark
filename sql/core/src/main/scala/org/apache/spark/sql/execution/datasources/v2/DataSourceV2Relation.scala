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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.sources.v2.reader._

case class DataSourceV2Relation(
    fullOutput: Seq[AttributeReference],
    reader: DataSourceV2Reader) extends LeafNode with DataSourceReaderHolder {

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataSourceV2Relation]

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

/**
 * A specialization of DataSourceV2Relation with the streaming bit set to true. Otherwise identical
 * to the non-streaming relation.
 */
class StreamingDataSourceV2Relation(
    fullOutput: Seq[AttributeReference],
    reader: DataSourceV2Reader) extends DataSourceV2Relation(fullOutput, reader) {
  override def isStreaming: Boolean = true
}

object DataSourceV2Relation {
  def apply(reader: DataSourceV2Reader): DataSourceV2Relation = {
    new DataSourceV2Relation(reader.readSchema().toAttributes, reader)
  }
}
