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

package org.example

import java.util.Optional

import com.google.protobuf.Any
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.example.{CustomPluginBase, CustomTable}
import org.example.proto

class CustomRelationPlugin extends RelationPlugin with CustomPluginBase {
  override def transform(
      raw: Array[Byte],
      planner: SparkConnectPlanner): Optional[LogicalPlan] = {
    val rel = Any.parseFrom(raw)
    if (rel.is(classOf[proto.CustomRelation])) {
      val customRelation = rel.unpack(classOf[proto.CustomRelation])
      // Transform the custom relation
      Optional.of(transformInner(customRelation, planner))
    } else {
      Optional.empty()
    }
  }

  private def transformInner(
      relation: proto.CustomRelation,
      planner: SparkConnectPlanner): LogicalPlan = {
    relation.getRelationTypeCase match {
      case proto.CustomRelation.RelationTypeCase.SCAN =>
        transformScan(relation.getScan, planner)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported relation type: ${relation.getRelationTypeCase}")
    }
  }

  private def transformScan(scan: proto.Scan, planner: SparkConnectPlanner): LogicalPlan = {
    val customTable = getCustomTable(scan.getTable)
    customTable.toDF().queryExecution.analyzed
  }
}
