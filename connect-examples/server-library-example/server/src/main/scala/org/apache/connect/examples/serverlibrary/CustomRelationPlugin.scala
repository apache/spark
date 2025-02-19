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

package org.apache.connect.examples.serverlibrary

import java.util.Optional

import com.google.protobuf.Any
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin

import org.apache.connect.examples.serverlibrary.{CustomPluginBase, CustomTable}
import org.apache.connect.examples.serverlibrary.proto

/**
 * Relations are fundamental to dataset transformations, acting as the mechanism through which an
 * input dataset is transformed into an output dataset. Conceptually, relations can be likened to
 * tables within a database, manipulated to achieve desired outcomes.
 * In this example, the `CustomRelationPlugin` handles the transformation related to scanning
 * custom tables. The scan relation would appear as leaf nodes in a dataset's associated logical
 * plan node when it involves reads from the custom tables.
 */
class CustomRelationPlugin extends RelationPlugin with CustomPluginBase {

  /**
   * Transforms the raw byte array containing the relation into a Spark logical plan.
   *
   * @param raw The raw byte array of the relation.
   * @param planner The SparkConnectPlanner instance.
   * @return An Optional containing the LogicalPlan if the relation was processed, empty otherwise.
   */
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

  /**
   * Transforms the unpacked CustomRelation into a Spark logical plan.
   *
   * @param relation The unpacked CustomRelation.
   * @param planner The SparkConnectPlanner instance.
   * @return The corresponding Spark LogicalPlan.
   */
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

  /**
   * Transforms the Scan relation into a Spark logical plan.
   *
   * @param scan The Scan message.
   * @param planner The SparkConnectPlanner instance.
   * @return The corresponding Spark LogicalPlan.
   */
  private def transformScan(scan: proto.Scan, planner: SparkConnectPlanner): LogicalPlan = {
    val customTable = getCustomTable(scan.getTable)
    // Convert the custom table to a DataFrame and get its logical plan
    customTable.toDF().queryExecution.analyzed
  }
}
