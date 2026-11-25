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

package org.apache.spark.sql.connect.rdd

import org.apache.spark.connect.proto
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connect.planner.{InvalidInputErrors, SparkConnectPlanner}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.python.PythonRDDOperators

/** Runs the commands of the RDD API of the Python client (see `RDDCommand` in rdd.proto). */
private[connect] class RDDCommandHandler(planner: SparkConnectPlanner) {

  private def session = planner.session

  /** Runs a command and returns its result. */
  def handle(command: proto.RDDCommand): proto.RDDCommandResult = {
    command.getCommandTypeCase match {
      case proto.RDDCommand.CommandTypeCase.DESCRIBE =>
        val plan = planner.transformRelation(command.getDescribe.getRdd)
        val numPartitions = PythonRDDOperators.numPartitions(plan).getOrElse {
          // The partitions of a DataFrame, as Spark Classic computes those of its RDD.
          val qe = Dataset.ofRows(session, plan).queryExecution
          SQLExecution.withNewExecutionId(qe, Some("getNumPartitions")) {
            qe.toRdd.getNumPartitions
          }
        }
        val info = proto.RDDCommandResult.Info.newBuilder().setNumPartitions(numPartitions)
        proto.RDDCommandResult.newBuilder().setInfo(info).build()

      case proto.RDDCommand.CommandTypeCase.GET_CONTEXT_INFO =>
        val sc = session.sparkContext
        val info = proto.RDDCommandResult.ContextInfo
          .newBuilder()
          .setDefaultParallelism(sc.defaultParallelism)
          .setDefaultParallelismSet(sc.conf.contains("spark.default.parallelism"))
          .setStartTime(sc.startTime)
          // As PySpark reads it from the SparkConf: no config entry defines it.
          .setPythonWorkerMemory(sc.conf.get("spark.python.worker.memory", "512m"))
        proto.RDDCommandResult.newBuilder().setContextInfo(info).build()

      case other =>
        throw InvalidInputErrors.invalidOneOfField(other, command.getDescriptorForType)
    }
  }
}
