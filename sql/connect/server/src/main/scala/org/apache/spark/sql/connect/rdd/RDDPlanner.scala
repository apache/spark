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

import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.{PythonBroadcast, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.{InvalidInputErrors, SparkConnectPlanner}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * Plans the relations of the RDD API of the Python client (see `RDDRelation` in relations.proto)
 * as the logical operators of PythonRDDOperators.scala, which Catalyst optimizes and plans like
 * the other operators of the DataFrames of the session.
 */
private[connect] class RDDPlanner(planner: SparkConnectPlanner) {

  private val sessionHolder = planner.sessionHolder

  private def session = planner.session

  /** The logical plan of an [[proto.RDDRelation]]. */
  def transformRDDRelation(rel: proto.RDDRelation): LogicalPlan = {
    rel.getRelationTypeCase match {
      case proto.RDDRelation.RelationTypeCase.MAP_PARTITIONS =>
        val mapPartitions = rel.getMapPartitions
        val input = planner.transformRelation(mapPartitions.getInput)
        val hashSeed = if (mapPartitions.hasPythonHashSeed) {
          Some(mapPartitions.getPythonHashSeed)
        } else {
          None
        }
        val func = SimplePythonFunction(
          command = mapPartitions.getCommand.toByteArray.toImmutableArraySeq,
          envVars = workerEnv(hashSeed),
          pythonIncludes = sessionHolder.artifactManager.getPythonIncludes.asJava,
          pythonExec = planner.pythonExec,
          pythonVer = mapPartitions.getPythonVer,
          broadcastVars = new JArrayList[Broadcast[PythonBroadcast]](),
          accumulator = sessionHolder.pythonAccumulator.orNull)
        PythonRDDMapPartitions(
          func,
          mapPartitions.getIsBarrier,
          mapPartitions.getOutputPairs,
          input)

      case proto.RDDRelation.RelationTypeCase.VALUES =>
        PythonRDDValues(planner.transformRelation(rel.getValues.getInput))

      case proto.RDDRelation.RelationTypeCase.UNION =>
        val inputs = rel.getUnion.getInputsList.asScala.map(planner.transformRelation).toSeq
        // The union of no RDDs is an RDD without partitions, as `SparkContext.emptyRDD`.
        val output = if (inputs.isEmpty) {
          PythonRDDOperators.valueOutput()
        } else {
          PythonRDDOperators.concatenatedOutput(inputs)
        }
        PythonRDDUnion(output, inputs)

      case proto.RDDRelation.RelationTypeCase.ZIP_PARTITIONS =>
        val zip = rel.getZipPartitions
        val inputs = zip.getInputsList.asScala.map(planner.transformRelation).toSeq
        if (inputs.isEmpty) {
          throw InvalidInputErrors.cannotBeEmpty("inputs", zip.getDescriptorForType)
        }
        val output = if (zip.getConcatenate) {
          PythonRDDOperators.concatenatedOutput(inputs)
        } else {
          if (inputs.length != 2) {
            throw InvalidInputErrors.rddZipInvalidInputs(inputs.length)
          }
          PythonRDDOperators.joinedOutput(inputs)
        }
        PythonRDDZipPartitions(zip.getConcatenate, output, inputs)

      case proto.RDDRelation.RelationTypeCase.CARTESIAN =>
        val cartesian = rel.getCartesian
        val left = planner.transformRelation(cartesian.getLeft)
        val right = planner.transformRelation(cartesian.getRight)
        PythonRDDCartesian(PythonRDDOperators.joinedOutput(Seq(left, right)), left, right)

      case proto.RDDRelation.RelationTypeCase.COALESCE =>
        val coalesce = rel.getCoalesce
        PythonRDDCoalesce(coalesce.getNumPartitions, planner.transformRelation(coalesce.getInput))

      case proto.RDDRelation.RelationTypeCase.PARALLELIZE =>
        val parallelize = rel.getParallelize
        val data = planner.transformRelation(parallelize.getData)
        if (data.output.length != 1 || data.output.head.dataType != BinaryType) {
          throw InvalidInputErrors.rddParallelizeInvalidData(data.schema)
        }
        PythonRDDParallelize(parallelize.getNumSlices, data)

      case proto.RDDRelation.RelationTypeCase.FROM_DATA_FRAME =>
        PythonRDDFromDataFrame(planner.transformRelation(rel.getFromDataFrame.getInput))

      case proto.RDDRelation.RelationTypeCase.TO_DATA_FRAME =>
        val toDataFrame = rel.getToDataFrame
        val schema = planner.transformDataType(toDataFrame.getSchema) match {
          case s: StructType => s
          case other => throw InvalidInputErrors.invalidSchemaTypeNonStruct(other)
        }
        PythonRDDToDataFrame(schema, planner.transformRelation(toDataFrame.getInput))

      case proto.RDDRelation.RelationTypeCase.ELEMENTS =>
        val elements = rel.getElements
        val input = planner.transformRelation(elements.getInput)
        val partitions = if (elements.hasPartitions) {
          Some(elements.getPartitions.getValuesList.asScala.map(_.intValue).toSeq)
        } else {
          None
        }
        PythonRDDElements(partitions, input)

      case proto.RDDRelation.RelationTypeCase.WITH_RESOURCES =>
        val withResources = rel.getWithResources
        val profile = session.sparkContext.resourceProfileManager
          .resourceProfileFromId(withResources.getProfileId)
        PythonRDDWithResources(profile, planner.transformRelation(withResources.getInput))

      case other =>
        throw InvalidInputErrors.invalidOneOfField(other, rel.getDescriptorForType)
    }
  }

  /**
   * The environment of the Python workers, as PySpark sets it: the PYTHONHASHSEED of the client,
   * or else of the server. It also enables the profiler of the commands (see
   * `pyspark.core.connect._runtime.RDDProfiler`) when the session profiles Python UDFs.
   */
  private def workerEnv(hashSeed: Option[String]): JHashMap[String, String] = {
    val env = new JHashMap[String, String]()
    env.put("PYTHONHASHSEED", hashSeed.getOrElse(sys.env.getOrElse("PYTHONHASHSEED", "0")))
    val conf = session.sessionState.conf
    conf.pythonUDFProfiler.foreach { profiler =>
      env.put("PYSPARK_CONNECT_RDD_PROFILER", profiler)
      if (profiler == "sampling") {
        env.put(
          "PYSPARK_CONNECT_RDD_PROFILER_SAMPLING_INTERVAL",
          conf.pythonUDFProfilerSamplingInterval.toString)
        env.put(
          "PYSPARK_CONNECT_RDD_PROFILER_SAMPLING_AGGREGATION",
          conf.pythonUDFProfilerSamplingAggregation)
      }
    }
    env
  }
}
