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

package org.apache.spark.sql.connect.service

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput, StorageLevelProtoConverter}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.execution.{CodegenMode, CostMode, ExtendedMode, FormattedMode, SimpleMode}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.ArrayImplicits._

private[connect] class SparkConnectAnalyzeHandler(
    responseObserver: StreamObserver[proto.AnalyzePlanResponse])
    extends Logging {

  def handle(request: proto.AnalyzePlanRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder = SparkConnectService.getOrCreateIsolatedSession(
      request.getUserContext.getUserId,
      request.getSessionId,
      previousSessionId)
    // `withSession` ensures that session-specific artifacts (such as JARs and class files) are
    // available during processing (such as deserialization).
    sessionHolder.withSession { _ =>
      val response = process(request, sessionHolder)
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    }
  }

  def process(
      request: proto.AnalyzePlanRequest,
      sessionHolder: SessionHolder): proto.AnalyzePlanResponse = {
    lazy val planner = new SparkConnectPlanner(sessionHolder)
    val session = sessionHolder.session
    val builder = proto.AnalyzePlanResponse.newBuilder()

    def transformRelation(rel: proto.Relation) = planner.transformRelation(rel, cachePlan = true)

    request.getAnalyzeCase match {
      case proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA =>
        val schema = Dataset
          .ofRows(session, transformRelation(request.getSchema.getPlan.getRoot))
          .schema
        builder.setSchema(
          proto.AnalyzePlanResponse.Schema
            .newBuilder()
            .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN =>
        val queryExecution = Dataset
          .ofRows(session, transformRelation(request.getExplain.getPlan.getRoot))
          .queryExecution
        val explainString = request.getExplain.getExplainMode match {
          case proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE =>
            queryExecution.explainString(SimpleMode)
          case proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED =>
            queryExecution.explainString(ExtendedMode)
          case proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN =>
            queryExecution.explainString(CodegenMode)
          case proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST =>
            queryExecution.explainString(CostMode)
          case proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED =>
            queryExecution.explainString(FormattedMode)
          case other => throw new UnsupportedOperationException(s"Unknown Explain Mode $other!")
        }
        builder.setExplain(
          proto.AnalyzePlanResponse.Explain
            .newBuilder()
            .setExplainString(explainString)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.TREE_STRING =>
        val schema = Dataset
          .ofRows(session, transformRelation(request.getTreeString.getPlan.getRoot))
          .schema
        val treeString = if (request.getTreeString.hasLevel) {
          schema.treeString(request.getTreeString.getLevel)
        } else {
          schema.treeString
        }
        builder.setTreeString(
          proto.AnalyzePlanResponse.TreeString
            .newBuilder()
            .setTreeString(treeString)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL =>
        val isLocal = Dataset
          .ofRows(session, transformRelation(request.getIsLocal.getPlan.getRoot))
          .isLocal
        builder.setIsLocal(
          proto.AnalyzePlanResponse.IsLocal
            .newBuilder()
            .setIsLocal(isLocal)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING =>
        val isStreaming = Dataset
          .ofRows(session, transformRelation(request.getIsStreaming.getPlan.getRoot))
          .isStreaming
        builder.setIsStreaming(
          proto.AnalyzePlanResponse.IsStreaming
            .newBuilder()
            .setIsStreaming(isStreaming)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES =>
        val inputFiles = Dataset
          .ofRows(session, transformRelation(request.getInputFiles.getPlan.getRoot))
          .inputFiles
        builder.setInputFiles(
          proto.AnalyzePlanResponse.InputFiles
            .newBuilder()
            .addAllFiles(inputFiles.toImmutableArraySeq.asJava)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION =>
        builder.setSparkVersion(
          proto.AnalyzePlanResponse.SparkVersion
            .newBuilder()
            .setVersion(session.version)
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.DDL_PARSE =>
        val schema = planner.parseDatatypeString(request.getDdlParse.getDdlString)
        builder.setDdlParse(
          proto.AnalyzePlanResponse.DDLParse
            .newBuilder()
            .setParsed(DataTypeProtoConverter.toConnectProtoType(schema))
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.SAME_SEMANTICS =>
        val target = Dataset.ofRows(
          session,
          transformRelation(request.getSameSemantics.getTargetPlan.getRoot))
        val other = Dataset.ofRows(
          session,
          transformRelation(request.getSameSemantics.getOtherPlan.getRoot))
        builder.setSameSemantics(
          proto.AnalyzePlanResponse.SameSemantics
            .newBuilder()
            .setResult(target.sameSemantics(other)))

      case proto.AnalyzePlanRequest.AnalyzeCase.SEMANTIC_HASH =>
        val semanticHash = Dataset
          .ofRows(session, transformRelation(request.getSemanticHash.getPlan.getRoot))
          .semanticHash()
        builder.setSemanticHash(
          proto.AnalyzePlanResponse.SemanticHash
            .newBuilder()
            .setResult(semanticHash))

      case proto.AnalyzePlanRequest.AnalyzeCase.PERSIST =>
        val target = Dataset
          .ofRows(session, transformRelation(request.getPersist.getRelation))
        if (request.getPersist.hasStorageLevel) {
          target.persist(
            StorageLevelProtoConverter.toStorageLevel(request.getPersist.getStorageLevel))
        } else {
          target.persist()
        }
        builder.setPersist(proto.AnalyzePlanResponse.Persist.newBuilder().build())

      case proto.AnalyzePlanRequest.AnalyzeCase.UNPERSIST =>
        val target = Dataset
          .ofRows(session, transformRelation(request.getUnpersist.getRelation))
        if (request.getUnpersist.hasBlocking) {
          target.unpersist(request.getUnpersist.getBlocking)
        } else {
          target.unpersist()
        }
        builder.setUnpersist(proto.AnalyzePlanResponse.Unpersist.newBuilder().build())

      case proto.AnalyzePlanRequest.AnalyzeCase.GET_STORAGE_LEVEL =>
        val target = Dataset
          .ofRows(session, transformRelation(request.getGetStorageLevel.getRelation))
        val storageLevel = target.storageLevel
        builder.setGetStorageLevel(
          proto.AnalyzePlanResponse.GetStorageLevel
            .newBuilder()
            .setStorageLevel(StorageLevelProtoConverter.toConnectProtoType(storageLevel))
            .build())

      case proto.AnalyzePlanRequest.AnalyzeCase.JSON_TO_DDL =>
        val ddl = DataType
          .fromJson(request.getJsonToDdl.getJsonString)
          .asInstanceOf[StructType]
          .toDDL
        builder.setJsonToDdl(
          proto.AnalyzePlanResponse.JsonToDDL
            .newBuilder()
            .setDdlString(ddl)
            .build())

      case other => throw InvalidPlanInput(s"Unknown Analyze Method $other!")
    }

    builder
      .setSessionId(request.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
    builder.build()
  }
}
