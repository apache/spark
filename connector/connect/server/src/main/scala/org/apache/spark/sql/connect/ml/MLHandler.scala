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

package org.apache.spark.sql.connect.ml

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.service.SessionHolder

object MLHandler {

  def handleMlCommand(
                       sessionHolder: SessionHolder,
                       mlCommand: proto.MlCommand
  ): proto.MlCommandResponse = {
    mlCommand.getMlCommandTypeCase match {
      case proto.MlCommand.MlCommandTypeCase.FIT =>
        val fitCommandProto = mlCommand.getFit
        val estimatorProto = fitCommandProto.getEstimator
        assert(estimatorProto.getType == proto.MlStage.StageType.ESTIMATOR)

        val algoName = fitCommandProto.getEstimator.getName
        val algo = AlgorithmRegistry.get(algoName)

        val estimator = algo.initiateEstimator(estimatorProto.getUid)
        MLUtils.setInstanceParams(estimator, estimatorProto.getParams)
        val dataset = MLUtils.parseRelationProto(fitCommandProto.getDataset, sessionHolder)
        val model = estimator.fit(dataset).asInstanceOf[Model[_]]
        val refId = sessionHolder.mlCache.modelCache.register(model, algo)

        proto.MlCommandResponse.newBuilder().setModelInfo(
          proto.MlCommandResponse.ModelInfo.newBuilder
            .setModelRefId(refId)
            .setModelUid(model.uid)
        ).build()

      case proto.MlCommand.MlCommandTypeCase.FETCH_MODEL_ATTR =>
        val getModelAttrProto = mlCommand.getFetchModelAttr
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          getModelAttrProto.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        algo.getModelAttr(model, getModelAttrProto.getName).left.get

      case proto.MlCommand.MlCommandTypeCase.FETCH_MODEL_SUMMARY_ATTR =>
        val getModelSummaryAttrProto = mlCommand.getFetchModelSummaryAttr
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          getModelSummaryAttrProto.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        // Create a copied model to avoid concurrently modify model params.
        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
        MLUtils.setInstanceParams(copiedModel, getModelSummaryAttrProto.getParams)

        val datasetOpt = if (getModelSummaryAttrProto.hasEvaluationDataset) {
          val evalDF = MLUtils.parseRelationProto(
            getModelSummaryAttrProto.getEvaluationDataset,
            sessionHolder
          )
          Some(evalDF)
        } else None

        algo.getModelSummaryAttr(
          copiedModel,
          getModelSummaryAttrProto.getName,
          datasetOpt
        ).left.get

      case proto.MlCommand.MlCommandTypeCase.LOAD_MODEL =>
        val loadModelProto = mlCommand.getLoadModel
        val algo = AlgorithmRegistry.get(loadModelProto.getName)
        val model = algo.loadModel(loadModelProto.getPath)
        val refId = sessionHolder.mlCache.modelCache.register(model, algo)

        proto.MlCommandResponse.newBuilder().setModelInfo(
          proto.MlCommandResponse.ModelInfo.newBuilder
            .setModelRefId(refId)
            .setModelUid(model.uid)
            .setParams(MLUtils.convertInstanceParamsToProto(model))
        ).build()

      case proto.MlCommand.MlCommandTypeCase.SAVE_MODEL =>
        val saveModelProto = mlCommand.getSaveModel
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          saveModelProto.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        algo.saveModel(
          model,
          saveModelProto.getPath,
          saveModelProto.getOverwrite,
          saveModelProto.getOptionsMap.asScala.toMap
        )
        proto.MlCommandResponse.newBuilder().setLiteral(
          LiteralValueProtoConverter.toLiteralProto(null)
        ).build()

      case proto.MlCommand.MlCommandTypeCase.LOAD_STAGE =>
        val loadStageProto = mlCommand.getLoadStage
        val name = loadStageProto.getName
        loadStageProto.getType match {
          case proto.MlStage.StageType.ESTIMATOR =>
            val algo = AlgorithmRegistry.get(name)
            val estimator = algo.loadEstimator(loadStageProto.getPath)

            proto.MlCommandResponse.newBuilder().setStage(
              proto.MlStage.newBuilder()
                .setName(name)
                .setType(proto.MlStage.StageType.ESTIMATOR)
                .setUid(estimator.uid)
                .setParams(MLUtils.convertInstanceParamsToProto(estimator))
            ).build()
          case _ =>
            throw new UnsupportedOperationException()
        }

      case proto.MlCommand.MlCommandTypeCase.SAVE_STAGE =>
        val saveStageProto = mlCommand.getSaveStage
        val stageProto = saveStageProto.getStage

        stageProto.getType match {
          case proto.MlStage.StageType.ESTIMATOR =>
            val name = stageProto.getName
            val algo = AlgorithmRegistry.get(name)
            val estimator = algo.initiateEstimator(stageProto.getUid)
            MLUtils.setInstanceParams(estimator, stageProto.getParams)
            algo.saveEstimator(
              estimator,
              saveStageProto.getPath,
              saveStageProto.getOverwrite,
              saveStageProto.getOptionsMap.asScala.toMap
            )
            proto.MlCommandResponse.newBuilder().setLiteral(
              LiteralValueProtoConverter.toLiteralProto(null)
            ).build()

          case _ =>
            throw new UnsupportedOperationException()
        }

      case proto.MlCommand.MlCommandTypeCase.COPY_MODEL =>
        val copyModelProto = mlCommand.getCopyModel
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          copyModelProto.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
        val refId = sessionHolder.mlCache.modelCache.register(copiedModel, algo)
        proto.MlCommandResponse.newBuilder().setLiteral(
          proto.Expression.Literal.newBuilder().setLong(refId)
        ).build()
      case _ =>
        throw new IllegalArgumentException()
    }
  }

  def transformMLRelation(
                           mlRelationProto: proto.MlRelation,
                           sessionHolder: SessionHolder): DataFrame = {
    mlRelationProto.getMlRelationTypeCase match {
      case proto.MlRelation.MlRelationTypeCase.MODEL_TRANSFORM =>
        val modelTransformRelationProto = mlRelationProto.getModelTransform
        val (model, _) = sessionHolder.mlCache.modelCache.get(
          modelTransformRelationProto.getModelRefId
        )
        // Create a copied model to avoid concurrently modify model params.
        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
        MLUtils.setInstanceParams(copiedModel, modelTransformRelationProto.getParams)
        val inputDF = MLUtils.parseRelationProto(
          modelTransformRelationProto.getInput,
          sessionHolder
        )
        copiedModel.transform(inputDF)

      case proto.MlRelation.MlRelationTypeCase.MODEL_ATTR =>
        val modelAttrProto = mlRelationProto.getModelAttr
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          modelAttrProto.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        algo.getModelAttr(model, modelAttrProto.getName).right.get

      case proto.MlRelation.MlRelationTypeCase.MODEL_SUMMARY_ATTR =>
        val modelSummaryAttr = mlRelationProto.getModelSummaryAttr
        val modelEntry = sessionHolder.mlCache.modelCache.get(
          modelSummaryAttr.getModelRefId
        )
        val model = modelEntry._1
        val algo = modelEntry._2
        // Create a copied model to avoid concurrently modify model params.
        val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
        MLUtils.setInstanceParams(copiedModel, modelSummaryAttr.getParams)

        val datasetOpt = if (modelSummaryAttr.hasEvaluationDataset) {
          val evalDF = MLUtils.parseRelationProto(
            modelSummaryAttr.getEvaluationDataset,
            sessionHolder
          )
          Some(evalDF)
        } else {
          None
        }
        algo.getModelSummaryAttr(
          copiedModel,
          modelSummaryAttr.getName,
          datasetOpt
        ).right.get

      case _ =>
        throw new IllegalArgumentException()
    }
  }
}
