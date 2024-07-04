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

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.ml.MLUtils.loadModel
import org.apache.spark.sql.connect.ml.Serializer.deserializeMethodArguments
import org.apache.spark.sql.connect.service.SessionHolder

private class ModelAttributeHelper(
    val sessionHolder: SessionHolder,
    val objIdentifier: String,
    val method: Option[String],
    val argValues: Array[Object] = Array.empty,
    val argClasses: Array[Class[_]] = Array.empty) {

  val methodChain = method.map(n => s"$objIdentifier.$n").getOrElse(objIdentifier)
  private val methodChains = methodChain.split("\\.")
  private val modelId = methodChains.head

  private lazy val model = sessionHolder.mlCache.get(modelId)
  private lazy val methods = methodChains.slice(1, methodChains.length)

  def getAttribute: Any = {
    assert(methods.length >= 1)
    if (argValues.length == 0) {
      methods.foldLeft(model.asInstanceOf[Object]) { (obj, attribute) =>
        MLUtils.invokeMethodAllowed(obj, attribute)
      }
    } else {
      val lastMethod = methods.last
      if (methods.length == 1) {
        MLUtils.invokeMethodAllowed(model.asInstanceOf[Object], lastMethod, argValues, argClasses)
      } else {
        val prevMethods = methods.slice(0, methods.length - 1)
        val finalObj = prevMethods.foldLeft(model.asInstanceOf[Object]) { (obj, attribute) =>
          MLUtils.invokeMethodAllowed(obj, attribute)
        }
        MLUtils.invokeMethodAllowed(finalObj, lastMethod, argValues, argClasses)
      }
    }
  }

  def transform(relation: proto.MlRelation.Transform): DataFrame = {
    // Create a copied model to avoid concurrently modify model params.
    val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
    MLUtils.setInstanceParams(copiedModel, relation.getParams)
    val inputDF = MLUtils.parseRelationProto(relation.getInput, sessionHolder)
    copiedModel.transform(inputDF)
  }
}

private object ModelAttributeHelper {
  def apply(
      sessionHolder: SessionHolder,
      modelId: String,
      method: Option[String] = None,
      args: Array[proto.FetchModelAttr.Args] = Array.empty): ModelAttributeHelper = {
    val tmp = deserializeMethodArguments(args, sessionHolder)
    val argValues = tmp.map(_._1)
    val argClasses = tmp.map(_._2)
    new ModelAttributeHelper(sessionHolder, modelId, method, argValues, argClasses)
  }
}

object MLHandler extends Logging {
  def handleMlCommand(
      sessionHolder: SessionHolder,
      mlCommand: proto.MlCommand): proto.MlCommandResponse = {

    val mlCache = sessionHolder.mlCache

    mlCommand.getCommandCase match {
      case proto.MlCommand.CommandCase.FIT =>
        val fitCmd = mlCommand.getFit
        val estimatorProto = fitCmd.getEstimator
        assert(estimatorProto.getType == proto.MlOperator.OperatorType.ESTIMATOR)

        val dataset = MLUtils.parseRelationProto(fitCmd.getDataset, sessionHolder)
        val estimator = MLUtils.getEstimator(fitCmd)
        val model = estimator.fit(dataset).asInstanceOf[Model[_]]
        val id = mlCache.register(model)
        proto.MlCommandResponse
          .newBuilder()
          .setOperatorInfo(
            proto.MlCommandResponse.MlOperatorInfo
              .newBuilder()
              .setModelRef(proto.ModelRef.newBuilder().setId(id)))
          .build()

      case proto.MlCommand.CommandCase.FETCH_MODEL_ATTR =>
        val args = mlCommand.getFetchModelAttr.getArgsList.asScala.toArray
        val helper = ModelAttributeHelper(
          sessionHolder,
          mlCommand.getFetchModelAttr.getModelRef.getId,
          Option(mlCommand.getFetchModelAttr.getMethod),
          args)
        val param = Serializer.serializeParam(helper.getAttribute)
        proto.MlCommandResponse
          .newBuilder()
          .setParam(param)
          .build()

      case proto.MlCommand.CommandCase.DELETE_MODEL =>
        val modelId = mlCommand.getDeleteModel.getModelRef.getId
        var result = false
        if (!modelId.contains(".")) {
          mlCache.remove(modelId)
          result = true
        }
        proto.MlCommandResponse
          .newBuilder()
          .setParam(
            proto.Param
              .newBuilder()
              .setLiteral(LiteralValueProtoConverter.toLiteralProto(result))
              .build())
          .build()

      case proto.MlCommand.CommandCase.WRITE =>
        mlCommand.getWrite.getTypeCase match {
          case proto.MlCommand.Writer.TypeCase.MODEL_REF => // save a model
            val modelId = mlCommand.getWrite.getModelRef.getId
            val model = mlCache.get(modelId)
            val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
            MLUtils.setInstanceParams(copiedModel, mlCommand.getWrite.getParams)

            copiedModel match {
              case m: MLWritable =>
                val writer = if (mlCommand.getWrite.getShouldOverwrite) {
                  m.write.overwrite()
                } else {
                  m.write
                }
                val path = mlCommand.getWrite.getPath
                val options = mlCommand.getWrite.getOptionsMap
                options.forEach((k, v) => writer.option(k, v))
                writer.save(path)
              case _ => throw new RuntimeException("Failed to handle model.save")
            }
            proto.MlCommandResponse.newBuilder().build()

          // save an estimator/evaluator/transformer
          case proto.MlCommand.Writer.TypeCase.OPERATOR =>
            throw new RuntimeException("Support it later")
          case _ => throw new RuntimeException("Unsupported operator")
        }

      case proto.MlCommand.CommandCase.READ =>
        val clazz = mlCommand.getRead.getClazz
        val path = mlCommand.getRead.getPath
        val model = loadModel(clazz, path)
        model match {
          case _: Model[_] =>
            val id = mlCache.register(model)
            proto.MlCommandResponse
              .newBuilder()
              .setOperatorInfo(
                proto.MlCommandResponse.MlOperatorInfo
                  .newBuilder()
                  .setModelRef(proto.ModelRef.newBuilder().setId(id))
                  .setUid(model.uid)
                  .setParams(Serializer.serializeParams(model)))
              .build()

          case _ =>
            throw new UnsupportedOperationException(f"Unsupported loading $clazz")
        }

      case _ => throw new UnsupportedOperationException("Unsupported ML command")
    }
  }

  def transformMLRelation(relation: proto.MlRelation, sessionHolder: SessionHolder): DataFrame = {
    relation.getMlTypeCase match {
      // Ml transform
      case proto.MlRelation.MlTypeCase.ML_TRANSFORM =>
        relation.getMlTransform.getOperatorCase match {
          // transform for a new ML transformer
          case proto.MlRelation.Transform.OperatorCase.TRANSFORMER =>
            val transformProto = relation.getMlTransform
            assert(
              transformProto.getTransformer.getType ==
                proto.MlOperator.OperatorType.TRANSFORMER)
            val dataset = MLUtils.parseRelationProto(transformProto.getInput, sessionHolder)
            val transformer = MLUtils.getTransformer(transformProto)
            transformer.transform(dataset)

          // transform on a cached model
          case proto.MlRelation.Transform.OperatorCase.MODEL_REF =>
            val helper =
              ModelAttributeHelper(sessionHolder, relation.getMlTransform.getModelRef.getId, None)
            helper.transform(relation.getMlTransform)

          case _ => throw new IllegalArgumentException("Unsupported ml operator")
        }

      // Get the model attribute
      case proto.MlRelation.MlTypeCase.MODEL_ATTR =>
        val helper = ModelAttributeHelper(
          sessionHolder,
          relation.getModelAttr.getModelRef.getId,
          Option(relation.getModelAttr.getMethod))
        helper.getAttribute.asInstanceOf[DataFrame]

      case _ =>
        throw new IllegalArgumentException("Unsupported ml relation")
    }
  }

}
