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
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
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
      args: Array[proto.FetchAttr.Args] = Array.empty): ModelAttributeHelper = {
    val tmp = deserializeMethodArguments(args, sessionHolder)
    val argValues = tmp.map(_._1)
    val argClasses = tmp.map(_._2)
    new ModelAttributeHelper(sessionHolder, modelId, method, argValues, argClasses)
  }
}

// MLHandler is a utility to group all ML operations
object MLHandler extends Logging {
  def handleMlCommand(
      sessionHolder: SessionHolder,
      mlCommand: proto.MlCommand): proto.MlCommandResult = {

    val mlCache = sessionHolder.mlCache

    mlCommand.getCommandCase match {
      case proto.MlCommand.CommandCase.FIT =>
        val fitCmd = mlCommand.getFit
        val estimatorProto = fitCmd.getEstimator
        assert(estimatorProto.getType == proto.MlOperator.OperatorType.ESTIMATOR)

        val dataset = MLUtils.parseRelationProto(fitCmd.getDataset, sessionHolder)
        val estimator = MLUtils.getEstimator(estimatorProto, Some(fitCmd.getParams))
        val model = estimator.fit(dataset).asInstanceOf[Model[_]]
        val id = mlCache.register(model)
        proto.MlCommandResult
          .newBuilder()
          .setOperatorInfo(
            proto.MlCommandResult.MlOperatorInfo
              .newBuilder()
              .setObjRef(proto.ObjectRef.newBuilder().setId(id)))
          .build()

      case proto.MlCommand.CommandCase.FETCH_ATTR =>
        val args = mlCommand.getFetchAttr.getArgsList.asScala.toArray
        val helper = ModelAttributeHelper(
          sessionHolder,
          mlCommand.getFetchAttr.getObjRef.getId,
          Option(mlCommand.getFetchAttr.getMethod),
          args)
        val param = Serializer.serializeParam(helper.getAttribute)
        proto.MlCommandResult
          .newBuilder()
          .setParam(param)
          .build()

      case proto.MlCommand.CommandCase.DELETE =>
        val modelId = mlCommand.getDelete.getObjRef.getId
        var result = false
        if (!modelId.contains(".")) {
          mlCache.remove(modelId)
          result = true
        }
        proto.MlCommandResult
          .newBuilder()
          .setParam(
            proto.Param
              .newBuilder()
              .setLiteral(LiteralValueProtoConverter.toLiteralProto(result))
              .build())
          .build()

      case proto.MlCommand.CommandCase.WRITE =>
        mlCommand.getWrite.getTypeCase match {
          case proto.MlCommand.Writer.TypeCase.OBJ_REF => // save a model
            val modelId = mlCommand.getWrite.getObjRef.getId
            val model = mlCache.get(modelId)
            val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
            MLUtils.setInstanceParams(copiedModel, mlCommand.getWrite.getParams)

            copiedModel match {
              case m: MLWritable => MLUtils.write(m, mlCommand.getWrite)
              case _ => throw new RuntimeException("Failed to handle model.save")
            }

          // save an estimator/evaluator/transformer
          case proto.MlCommand.Writer.TypeCase.OPERATOR =>
            val writer = mlCommand.getWrite
            if (writer.getOperator.getType == proto.MlOperator.OperatorType.ESTIMATOR) {
              val estimator = MLUtils.getEstimator(writer.getOperator, Some(writer.getParams))
              estimator match {
                case m: MLWritable => MLUtils.write(m, mlCommand.getWrite)
                case _ => throw new RuntimeException("Failed to handle Estimator.save")
              }
            } else {
              throw new RuntimeException(
                s"Unsupported writting for ${writer.getOperator.getName}")
            }

          case _ => throw new RuntimeException("Unsupported operator")
        }
        proto.MlCommandResult.newBuilder().build()

      case proto.MlCommand.CommandCase.READ =>
        val operator = mlCommand.getRead.getOperator
        val name = operator.getName
        val path = mlCommand.getRead.getPath

        if (operator.getType == proto.MlOperator.OperatorType.MODEL) {
          val model = MLUtils.load(name, path).asInstanceOf[Model[_]]
          val id = mlCache.register(model)
          proto.MlCommandResult
            .newBuilder()
            .setOperatorInfo(
              proto.MlCommandResult.MlOperatorInfo
                .newBuilder()
                .setObjRef(proto.ObjectRef.newBuilder().setId(id))
                .setUid(model.uid)
                .setParams(Serializer.serializeParams(model)))
            .build()

        } else if (operator.getType == proto.MlOperator.OperatorType.ESTIMATOR) {
          val estimator = MLUtils.load(name, path).asInstanceOf[Estimator[_]]
          proto.MlCommandResult
            .newBuilder()
            .setOperatorInfo(
              proto.MlCommandResult.MlOperatorInfo
                .newBuilder()
                .setName(name)
                .setUid(estimator.uid)
                .setParams(Serializer.serializeParams(estimator)))
            .build()
        } else {
          throw new UnsupportedOperationException(s"Unsupported reading for ${name}")
        }

      case _ => throw new UnsupportedOperationException("Unsupported ML command")
    }
  }

  def transformMLRelation(relation: proto.MlRelation, sessionHolder: SessionHolder): DataFrame = {
    relation.getMlTypeCase match {
      // Ml transform
      case proto.MlRelation.MlTypeCase.TRANSFORM =>
        relation.getTransform.getOperatorCase match {
          // transform for a new ML transformer
          case proto.MlRelation.Transform.OperatorCase.TRANSFORMER =>
            val transformProto = relation.getTransform
            assert(
              transformProto.getTransformer.getType ==
                proto.MlOperator.OperatorType.TRANSFORMER)
            val dataset = MLUtils.parseRelationProto(transformProto.getInput, sessionHolder)
            val transformer = MLUtils.getTransformer(transformProto)
            transformer.transform(dataset)

          // transform on a cached model
          case proto.MlRelation.Transform.OperatorCase.OBJ_REF =>
            val helper =
              ModelAttributeHelper(sessionHolder, relation.getTransform.getObjRef.getId, None)
            helper.transform(relation.getTransform)

          case _ => throw new IllegalArgumentException("Unsupported ml operator")
        }

      // Get the model attribute
      case proto.MlRelation.MlTypeCase.FETCH_ATTR =>
        val helper = ModelAttributeHelper(
          sessionHolder,
          relation.getFetchAttr.getObjRef.getId,
          Option(relation.getFetchAttr.getMethod))
        helper.getAttribute.asInstanceOf[DataFrame]

      case _ =>
        throw new IllegalArgumentException("Unsupported ml relation")
    }
  }

}
