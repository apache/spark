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

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{EstimatorUtils, Model}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.tree.TreeConfig
import org.apache.spark.ml.util.{MLWritable, Summary}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.ml.Serializer.deserializeMethodArguments
import org.apache.spark.sql.connect.service.SessionHolder

private case class Method(
    name: String,
    argValues: Array[Object] = Array.empty,
    argClasses: Array[Class[_]] = Array.empty)

/**
 * Helper function to get the attribute from an object by reflection
 */
private class AttributeHelper(
    val sessionHolder: SessionHolder,
    val objRef: String,
    val methods: Array[Method]) {
  protected def instance(): Object = {
    val obj = sessionHolder.mlCache.get(objRef)
    if (obj == null) {
      throw MLCacheInvalidException(s"object $objRef")
    }
    obj
  }
  // Get the attribute by reflection
  def getAttribute: Any = {
    assert(methods.length >= 1)
    methods.foldLeft(instance()) { (obj, m) =>
      if (m.argValues.isEmpty) {
        MLUtils.invokeMethodAllowed(obj, m.name)
      } else {
        MLUtils.invokeMethodAllowed(obj, m.name, m.argValues, m.argClasses)
      }
    }
  }
}

// Model specific attribute helper with transform supported
private class ModelAttributeHelper(
    sessionHolder: SessionHolder,
    objRef: String,
    methods: Array[Method])
    extends AttributeHelper(sessionHolder, objRef, methods) {

  def transform(relation: proto.MlRelation.Transform): DataFrame = {
    // Create a copied model to avoid concurrently modify model params.
    val model = instance().asInstanceOf[Model[_]]
    val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
    MLUtils.setInstanceParams(copiedModel, relation.getParams)
    val inputDF = MLUtils.parseRelationProto(relation.getInput, sessionHolder)
    copiedModel.transform(inputDF)
  }
}

private object AttributeHelper {
  def parseMethods(
      sessionHolder: SessionHolder,
      methodsProto: Array[proto.Fetch.Method] = Array.empty): Array[Method] = {
    methodsProto.map { m =>
      val (argValues, argClasses) =
        deserializeMethodArguments(m.getArgsList.asScala.toArray, sessionHolder).unzip
      Method(m.getMethod, argValues, argClasses)
    }
  }
  def apply(
      sessionHolder: SessionHolder,
      objId: String,
      methodsProto: Array[proto.Fetch.Method] = Array.empty): AttributeHelper = {
    new AttributeHelper(sessionHolder, objId, parseMethods(sessionHolder, methodsProto))
  }
}

private object ModelAttributeHelper {
  def apply(
      sessionHolder: SessionHolder,
      objId: String,
      methodsProto: Array[proto.Fetch.Method] = Array.empty): ModelAttributeHelper = {
    new ModelAttributeHelper(
      sessionHolder,
      objId,
      AttributeHelper.parseMethods(sessionHolder, methodsProto))
  }
}

// MLHandler is a utility to group all ML operations
private[connect] object MLHandler extends Logging {
  def handleMlCommand(
      sessionHolder: SessionHolder,
      mlCommand: proto.MlCommand): proto.MlCommandResult = {

    val mlCache = sessionHolder.mlCache
    val memoryControlEnabled = sessionHolder.mlCache.getMemoryControlEnabled

    if (memoryControlEnabled) {
      val maxModelSize = sessionHolder.mlCache.getModelMaxSize

      // Note: Tree training stops early when the growing tree model exceeds
      //  `TreeConfig.trainingEarlyStopModelSizeThresholdInBytes`, to ensure the final
      // model size is lower than `maxModelSize`, set early-stop threshold to
      // half of `maxModelSize`, because in each tree training iteration, the tree
      // nodes will grow up to 2 times, the additional 0.5 is for buffer
      // because the in-memory size is not exactly in direct proportion to the tree nodes.
      TreeConfig.trainingEarlyStopModelSizeThresholdInBytes = (maxModelSize.toDouble / 2.5).toLong
    }

    mlCommand.getCommandCase match {
      case proto.MlCommand.CommandCase.FIT =>
        val fitCmd = mlCommand.getFit
        val estimatorProto = fitCmd.getEstimator
        assert(estimatorProto.getType == proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)

        val dataset = MLUtils.parseRelationProto(fitCmd.getDataset, sessionHolder)
        val estimator =
          MLUtils.getEstimator(sessionHolder, estimatorProto, Some(fitCmd.getParams))

        if (memoryControlEnabled) {
          try {
            val estimatedModelSize = estimator.estimateModelSize(dataset)
            mlCache.checkModelSize(estimatedModelSize)
          } catch {
            case _: UnsupportedOperationException => ()
          }
          if (estimator.getClass.getName == "org.apache.spark.ml.fpm.FPGrowth") {
            throw MlUnsupportedException(
              "FPGrowth algorithm is not supported " +
                "if Spark Connect model cache offloading is enabled.")
          }
          if (estimator.getClass.getName == "org.apache.spark.ml.clustering.LDA"
            && estimator
              .asInstanceOf[org.apache.spark.ml.clustering.LDA]
              .getOptimizer
              .toLowerCase() == "em") {
            throw MlUnsupportedException(
              "LDA algorithm with 'em' optimizer is not supported " +
                "if Spark Connect model cache offloading is enabled.")
          }
        }

        EstimatorUtils.warningMessagesBuffer.set(new mutable.ArrayBuffer[String]())
        val model = estimator.fit(dataset).asInstanceOf[Model[_]]
        val id = mlCache.register(model)

        val fitWarningMessage = if (EstimatorUtils.warningMessagesBuffer.get().length > 0) {
          EstimatorUtils.warningMessagesBuffer.get().mkString("\n")
        } else { null }
        EstimatorUtils.warningMessagesBuffer.set(null)
        val opInfo = proto.MlCommandResult.MlOperatorInfo
          .newBuilder()
          .setObjRef(proto.ObjectRef.newBuilder().setId(id))
        if (fitWarningMessage != null) {
          opInfo.setWarningMessage(fitWarningMessage)
        }
        proto.MlCommandResult
          .newBuilder()
          .setOperatorInfo(opInfo)
          .build()

      case proto.MlCommand.CommandCase.FETCH =>
        val helper = AttributeHelper(
          sessionHolder,
          mlCommand.getFetch.getObjRef.getId,
          mlCommand.getFetch.getMethodsList.asScala.toArray)
        val attrResult = helper.getAttribute
        attrResult match {
          case s: Summary =>
            val id = mlCache.register(s)
            proto.MlCommandResult.newBuilder().setSummary(id).build()
          case m: Model[_] =>
            val id = mlCache.register(m)
            proto.MlCommandResult
              .newBuilder()
              .setOperatorInfo(
                proto.MlCommandResult.MlOperatorInfo
                  .newBuilder()
                  .setObjRef(proto.ObjectRef.newBuilder().setId(id)))
              .build()
          case a: Array[_] if a.nonEmpty && a.forall(_.isInstanceOf[Model[_]]) =>
            val ids = a.map(m => mlCache.register(m.asInstanceOf[Model[_]]))
            proto.MlCommandResult
              .newBuilder()
              .setOperatorInfo(
                proto.MlCommandResult.MlOperatorInfo
                  .newBuilder()
                  .setObjRef(proto.ObjectRef.newBuilder().setId(ids.mkString(","))))
              .build()
          case _ =>
            val param = Serializer.serializeParam(attrResult)
            proto.MlCommandResult.newBuilder().setParam(param).build()
        }

      case proto.MlCommand.CommandCase.DELETE =>
        val ids = mutable.ArrayBuilder.make[String]
        mlCommand.getDelete.getObjRefsList.asScala.toArray.foreach { objId =>
          if (!objId.getId.contains(".")) {
            if (mlCache.remove(objId.getId)) {
              ids += objId.getId
            }
          }
        }
        proto.MlCommandResult
          .newBuilder()
          .setOperatorInfo(
            proto.MlCommandResult.MlOperatorInfo
              .newBuilder()
              .setObjRef(proto.ObjectRef.newBuilder().setId(ids.result().mkString(","))))
          .build()

      case proto.MlCommand.CommandCase.CLEAN_CACHE =>
        val size = mlCache.clear()
        proto.MlCommandResult
          .newBuilder()
          .setParam(LiteralValueProtoConverter.toLiteralProto(size))
          .build()

      case proto.MlCommand.CommandCase.GET_CACHE_INFO =>
        proto.MlCommandResult
          .newBuilder()
          .setParam(LiteralValueProtoConverter.toLiteralProto(mlCache.getInfo()))
          .build()

      case proto.MlCommand.CommandCase.WRITE =>
        mlCommand.getWrite.getTypeCase match {
          case proto.MlCommand.Write.TypeCase.OBJ_REF => // save a model
            val objId = mlCommand.getWrite.getObjRef.getId
            val model = mlCache.get(objId).asInstanceOf[Model[_]]
            val copiedModel = model.copy(ParamMap.empty).asInstanceOf[Model[_]]
            MLUtils.setInstanceParams(copiedModel, mlCommand.getWrite.getParams)

            copiedModel match {
              case m: MLWritable => MLUtils.write(m, mlCommand.getWrite)
              case other => throw MlUnsupportedException(s"$other is not writable")
            }

          // save an estimator/evaluator/transformer
          case proto.MlCommand.Write.TypeCase.OPERATOR =>
            val writer = mlCommand.getWrite
            val operatorType = writer.getOperator.getType
            val operatorName = writer.getOperator.getName
            val params = Some(writer.getParams)

            operatorType match {
              case proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR =>
                val estimator = MLUtils.getEstimator(sessionHolder, writer.getOperator, params)
                estimator match {
                  case writable: MLWritable => MLUtils.write(writable, mlCommand.getWrite)
                  case other => throw MlUnsupportedException(s"Estimator $other is not writable")
                }

              case proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR =>
                val evaluator = MLUtils.getEvaluator(sessionHolder, writer.getOperator, params)
                evaluator match {
                  case writable: MLWritable => MLUtils.write(writable, mlCommand.getWrite)
                  case other => throw MlUnsupportedException(s"Evaluator $other is not writable")
                }

              case proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER =>
                val transformer =
                  MLUtils.getTransformer(sessionHolder, writer.getOperator, params)
                transformer match {
                  case writable: MLWritable => MLUtils.write(writable, mlCommand.getWrite)
                  case other =>
                    throw MlUnsupportedException(s"Transformer $other is not writable")
                }

              case _ =>
                throw MlUnsupportedException(s"Operator $operatorName is not supported")
            }
          case other => throw MlUnsupportedException(s"$other write not supported")
        }
        proto.MlCommandResult.newBuilder().build()

      case proto.MlCommand.CommandCase.READ =>
        val operator = mlCommand.getRead.getOperator
        val name = operator.getName
        val path = mlCommand.getRead.getPath

        if (operator.getType == proto.MlOperator.OperatorType.OPERATOR_TYPE_MODEL) {
          val model = MLUtils.loadTransformer(sessionHolder, name, path)
          val id = mlCache.register(model)
          return proto.MlCommandResult
            .newBuilder()
            .setOperatorInfo(
              proto.MlCommandResult.MlOperatorInfo
                .newBuilder()
                .setObjRef(proto.ObjectRef.newBuilder().setId(id))
                .setUid(model.uid)
                .setParams(Serializer.serializeParams(model)))
            .build()
        }

        val mlOperator =
          if (operator.getType ==
              proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR) {
            MLUtils.loadEstimator(sessionHolder, name, path).asInstanceOf[Params]
          } else if (operator.getType ==
              proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR) {
            MLUtils.loadEvaluator(sessionHolder, name, path).asInstanceOf[Params]
          } else if (operator.getType ==
              proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER) {
            MLUtils.loadTransformer(sessionHolder, name, path).asInstanceOf[Params]
          } else {
            throw MlUnsupportedException(s"${operator.getType} read not supported")
          }

        proto.MlCommandResult
          .newBuilder()
          .setOperatorInfo(
            proto.MlCommandResult.MlOperatorInfo
              .newBuilder()
              .setName(name)
              .setUid(mlOperator.uid)
              .setParams(Serializer.serializeParams(mlOperator)))
          .build()

      case proto.MlCommand.CommandCase.EVALUATE =>
        val evalCmd = mlCommand.getEvaluate
        val evalProto = evalCmd.getEvaluator
        assert(evalProto.getType == proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR)

        val dataset = MLUtils.parseRelationProto(evalCmd.getDataset, sessionHolder)
        val evaluator =
          MLUtils.getEvaluator(sessionHolder, evalProto, Some(evalCmd.getParams))
        val metric = evaluator.evaluate(dataset)
        proto.MlCommandResult
          .newBuilder()
          .setParam(LiteralValueProtoConverter.toLiteralProto(metric))
          .build()

      case other => throw MlUnsupportedException(s"$other not supported")
    }
  }

  def transformMLRelation(relation: proto.MlRelation, sessionHolder: SessionHolder): DataFrame = {
    relation.getMlTypeCase match {
      // Ml transform
      case proto.MlRelation.MlTypeCase.TRANSFORM =>
        relation.getTransform.getOperatorCase match {
          // transform with a new ML transformer
          case proto.MlRelation.Transform.OperatorCase.TRANSFORMER =>
            val transformProto = relation.getTransform
            assert(
              transformProto.getTransformer.getType ==
                proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER)
            val dataset = MLUtils.parseRelationProto(transformProto.getInput, sessionHolder)
            val transformer = MLUtils.getTransformer(sessionHolder, transformProto)
            transformer.transform(dataset)

          // transform on a cached model
          case proto.MlRelation.Transform.OperatorCase.OBJ_REF =>
            val helper =
              ModelAttributeHelper(
                sessionHolder,
                relation.getTransform.getObjRef.getId,
                Array.empty)
            helper.transform(relation.getTransform)

          case other => throw new IllegalArgumentException(s"$other not supported")
        }

      // Get the attribute from a cached object which could be a model or summary
      case proto.MlRelation.MlTypeCase.FETCH =>
        val helper = AttributeHelper(
          sessionHolder,
          relation.getFetch.getObjRef.getId,
          relation.getFetch.getMethodsList.asScala.toArray)
        helper.getAttribute.asInstanceOf[DataFrame]

      case other => throw MlUnsupportedException(s"$other not supported")
    }
  }
}
