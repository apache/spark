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

package org.apache.spark.sql.connect.planner

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, ExecutePlanResponse, MlCommand}
import org.apache.spark.ml.{Estimator, PipelineStage, Transformer}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.service.ServerSideObjectManager
import org.apache.spark.util.Utils

object SparkMLPlanner {

  def createStage(uid: String, className: String): PipelineStage = {
    val clazz = Utils.classForName(className)
    val ctorOpt = clazz.getConstructors.find { ctor =>
      // Find the constructor with signature `this(uid: String)`
      ctor.getParameterCount == 1 && ctor.getParameterTypes()(0).eq(classOf[String])
    }
    if (ctorOpt.isEmpty) {
      throw new RuntimeException(
        s"Could not find 'this(uid: String)' constructor for class $className"
      )
    }

    ctorOpt.get.newInstance(uid).asInstanceOf[PipelineStage]
  }

  def buildMlResponse(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse],
      mlCommandResponseOpt: Option[ExecutePlanResponse.MlCommandResponse]
  ): Unit = {
    val response = proto.ExecutePlanResponse.newBuilder
      .setClientId(request.getClientId)

    mlCommandResponseOpt.foreach(response.setMlCommandResponse(_))

    responseObserver.onNext(response.build())
    responseObserver.onCompleted()
  }

  def loadInputRelation(sparkSession: SparkSession, inputRelation: proto.Relation): DataFrame = {
    val relationalPlanner = new SparkConnectPlanner(sparkSession)
    val plan = relationalPlanner.transformRelation(inputRelation)
    Dataset.ofRows(sparkSession, plan)
  }

  def extractParamPair(
     instance: Params,
     paramName: String,
     protoValue: MlCommand.ParamValue
  ): (Param[Any], Any) = {
    import MlCommand.ParamValue.ValueCase

    val param = instance.getParam(paramName)
    val valueType = param.paramValueClassTag.runtimeClass
    val extractedValue = protoValue.getValueCase match {
      case ValueCase.INT_VAL =>
        assert(valueType == classOf[Int])
        protoValue.getIntVal

      case ValueCase.LONG_VAL =>
        if (valueType == classOf[Long]) {
          protoValue.getLongVal
        } else if (valueType == classOf[Int]) {
          protoValue.getLongVal.toInt
        } else {
          throw new java.lang.AssertionError()
        }

      case ValueCase.FLOAT_VAL =>
        assert(valueType == classOf[Float])
        protoValue.getFloatVal

      case ValueCase.DOUBLE_VAL =>
        if (valueType == classOf[Double]) {
          protoValue.getDoubleVal
        } else if (valueType == classOf[Float]) {
          protoValue.getDoubleVal.toFloat
        } else {
          throw new java.lang.AssertionError()
        }

      case ValueCase.BOOL_VAL =>
        assert(valueType == classOf[Boolean])
        protoValue.getBoolVal

      case ValueCase.STR_VAL =>
        assert(valueType == classOf[String])
        protoValue.getStrVal

      case ValueCase.INT_ARRAY =>
        assert(valueType == classOf[Array[Int]])
        protoValue.getIntArray.getElementList.stream().mapToInt(_.toInt).toArray

      case ValueCase.LONG_ARRAY =>
        val elemList = protoValue.getLongArray.getElementList
        if (valueType == classOf[Array[Long]]) {
          elemList.stream().mapToLong(_.toLong).toArray
        } else if (valueType == classOf[Array[Int]]) {
          elemList.stream().mapToInt(_.toInt).toArray
        } else {
          throw new java.lang.AssertionError()
        }

      case ValueCase.FLOAT_ARRAY =>
        assert(valueType == classOf[Float])
        val floatList = protoValue.getFloatArray.getElementList
        val floatArray = new Array[Float](floatList.size())
        for (i <- 0 until floatList.size()) {
          floatArray(i) = floatList.get(i)
        }
        floatArray

      case ValueCase.DOUBLE_ARRAY =>
        val doubleList = protoValue.getDoubleArray.getElementList
        if (valueType == classOf[Array[Double]]) {
          doubleList.stream().mapToDouble(_.toDouble).toArray
        } else if (valueType == classOf[Array[Float]]) {
          val floatArray = new Array[Float](doubleList.size())
          for (i <- 0 until doubleList.size()) {
            floatArray(i) = doubleList.get(i).toFloat
          }
          floatArray
        } else {
          throw new java.lang.AssertionError()
        }

      case ValueCase.STR_ARRAY =>
        assert(valueType == classOf[Array[String]])
        protoValue.getStrArray.getElementList.toArray(Array[String]())

      case ValueCase.BOOL_ARRAY =>
        assert(valueType == classOf[Array[Boolean]])
        val boolList = protoValue.getBoolArray.getElementList
        val boolArray = new Array[Boolean](boolList.size())
        for (i <- 0 until boolList.size()) {
          boolArray(i) = boolList.get(i)
        }
        boolArray

      case _ =>
        throw InvalidPlanInput()
    }
    (param, extractedValue)
  }

  def setParams(instance: Params, protoParams: MlCommand.Params): Unit = {
    protoParams.getParamsMap.forEach { (paramName, protoValue) =>
      val (param, paramValue) = extractParamPair(
        instance, paramName, protoValue
      )
      instance.set(param, paramValue)
    }
    protoParams.getDefaultParamsMap.forEach { (paramName, protoValue) =>
      val (param, paramValue) = extractParamPair(
        instance, paramName, protoValue
      )
      instance._setDefault(param, paramValue)
    }
  }

  def handleMlCommand(
       session: SparkSession,
       request: ExecutePlanRequest,
       serverSideObjectManager: ServerSideObjectManager,
       responseObserver: StreamObserver[ExecutePlanResponse]
  ): Unit = {
    val mlCommand = request.getPlan.getMlCommand
    mlCommand.getOpTypeCase match {
      case proto.MlCommand.OpTypeCase.CONSTRUCT_STAGE =>
        val constructStage = mlCommand.getConstructStage
        val stage = createStage(
          constructStage.getUid,
          constructStage.getClassName
        )
        val objectId = serverSideObjectManager.registerObject(stage)

        val resp = ExecutePlanResponse.MlCommandResponse
          .newBuilder.setServerSideObjectId(objectId).build()

        buildMlResponse(request, responseObserver, Some(resp))

      case proto.MlCommand.OpTypeCase.DESTRUCT_OBJECT =>
        serverSideObjectManager.removeObject(mlCommand.getDestructObject.getId)

      case proto.MlCommand.OpTypeCase.FIT =>
        val estimator = serverSideObjectManager
          .getObject(mlCommand.getFit.getId).asInstanceOf[Estimator[_]]
        val inputDF = loadInputRelation(session, mlCommand.getFit.getInput)
        val model = estimator.fit(inputDF)
        val modelObjId = serverSideObjectManager.
          registerObject(model.asInstanceOf[Object])

        val resp = ExecutePlanResponse.MlCommandResponse
          .newBuilder.setServerSideObjectId(modelObjId).build()

        buildMlResponse(request, responseObserver, Some(resp))

      case proto.MlCommand.OpTypeCase.TRANSFORM =>
        val transformer = serverSideObjectManager.getObject(mlCommand.getTransform.getId)
          .asInstanceOf[Transformer]
        val inputDF = loadInputRelation(session, mlCommand.getTransform.getInput)
        val transformedDF = transformer.transform(inputDF)
        val transformedDFId = serverSideObjectManager.registerObject(transformedDF)

        val resp = ExecutePlanResponse.MlCommandResponse
          .newBuilder.setServerSideObjectId(transformedDFId).build()

        buildMlResponse(request, responseObserver, Some(resp))

      case proto.MlCommand.OpTypeCase.TRANSFER_PARAMS_TO_SERVER =>
        val instance = serverSideObjectManager.getObject(mlCommand.getTransferParamsToServer.getId)
          .asInstanceOf[Params]
        val protoParams = mlCommand.getTransferParamsToServer.getParams
        setParams(instance, protoParams)

        buildMlResponse(request, responseObserver, None)

      case _ =>
        throw new UnsupportedOperationException(s"${mlCommand.getOpTypeCase} not supported.")
    }
  }
}
