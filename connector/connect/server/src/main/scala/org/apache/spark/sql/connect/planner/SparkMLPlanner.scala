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
import java.lang
import scala.reflect.runtime.universe

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, ExecutePlanResponse, RemoteCall}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.service.{SessionHolder}
import org.apache.spark.util.Utils

object SparkMLPlanner {

  val runtimeMirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)

  def buildRemoteCallResponse(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse],
      returnValueOpt: Option[RemoteCall.ArgValue]
  ): Unit = {
    val response = proto.ExecutePlanResponse.newBuilder
      .setClientId(request.getClientId)

    returnValueOpt.foreach(response.setRemoteCallReturnValue(_))

    responseObserver.onNext(response.build())
    responseObserver.onCompleted()
  }

  def loadInputRelation(sessionHolder: SessionHolder, inputRelation: proto.Relation): DataFrame = {
    val relationalPlanner = new SparkConnectPlanner(sessionHolder)
    val plan = relationalPlanner.transformRelation(inputRelation)
    Dataset.ofRows(sessionHolder.session, plan)
  }

  /*
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

  def handleRemoteCall(
       sessionHolder: SessionHolder,
       request: ExecutePlanRequest,
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
  */

  def parseArg(protoArg: proto.RemoteCall.ArgValue, sessionHolder: SessionHolder): Object = {
    import scala.collection.JavaConverters._
    import proto.RemoteCall.ArgValue
    protoArg.getArgTypeCase match {
      case ArgValue.ArgTypeCase.INT32_VALUE =>
        lang.Integer.valueOf(protoArg.getInt32Value)
      case ArgValue.ArgTypeCase.INT64_VALUE =>
        lang.Long.valueOf(protoArg.getInt64Value)
      case ArgValue.ArgTypeCase.FLOAT_VALUE =>
        lang.Float.valueOf(protoArg.getFloatValue)
      case ArgValue.ArgTypeCase.DOUBLE_VALUE =>
        lang.Double.valueOf(protoArg.getDoubleValue)
      case ArgValue.ArgTypeCase.STRING_VALUE =>
        protoArg.getStringValue
      case ArgValue.ArgTypeCase.BOOL_VALUE =>
        lang.Boolean.valueOf(protoArg.getBoolValue)
      case ArgValue.ArgTypeCase.RELATION =>
        loadInputRelation(sessionHolder, protoArg.getRelation)
      case ArgValue.ArgTypeCase.REMOTE_OBJECT =>
        sessionHolder.serverSideObjectManager.getObject(protoArg.getRemoteObject.getId)
      case ArgValue.ArgTypeCase.LIST =>
        val protoValues = protoArg.getList.getElementList.asScala.toList
        protoValues.map { x => parseArg(x, sessionHolder) }
      case ArgValue.ArgTypeCase.MAP =>
        val protoMap = protoArg.getMap.getMapMap().asScala.toMap
        protoMap.mapValues(x => parseArg(x, sessionHolder))
      case _ =>
        throw InvalidPlanInput()
    }
  }

  /**
   * Parse prototype message of argument list, returns a tuple of (values, types)
   */
  def parseArgs(
      protoArgList: List[proto.RemoteCall.ArgValue],
      sessionHolder: SessionHolder
  ): Array[Object] = {
    val argValues = new Array[Object](protoArgList.size)

    for (i <- 0 until protoArgList.size) {
      val protoValue = protoArgList(i)
      argValues(i) = parseArg(protoValue, sessionHolder)
    }
    argValues
  }

  def serializeValue(value: Object, sessionHolder: SessionHolder): proto.RemoteCall.ArgValue = {
    val protoBuilder = proto.RemoteCall.ArgValue.newBuilder()
    value match {
      case v: lang.Integer =>
        protoBuilder.setInt32Value(v.intValue())
      case v: lang.Long =>
        protoBuilder.setInt64Value(v.longValue())
      case v: lang.Float =>
        protoBuilder.setFloatValue(v.floatValue())
      case v: lang.Double =>
        protoBuilder.setDoubleValue(v.doubleValue())
      case v: String =>
        protoBuilder.setStringValue(v)
      case v: lang.Boolean =>
        protoBuilder.setBoolValue(v)
      case v: List[_] =>
        // TODO
        throw new UnsupportedOperationException()
      case v: Map[_, _] =>
        // TODO
        throw new UnsupportedOperationException()
      case v: Object =>
        val instanceId = sessionHolder.serverSideObjectManager.registerObject(v)
        protoBuilder.setRemoteObject(
          RemoteCall.RemoteObject.newBuilder()
            .setId(instanceId)
            .build()
        )
    }
    protoBuilder.build()
  }

  def invokeMethod(
        instance: Object,
        methodName: String,
        argsProto: List[RemoteCall.ArgValue],
        sessionHolder: SessionHolder): Option[RemoteCall.ArgValue] = {
    val argValues = parseArgs(argsProto, sessionHolder)
    val method = instance.getClass.getMethods().find { method =>
      method.getName == methodName &&
        method.getParameterTypes().zip(argValues).forall {
          case (clazz, instance) => clazz.isInstance(instance)
        }
    }.get
    val result = method.invoke(instance, argValues: _*)

    if (result != null) {
      Some(serializeValue(result, sessionHolder))
    } else None
  }

  def handleRemoteCall(
      sessionHolder: SessionHolder,
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]
  ): Unit = {
    import scala.collection.JavaConverters._
    val remoteCallProto = request.getPlan.getRemoteCall

    remoteCallProto.getCallTypeCase match {
      case proto.RemoteCall.CallTypeCase.CONSTRUCT_OBJECT =>
        val className = remoteCallProto.getConstructObject.getClassName
        val argsProto = remoteCallProto.getConstructObject.getArgValuesList.asScala.toList
        val clazz = Utils.classForName(className)
        val argValues = parseArgs(argsProto, sessionHolder)
        // TODO: primitive type checking.
        val ctor = clazz.getConstructors().find { ctor =>
          ctor.getParameterTypes().zip(argValues).forall {
            case (clazz, instance) => clazz.isInstance(instance)
          }
        }.get
        val instance = ctor.newInstance(argValues: _*).asInstanceOf[Object]
        val serializedReturnValue = serializeValue(instance, sessionHolder)

        buildRemoteCallResponse(request, responseObserver, Some(serializedReturnValue))

      case proto.RemoteCall.CallTypeCase.DESTRUCT_OBJECT =>
        val objectId = remoteCallProto.getDestructObject.getRemoteObject.getId
        sessionHolder.serverSideObjectManager.removeObject(objectId)

        buildRemoteCallResponse(request, responseObserver, None)

      case proto.RemoteCall.CallTypeCase.CALL_METHOD =>
        val objectId = remoteCallProto.getCallMethod.getRemoteObject.getId
        val argsProto = remoteCallProto.getCallMethod.getArgValuesList.asScala.toList
        val methodName = remoteCallProto.getCallMethod.getMethodName
        val instance = sessionHolder.serverSideObjectManager.getObject(objectId)

        val serializedReturnValueOpt = invokeMethod(instance, methodName, argsProto, sessionHolder)

        buildRemoteCallResponse(request, responseObserver, serializedReturnValueOpt)

      case proto.RemoteCall.CallTypeCase.CALL_FUNCTION =>
        val moduleName = remoteCallProto.getCallFunction.getModuleName
        val module = runtimeMirror.staticModule(moduleName)
        val objectInstance = runtimeMirror.reflectModule(module).instance.asInstanceOf[Object]
        val argsProto = remoteCallProto.getCallFunction.getArgValuesList.asScala.toList
        val functionName = remoteCallProto.getCallFunction.getFunctionName

        val serializedReturnValueOpt = invokeMethod(
          objectInstance, functionName, argsProto, sessionHolder
        )

        buildRemoteCallResponse(request, responseObserver, serializedReturnValueOpt)

      case _ => throw InvalidPlanInput()
    }
  }
}
