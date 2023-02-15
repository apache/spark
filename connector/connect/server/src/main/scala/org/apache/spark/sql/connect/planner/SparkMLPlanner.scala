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
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.service.SessionHolder
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
      case v: Vector =>
        val protoVectorBuilder = RemoteCall.Vector.newBuilder()
        val data = v.toArray
        for (i <- 0 until data.length) {
          protoVectorBuilder.setElement(i, data(i))
        }
        protoBuilder.setVector(protoVectorBuilder.build())
      case v: Matrix =>
        val protoMatrixBuilder = RemoteCall.Matrix.newBuilder()
        protoMatrixBuilder.setNumRows(v.numRows)
        protoMatrixBuilder.setNumCols(v.numCols)
        val data = v.toArray
        for (i <- 0 until data.length) {
          protoMatrixBuilder.setElement(i, data(i))
        }
        protoBuilder.setMatrix(protoMatrixBuilder.build())
      case v: Object =>
        val instanceId = sessionHolder.serverSideObjectManager.registerObject(v)
        val className = v.getClass.getName
        protoBuilder.setRemoteObject(
          RemoteCall.RemoteObject.newBuilder()
            .setId(instanceId)
            .setClassName(className)
            .build()
        )
    }
    protoBuilder.build()
  }

  def _checkArgTypeMatch(argType: Class[_], argValue: Object): Boolean = {
    argValue match {
      case _: lang.Integer | _: lang.Long => argType == classOf[Int] || argType == classOf[Long]
      case _: lang.Float | _: lang.Double => argType == classOf[Float] || argType == classOf[Double]
      case _: lang.Boolean => argType == classOf[Boolean]
      case _: Object => argType.isInstance(argValue)
    }
  }

  def _castArgValue(argType: Class[_], argValue: Object): Object = {
    if (argType == classOf[Int]) {
      lang.Integer.valueOf(argValue.asInstanceOf[lang.Number].intValue())
    } else if (argType == classOf[Long]) {
      lang.Long.valueOf(argValue.asInstanceOf[lang.Number].longValue())
    } else if (argType == classOf[Float]) {
      lang.Float.valueOf(argValue.asInstanceOf[lang.Number].floatValue())
    } else if (argType == classOf[Double]) {
      lang.Double.valueOf(argValue.asInstanceOf[lang.Number].doubleValue())
    } else {
      argValue
    }
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
          case (clazz, argValue) => _checkArgTypeMatch(clazz, argValue)
        }
    }.get
    val castedArgValues = method.getParameterTypes().zip(argValues).map {
      case (clazz, argValue) => _castArgValue(clazz, argValue)
    }
    val result = method.invoke(instance, castedArgValues: _*)

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
            case (clazz, argValue) => _checkArgTypeMatch(clazz, argValue)
          }
        }.get
        val castedArgValues = ctor.getParameterTypes().zip(argValues).map {
          case (clazz, argValue) => _castArgValue(clazz, argValue)
        }
        val instance = ctor.newInstance(castedArgValues: _*).asInstanceOf[Object]
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
