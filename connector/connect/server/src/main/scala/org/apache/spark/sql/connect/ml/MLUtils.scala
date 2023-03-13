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

import org.apache.spark.connect.proto
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.planner.{LiteralValueProtoConverter, SparkConnectPlanner}
import org.apache.spark.sql.connect.service.SessionHolder

object MLUtils {

  def setInstanceParams(instance: Params, paramsProto: proto.MlParams): Unit = {
    import scala.collection.JavaConverters._
    paramsProto.getParamsMap.asScala.foreach { case (paramName, paramValueProto) =>
      val paramDef = instance.getParam(paramName)
      val paramValue = parseParamValue(paramDef.paramValueClassTag.runtimeClass, paramValueProto)
      instance.set(paramDef, paramValue)
    }
    paramsProto.getDefaultParamsMap.asScala.foreach { case (paramName, paramValueProto) =>
      val paramDef = instance.getParam(paramName)
      val paramValue = parseParamValue(paramDef.paramValueClassTag.runtimeClass, paramValueProto)
      instance._setDefault(paramDef -> paramValue)
    }
  }

  def parseParamValue(paramType: Class[_], paramValueProto: proto.Expression.Literal): Any = {
    val value = LiteralValueProtoConverter.toCatalystValue(paramValueProto)
    _convertParamValue(paramType, value)
  }

  def _convertParamValue(paramType: Class[_], value: Any): Any = {
    // Some cases the param type might be mismatched with the value type.
    // Because in python side we only have int / float type for numeric params.
    // e.g.:
    // param type is Int but client sends a Long type.
    // param type is Long but client sends a Int type.
    // param type is Float but client sends a Double type.
    // param type is Array[Int] but client sends a Array[Long] type.
    // param type is Array[Float] but client sends a Array[Double] type.
    // param type is Array[Array[Int]] but client sends a Array[Array[Long]] type.
    // param type is Array[Array[Float]] but client sends a Array[Array[Double]] type.
    if (paramType == classOf[Byte]) {
      value.asInstanceOf[java.lang.Number].byteValue()
    } else if (paramType == classOf[Short]) {
      value.asInstanceOf[java.lang.Number].shortValue()
    } else if (paramType == classOf[Int]) {
      value.asInstanceOf[java.lang.Number].intValue()
    } else if (paramType == classOf[Long]) {
      value.asInstanceOf[java.lang.Number].longValue()
    } else if (paramType == classOf[Float]) {
      value.asInstanceOf[java.lang.Number].floatValue()
    } else if (paramType == classOf[Double]) {
      value.asInstanceOf[java.lang.Number].doubleValue()
    } else if (paramType.isArray) {
      val compType = paramType.getComponentType
      value.asInstanceOf[Array[_]].map { e =>
        _convertParamValue(compType, e)
      }
    } else {
      value
    }
  }

  def convertInstanceParamsToProto(instance: Params): proto.MlParams = {
    val builder = proto.MlParams.newBuilder()
    instance.params.foreach { param =>
      val name = param.name
      val valueOpt = instance.get(param)
      val defaultValueOpt = instance.getDefault(param)

      if (valueOpt.isDefined) {
        val valueProto = LiteralValueProtoConverter.toConnectProtoValue(valueOpt.get)
        builder.putParams(name, valueProto)
      }
      if (defaultValueOpt.isDefined) {
        val defaultValueProto = LiteralValueProtoConverter.toConnectProtoValue(defaultValueOpt.get)
        builder.putDefaultParams(name, defaultValueProto)
      }
    }
    builder.build()
  }

  def parseRelationProto(relationProto: proto.Relation, sessionHolder: SessionHolder): DataFrame = {
    val relationalPlanner = new SparkConnectPlanner(sessionHolder)
    val plan = relationalPlanner.transformRelation(relationProto)
    Dataset.ofRows(sessionHolder.session, plan)
  }
}
