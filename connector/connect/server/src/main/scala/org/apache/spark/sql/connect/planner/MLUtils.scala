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

import org.apache.spark.ml.param.{Param, Params}

object SparkConnectUtils {

  private def castParamValue(paramType: Class[_], paramValue: Object): Object = {
    if (paramType == classOf[Int]) {
      java.lang.Integer.valueOf(paramValue.asInstanceOf[java.lang.Number].intValue())
    } else if (paramType == classOf[Long]) {
      java.lang.Long.valueOf(paramValue.asInstanceOf[java.lang.Number].longValue())
    } else if (paramType == classOf[Float]) {
      java.lang.Float.valueOf(paramValue.asInstanceOf[java.lang.Number].floatValue())
    } else if (paramType == classOf[Double]) {
      java.lang.Double.valueOf(paramValue.asInstanceOf[java.lang.Number].doubleValue())
    } else if (paramType == classOf[Array[Int]]) {
      paramValue.asInstanceOf[List[Object]]
        .map(castParamValue(classOf[Int], _).asInstanceOf[java.lang.Integer])
        .map(_.intValue()).toArray
    } else if (paramType == classOf[Array[Long]]) {
      paramValue.asInstanceOf[List[Object]]
        .map(castParamValue(classOf[Long], _).asInstanceOf[java.lang.Long])
        .map(_.longValue()).toArray
    } else if (paramType == classOf[Array[Float]]) {
      paramValue.asInstanceOf[List[Object]]
        .map(castParamValue(classOf[Float], _).asInstanceOf[java.lang.Float])
        .map(_.floatValue()).toArray
    } else if (paramType == classOf[Array[Double]]) {
      paramValue.asInstanceOf[List[Object]]
        .map(castParamValue(classOf[Double], _).asInstanceOf[java.lang.Double])
        .map(_.doubleValue()).toArray
    } else if (paramType == classOf[Array[Boolean]]) {
      paramValue.asInstanceOf[List[java.lang.Boolean]].map(_.booleanValue()).toArray
    } else if (paramType == classOf[Array[String]]) {
      paramValue.asInstanceOf[List[String]].toArray
    } else {
      throw new RuntimeException()
    }
  }

  private def makeParamPair(
                             instance: Params, paramName: String, paramValue: Object
                           ): (Param[Any], Object) = {
    val param = instance.getParam(paramName)
    val castedValue = castParamValue(param.paramValueClassTag.runtimeClass, paramValue)
    (param, castedValue)
  }

  def setInstanceParams(
                         instance: Params,
                         paramMap: Map[String, Object],
                         defaultParamMap: Map[String, Object]
                       ): Unit = {
    paramMap.foreach { case (paramName, paramValue) =>
      val (param, castedValue) = makeParamPair(instance, paramName, paramValue)
      instance.set(param, castedValue)
    }
    defaultParamMap.foreach { case (paramName, paramValue) =>
      val (param, castedValue) = makeParamPair(instance, paramName, paramValue)
      instance._setDefault(param, castedValue)
    }
  }
}
