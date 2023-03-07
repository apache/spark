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

import scala.reflect.ClassTag

import org.apache.spark.connect.proto
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder

object MLUtils {

  def setInstanceParams(instance: Params, paramsProto: proto.Params): Unit = {
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
    if (paramType == classOf[Int]) {
      assert (paramValueProto.hasInteger || paramValueProto.hasLong)
      if (paramValueProto.hasInteger) {
        paramValueProto.getInteger
      } else {
        paramValueProto.getLong
      }
    } else if (paramType == classOf[Long]) {
      assert(paramValueProto.hasLong)
      paramValueProto.getLong
    } else if (paramType == classOf[Float]) {
      assert (paramValueProto.hasFloat || paramValueProto.hasDouble)
      if (paramValueProto.hasFloat) {
        paramValueProto.getFloat
      } else {
        paramValueProto.getDouble
      }
    } else if (paramType == classOf[Double]) {
      assert (paramValueProto.hasDouble)
      paramValueProto.getDouble
    } else if (paramType == classOf[Boolean]) {
      assert (paramValueProto.hasBoolean)
      paramValueProto.getBoolean
    } else if (paramType == classOf[String]) {
      assert (paramValueProto.hasString)
      paramValueProto.getString
    } else if (paramType.isArray) {
      assert (paramValueProto.hasArray)
      val listProto = paramValueProto.getArray
      val compType = paramType.getComponentType

      def protoListToArray[T: ClassTag](listProto: proto.Expression.Literal.Array): Unit = {
        Array.tabulate[T](listProto.getElementCount) { index =>
          parseParamValue(
            implicitly[ClassTag[T]].runtimeClass,
            listProto.getElement(index)).asInstanceOf[T]
        }
      }

      if (compType == classOf[Int]) {
        protoListToArray[Int](listProto)
      } else if (compType == classOf[Long]) {
        protoListToArray[Long](listProto)
      } else if (compType == classOf[Float]) {
        protoListToArray[Float](listProto)
      } else if (compType == classOf[Double]) {
        protoListToArray[Double](listProto)
      } else if (compType == classOf[Boolean]) {
        protoListToArray[Boolean](listProto)
      } else if (compType == classOf[String]) {
        protoListToArray[String](listProto)
      } else if (compType.isArray) {
        Array.tabulate(listProto.getElementCount) { index =>
          compType.cast(parseParamValue(compType, listProto.getElement(index)))
        }
      } else {
        throw new IllegalArgumentException()
      }
    }
  }

  def copyInstance(instance: Params): Params = instance.copy(ParamMap.empty)

  def parseRelationProto(relationProto: proto.Relation, sessionHolder: SessionHolder): DataFrame = {
    val relationalPlanner = new SparkConnectPlanner(sessionHolder)
    val plan = relationalPlanner.transformRelation(relationProto)
    Dataset.ofRows(sessionHolder.session, plan)
  }
}
