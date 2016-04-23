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

package org.apache.spark.sql.execution.datasources

private[datasources] object ParameterUtils {
  def getNullSafeChar(
      parameters: Map[String, String],
      paramName: String,
      default: Char): Char = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  def getNullSafeInt(
      parameters: Map[String, String],
      paramName: String,
      default: Int): Int = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toInt
      } catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }

  def getNullSafeBool(
      parameters: Map[String, String],
      paramName: String,
      default: Boolean): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase == "true") {
      true
    } else if (param.toLowerCase == "false") {
      false
    } else {
      throw new RuntimeException(s"$paramName flag can be true or false")
    }
  }

  def getNullSafeString(
      parameters: Map[String, String],
      paramName: String,
      default: String): String = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => value
    }
  }

  def getNullSafeDouble(
      parameters: Map[String, String],
      paramName: String,
      default: Double): Double = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toDouble
      } catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }
}
