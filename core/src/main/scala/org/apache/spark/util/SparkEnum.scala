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
package org.apache.spark.util

import org.apache.spark.SparkException

/**
 * Util for the "enum" pattern we have adopted
 */
private[spark] trait SparkEnum {
  override def toString: String = {
    val simpleName = getClass().getSimpleName()
    val a = simpleName.indexOf('$')
    simpleName.substring(0,a)
  }

}

/**
 * Util for the "enum" pattern we've adopted.  It adds methods to parse the enum from a String.
 * Note that you must still manually keep {{values}} in sync with the values you create.
 */
private[spark] trait SparkEnumCompanion[T <: SparkEnum] {
  val values: Seq[T]

  lazy val enumNames: Map[String, T] = {
    try {
      val tmpMap = values.map { t =>
        t.toString -> t
      }.toMap
      if (tmpMap.size != values.size) {
        throw new SparkException("It appears you have multiple constants with the same" +
          " name.  Perhaps your naming scheme is incompatible with SparkEnum. found names: " +
          tmpMap.keys)
      }
      tmpMap
    } catch {
      case ie: InternalError =>
        throw new SparkException("It appears you are using SparkEnum in a class which does not " +
          "follow the naming conventions")
    }
  }


  def parse(s:String): Option[T] = {
    enumNames.get(s)
  }

  def parseIgnoreCase(s: String): Option[T] = {
    enumNames.find { case (k, v) =>
      k.toLowerCase() == s.toLowerCase()
    }.map{_._2}
  }

}
