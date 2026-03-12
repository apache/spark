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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Driver, DriverManager}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * java.sql.DriverManager is always loaded by bootstrap classloader,
 * so it can't load JDBC drivers accessible by Spark ClassLoader.
 *
 * To solve the problem, drivers from user-supplied jars are wrapped into thin wrapper.
 */
object DriverRegistry extends Logging {

  /**
   * Load DriverManager first to avoid any race condition between
   * DriverManager static initialization block and specific driver class's
   * static initialization block. e.g. PhoenixDriver
   */
  DriverManager.getDrivers

  private val wrapperMap: mutable.Map[String, DriverWrapper] = mutable.Map.empty

  def register(className: String): Unit = {
    val cls = Utils.getContextOrSparkClassLoader.loadClass(className)
    if (cls.getClassLoader == null) {
      logTrace(s"$className has been loaded with bootstrap ClassLoader, wrapper is not required")
    } else if (wrapperMap.get(className).isDefined) {
      logTrace(s"Wrapper for $className already exists")
    } else {
      synchronized {
        if (wrapperMap.get(className).isEmpty) {
          val wrapper = new DriverWrapper(cls.getConstructor().newInstance().asInstanceOf[Driver])
          DriverManager.registerDriver(wrapper)
          wrapperMap(className) = wrapper
          logTrace(s"Wrapper for $className registered")
        }
      }
    }
  }

  def get(className: String): Driver = {
    DriverManager.getDrivers.asScala.collectFirst {
      case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == className => d.wrapped
      case d if d.getClass.getCanonicalName == className => d
    }.getOrElse {
      throw SparkException.internalError(
        s"Did not find registered driver with class $className")
    }
  }
}

