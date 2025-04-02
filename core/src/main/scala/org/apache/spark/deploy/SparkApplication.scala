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

package org.apache.spark.deploy

import java.lang.reflect.Modifier

import org.apache.spark.SparkConf

/**
 * Entry point for a Spark application. Implementations must provide a no-argument constructor.
 */
private[spark] trait SparkApplication {

  def start(args: Array[String], conf: SparkConf): Unit

}

/**
 * Implementation of SparkApplication that wraps a standard Java class with a "main" method.
 *
 * Configuration is propagated to the application via system properties, so running multiple
 * of these in the same JVM may lead to undefined behavior due to configuration leaks.
 */
private[deploy] class JavaMainApplication(klass: Class[_]) extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    val sysProps = conf.getAll.toMap
    sysProps.foreach { case (k, v) =>
      sys.props(k) = v
    }

    mainMethod.invoke(null, args)
  }

}
