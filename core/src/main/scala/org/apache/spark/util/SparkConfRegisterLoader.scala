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

import java.net.URL

import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.runtime.{universe => ru}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

object SparkConfRegisterLoader extends Logging {

  private val CONFIG_OBJECTS_FILE = "spark-conf-registers"

  private def loadObjectInstance(className: String, loader: ClassLoader): Any = {
    val runtimeMirror = ru.runtimeMirror(loader)
    // Remove the '$' at the end of class name of an object
    val module = runtimeMirror.staticModule(className.replaceFirst("\\$$", ""))
    runtimeMirror.reflectModule(module).instance
  }

  private def parse(url: URL): Set[String] = {
    Utils.tryWithResource(url.openStream()) { input =>
      Source
        .fromInputStream(input)
        .getLines
        .toSet
    }
  }

  def load(loader: ClassLoader): Unit = {
    try {
      val configs = loader.getResources(CONFIG_OBJECTS_FILE)
      configs.asScala.flatMap(parse).foreach { className =>
        loadObjectInstance(className, loader)
      }
    } catch {
      case NonFatal(e) =>
        // We ignore the exceptions when loading class to make sure it won't fail the application
        logWarning(s"Failed to load spark conf objects", e)
    }
  }
}
