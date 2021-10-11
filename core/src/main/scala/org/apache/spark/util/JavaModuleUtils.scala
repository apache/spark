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

import scala.collection.mutable

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigEntry, DRIVER_JAVA_MODULE_OPTIONS, DRIVER_JAVA_OPTIONS, EXECUTOR_JAVA_MODULE_OPTIONS, EXECUTOR_JAVA_OPTIONS, OptionalConfigEntry}
import org.apache.spark.launcher.JavaModuleOptions

object JavaModuleUtils {

  def isJavaVersionAtLeast17: Boolean = SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_17)

  def defaultModuleOptions(): String =
    JavaModuleOptions.DEFAULT_MODULE_OPTIONS.mkString(" ", " ", " ")

  def supplementJavaModuleOptions(conf: SparkConf): Unit = {

    def supplementModuleOpts(
        targetEntry: OptionalConfigEntry[String],
        sourceEntry: ConfigEntry[String]): Unit = {
      val v = conf.get(targetEntry) match {
        case Some(opts) => conf.get(sourceEntry) + opts
        case None => conf.get(sourceEntry)
      }
      conf.set(targetEntry.key, v)
    }

    supplementModuleOpts(DRIVER_JAVA_OPTIONS, DRIVER_JAVA_MODULE_OPTIONS)
    supplementModuleOpts(EXECUTOR_JAVA_OPTIONS, EXECUTOR_JAVA_MODULE_OPTIONS)
  }

  def supplementJava17ModuleOptsIfNeeded(args: Seq[String]): Seq[String] = {

    def supplementModuleOpts(buffer: mutable.Buffer[String], key: String): Unit = {
      val index = buffer.indexWhere(_.startsWith(s"$key="))
      if (index != -1) {
        buffer.update(index, buffer(index) + JavaModuleUtils.defaultModuleOptions())
      } else {
        buffer.prependAll(Seq("--conf", s"$key=${JavaModuleUtils.defaultModuleOptions()}"))
      }
    }

    if (Utils.isTesting && isJavaVersionAtLeast17) {
      val buffer = args.toBuffer
      supplementModuleOpts(buffer, DRIVER_JAVA_OPTIONS.key)
      supplementModuleOpts(buffer, EXECUTOR_JAVA_OPTIONS.key)
      buffer.toSeq
    } else args
  }
}
