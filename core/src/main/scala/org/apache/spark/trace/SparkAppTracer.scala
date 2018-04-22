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

package org.apache.spark.trace

import org.apache.htrace.core.{HTraceConfiguration, Tracer}

import org.apache.spark.SparkConf

object SparkAppTracer {
  private val SPARK_HTRACE_PREFIX = "spark.htrace."

  def getTracer(name: String, conf: SparkConf): Tracer = {
    new Tracer.Builder(name).conf(wrapSparkConf(conf)).build()
  }

  def wrapSparkConf(conf: SparkConf): HTraceConfiguration = {
    new HTraceConfiguration {
      override def get(key: String, defaultVal: String): String =
        conf.get(SPARK_HTRACE_PREFIX + key, defaultVal)

      override def get(key: String): String = {
        get(key, null)
      }
    }
  }
}
