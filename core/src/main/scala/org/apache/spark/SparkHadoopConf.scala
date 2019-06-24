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

package org.apache.spark

import java.net.URL

import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Hadoop Configuration for the Hadoop code (e.g. file systems).
 */

class SparkHadoopConf(var conf: Configuration) {

  def get(name: String): String = conf.get(name)

  def set(entries: (String, String)*): Unit = {
    entries.foreach { case (name, value) =>
      conf.set(name, value)
    }
  }

  def set(name: String, value: String): Unit = set(name -> value)

  def unset(name: String): Unit = conf.unset(name)

  def addResource(url: URL): Unit = conf.addResource(url)
}

object SparkHadoopConf {
  private var hadoopConf: ThreadLocal[SparkHadoopConf] = _

  def init(conf: SparkConf): Unit = {
    hadoopConf = new ThreadLocal[SparkHadoopConf]() {
      override def initialValue: SparkHadoopConf = {
        val _hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
        // Performance optimization: this dummy call to .size() triggers eager evaluation of
        // Configuration's internal  `properties` field, guaranteeing that it will be computed and
        // cached before SessionState.newHadoopConf() uses `sc.hadoopConfiguration` to create
        // a new per-session Configuration. If `properties` has not been computed by that time
        // then each newly-created Configuration will perform its own expensive IO and XML
        // parsing to load configuration defaults and populate its own properties. By ensuring
        // that we've pre-computed the parent's properties, the child Configuration will simply
        // clone the parent's properties.
        _hadoopConf.size()
        new SparkHadoopConf(_hadoopConf)
      }
    }
  }

  def get: SparkHadoopConf = hadoopConf.get
}
