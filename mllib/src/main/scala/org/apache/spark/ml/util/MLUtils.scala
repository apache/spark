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

package org.apache.spark.ml.util

import org.apache.spark.SparkConf
import org.apache.spark.ml.tree.impl._
import org.apache.spark.util.Utils

private[spark] object MLUtils {

  private[this] var kryoRegistered: Boolean = false

  def registerKryoClasses(conf: SparkConf): Unit = {
    if (!kryoRegistered) {
      conf.registerKryoClasses(
        Array(
          Utils.classForName("org.apache.spark.ml.tree.impl.TreePoint$mcI$sp"),
          Utils.classForName("org.apache.spark.ml.tree.impl.TreePoint$mcS$sp"),
          Utils.classForName("org.apache.spark.ml.tree.impl.TreePoint$mcB$sp"),
          Utils.classForName("scala.math.Numeric$IntIsIntegral$"),
          Utils.classForName("scala.math.Numeric$ShortIsIntegral$"),
          Utils.classForName("scala.math.Numeric$ByteIsIntegral$"),
          Utils.classForName("scala.reflect.ManifestFactory$IntManifest"),
          Utils.classForName("scala.reflect.ManifestFactory$ShortManifest"),
          Utils.classForName("scala.reflect.ManifestFactory$ByteManifest"),
          classOf[BaggedPoint[TreePoint[_]]]
        )
      )
      kryoRegistered = true
    }
  }
}
