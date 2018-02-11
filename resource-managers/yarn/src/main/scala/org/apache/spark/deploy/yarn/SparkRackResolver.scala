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

package org.apache.spark.deploy.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

/**
 * Wrapper around YARN's [[RackResolver]]. This allows Spark tests to easily override the
 * default behavior, since YARN's class self-initializes the first time it's called, and
 * future calls all use the initial configuration.
 */
private[yarn] class SparkRackResolver {

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  def resolve(conf: Configuration, hostName: String): String = {
    RackResolver.resolve(conf, hostName).getNetworkLocation()
  }

}
