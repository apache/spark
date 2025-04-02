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

package org.apache.spark.scheduler.cluster

import org.apache.spark._
import org.apache.spark.deploy.yarn.ApplicationMaster

/**
 * This is a simple extension to YarnScheduler - to ensure that appropriate initialization of
 * ApplicationMaster, etc is done
 */
private[spark] class YarnClusterScheduler(sc: SparkContext) extends YarnScheduler(sc) {

  logInfo("Created YarnClusterScheduler")

  override def postStartHook(): Unit = {
    ApplicationMaster.sparkContextInitialized(sc)
    super.postStartHook()
    logInfo("YarnClusterScheduler.postStartHook done")
  }

}
