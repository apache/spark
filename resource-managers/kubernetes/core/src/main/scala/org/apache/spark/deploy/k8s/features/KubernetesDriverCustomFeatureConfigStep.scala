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
package org.apache.spark.deploy.k8s.features

import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.deploy.k8s.KubernetesDriverConf

/**
 * :: DeveloperApi ::
 *
 * A base interface to help user extend custom feature step in driver side.
 * Note: If your custom feature step would be used only in driver or both in driver and executor,
 * please use this.
 */
@Unstable
@DeveloperApi
trait KubernetesDriverCustomFeatureConfigStep extends KubernetesFeatureConfigStep {
  /**
   * Initialize the configuration for driver user feature step, this only applicable when user
   * specified `spark.kubernetes.driver.pod.featureSteps`, the init will be called after feature
   * step loading.
   */
  def init(config: KubernetesDriverConf): Unit
}

