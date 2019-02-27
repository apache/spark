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

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s._

class EnvSecretsFeatureStepSuite extends SparkFunSuite {
  private val KEY_REF_NAME_FOO = "foo"
  private val KEY_REF_NAME_BAR = "bar"
  private val KEY_REF_KEY_FOO = "key_foo"
  private val KEY_REF_KEY_BAR = "key_bar"
  private val ENV_NAME_FOO = "MY_FOO"
  private val ENV_NAME_BAR = "MY_bar"

  test("sets up all keyRefs") {
    val baseDriverPod = SparkPod.initialPod()
    val envVarsToKeys = Map(
      ENV_NAME_BAR -> s"${KEY_REF_NAME_BAR}:${KEY_REF_KEY_BAR}",
      ENV_NAME_FOO -> s"${KEY_REF_NAME_FOO}:${KEY_REF_KEY_FOO}")
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      secretEnvNamesToKeyRefs = envVarsToKeys)

    val step = new EnvSecretsFeatureStep(kubernetesConf)
    val container = step.configurePod(baseDriverPod).container
    val containerEnvKeys = container.getEnv.asScala.map { v => v.getName }.toSet
    envVarsToKeys.keys.foreach { envName =>
      assert(containerEnvKeys.contains(envName))
    }
  }
}
