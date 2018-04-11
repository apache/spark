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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.LocalObjectReference

import org.apache.spark.SparkFunSuite

class KubernetesUtilsTest extends SparkFunSuite {

  test("testParseImagePullSecrets") {
    val noSecrets = KubernetesUtils.parseImagePullSecrets(None)
    assert(noSecrets === Nil)

    val oneSecret = KubernetesUtils.parseImagePullSecrets(Some("imagePullSecret"))
    assert(oneSecret === new LocalObjectReference("imagePullSecret") :: Nil)

    val commaSeparatedSecrets = KubernetesUtils.parseImagePullSecrets(Some("s1, s2  , s3,s4"))
    assert(commaSeparatedSecrets.map(_.getName) === "s1" :: "s2" :: "s3" :: "s4" :: Nil)
  }

}
