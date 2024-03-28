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
package org.apache.spark.scheduler.cluster.k8s

import org.mockito.Mockito.mock
import org.mockito.Mockito.when

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

class KubernetesExecutorBackendSuite extends SparkFunSuite {
  test("extract log urls and attributes") {
    val mockRpcEnv = mock(classOf[RpcEnv])
    val mockSparkEnv = mock(classOf[SparkEnv])
    val conf = new SparkConf()
    when(mockSparkEnv.conf).thenReturn(conf)
    val mockResourceProfile = mock(classOf[ResourceProfile])

    val backend = new KubernetesExecutorBackend(
      mockRpcEnv, "app-id", "driver-url", "executor-id", "bind-address", "hostname", "pod-name",
      1, mockSparkEnv, None, mockResourceProfile)

    val expectedKubernetesAttributes = Map(
      "LOG_FILES" -> "log",
      "APP_ID" -> "app-id",
      "EXECUTOR_ID" -> "executor-id",
      "HOSTNAME" -> "hostname",
      "KUBERNETES_NAMESPACE" -> "default",
      "KUBERNETES_POD_NAME" -> "pod-name"
    )

    assert(backend.extractLogUrls === Map.empty)
    assert(backend.extractAttributes === expectedKubernetesAttributes)

    withEnvs(
      "SPARK_LOG_URL_STDOUT" -> "https://my.custom.url/logs/stdout",
      "SPARK_LOG_URL_STDERR" -> "https://my.custom.url/logs/stderr",
      "SPARK_EXECUTOR_ATTRIBUTE_ENV_1" -> "val1",
      "SPARK_EXECUTOR_ATTRIBUTE_ENV_2" -> "val2") {
      assert(backend.extractLogUrls === Map(
        "stdout" -> "https://my.custom.url/logs/stdout",
        "stderr" -> "https://my.custom.url/logs/stderr"
      ))
      assert(backend.extractAttributes === Map(
        "ENV_1" -> "val1", "ENV_2" -> "val2"
      ) ++ expectedKubernetesAttributes)
    }

    // env vars have precedence
    withEnvs(
      "SPARK_EXECUTOR_ATTRIBUTE_LOG_FILES" -> "env-log",
      "SPARK_EXECUTOR_ATTRIBUTE_APP_ID" -> "env-app-id",
      "SPARK_EXECUTOR_ATTRIBUTE_EXECUTOR_ID" -> "env-exec-id",
      "SPARK_EXECUTOR_ATTRIBUTE_HOSTNAME" -> "env-hostname",
      "SPARK_EXECUTOR_ATTRIBUTE_KUBERNETES_NAMESPACE" -> "env-namespace",
      "SPARK_EXECUTOR_ATTRIBUTE_KUBERNETES_POD_NAME" -> "env-pod-name") {
      assert(backend.extractAttributes === Map(
        "LOG_FILES" -> "env-log",
        "APP_ID" -> "env-app-id",
        "EXECUTOR_ID" -> "env-exec-id",
        "HOSTNAME" -> "env-hostname",
        "KUBERNETES_NAMESPACE" -> "env-namespace",
        "KUBERNETES_POD_NAME" -> "env-pod-name"
      ))
    }

    // namespace 'default' above is the config default, here we set a value
    conf.set("spark.kubernetes.namespace", "my-namespace")
    assert(backend.extractAttributes === expectedKubernetesAttributes ++
      Map("KUBERNETES_NAMESPACE" -> "my-namespace"))
  }

  private def withEnvs(pairs: (String, String)*)(f: => Unit): Unit = {
    val readonlyEnv = System.getenv()
    val field = readonlyEnv.getClass.getDeclaredField("m")
    field.setAccessible(true)
    val modifiableEnv = field.get(readonlyEnv).asInstanceOf[java.util.Map[String, String]]
    try {
      for ((k, v) <- pairs) {
        assert(!modifiableEnv.containsKey(k))
        modifiableEnv.put(k, v)
      }
      f
    } finally {
      for ((k, _) <- pairs) {
        modifiableEnv.remove(k)
      }
    }
  }
}
