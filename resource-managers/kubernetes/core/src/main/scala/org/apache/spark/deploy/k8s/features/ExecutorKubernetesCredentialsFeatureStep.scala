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

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME}
import org.apache.spark.deploy.k8s.KubernetesUtils.{buildPodWithServiceAccount, podServiceAccount}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, SERVICE_ACCOUNT_NAME, VALUE}

private[spark] class ExecutorKubernetesCredentialsFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging {

  private lazy val driverServiceAccount = kubernetesConf.get(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME)
  private lazy val executorServiceAccount =
    kubernetesConf.get(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME)

  override def configurePod(pod: SparkPod): SparkPod = {
    val templateServiceAccount = podServiceAccount(pod)
    templateServiceAccount.foreach(reportAccountNotApplied)
    pod.copy(
      // if not setup by the pod template, fallback to the executor's sa,
      // if executor's sa is not setup, the last option is driver's sa.
      pod = if (templateServiceAccount.isEmpty) {
        buildPodWithServiceAccount(executorServiceAccount.orElse(driverServiceAccount), pod)
          .getOrElse(pod.pod)
      } else {
        pod.pod
      })
  }

  /**
   * The pod template's account takes precedence, so report a configured one that did not apply.
   * An account set through the executor configuration is an explicit instruction that had no
   * effect, so that is a warning. The driver's account only ever served as a fallback here, so a
   * template superseding it is the documented outcome and says so at INFO. Either way, stay quiet
   * when the template names the same account, and when the configured value is empty and so names
   * no account at all.
   *
   * With the default pod allocator both repeat once per executor pod, since the feature steps are
   * rebuilt for each one. Each warning corresponds to one pod launched with a different account
   * than the configuration asked for. The INFO reports a correct configuration, and adds one line
   * to the one `BasicExecutorFeatureStep` already logs per pod.
   */
  private def reportAccountNotApplied(templateAccount: String): Unit = {
    executorServiceAccount match {
      case Some(configured) =>
        // An empty value counts as set on this path, which is long-standing; nothing to report.
        if (configured.nonEmpty && configured != templateAccount) {
          logWarning(log"Not applying " +
            log"${MDC(CONFIG, KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME.key)}=" +
            log"${MDC(VALUE, configured)} to the executor pod, because its pod template already " +
            log"names ${MDC(SERVICE_ACCOUNT_NAME, templateAccount)}, which takes precedence. " +
            log"Remove the account from the template to have Spark apply that configuration " +
            log"instead.")
        }
      case None =>
        driverServiceAccount.filter(a => a.nonEmpty && a != templateAccount).foreach { account =>
          logInfo(log"The executor pod template names " +
            log"${MDC(SERVICE_ACCOUNT_NAME, templateAccount)}, so the executor pods use it " +
            log"rather than ${MDC(CONFIG, KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME.key)}=" +
            log"${MDC(VALUE, account)}, which is only a fallback for executor pods.")
        }
    }
  }
}
