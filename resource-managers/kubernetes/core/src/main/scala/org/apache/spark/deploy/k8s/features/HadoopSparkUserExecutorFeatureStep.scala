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

import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil

/**
 * This step is responsible for setting ENV_SPARK_USER when HADOOP_FILES are detected
 * however, this step would not be run if Kerberos is enabled, as Kerberos sets SPARK_USER
 */
private[spark] class HadoopSparkUserExecutorFeatureStep(conf: KubernetesExecutorConf)
  extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    conf.getOption(KERBEROS_SPARK_USER_NAME).map { user =>
      HadoopBootstrapUtil.bootstrapSparkUserPod(user, pod)
    }.getOrElse(pod)
  }
}
