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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, ContainerBuilder, EnvVar, HasMetadata, PodBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH
import org.apache.spark.deploy.k8s.Constants._

private[spark] class ReconstructEnvFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {
  import ReconstructEnvFeatureStep._

  private val additionalResources = ArrayBuffer.empty[HasMetadata]

  private def configMapName = {
    val suffix = "-" + POD_SHELL_PROFILE_CONFIGMAP
    val prefix = conf.resourceNamePrefix
    s"${prefix.take(KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH - suffix.length)}$suffix"
  }

  private def generateProfileContent(envWithDeps: Seq[EnvVar],
                                     mappings: Map[String, Set[String]]): String = {
    // preprocess, build a map of env name to EnvVar
    val envWithDepsMap = envWithDeps.map { env =>
      env.getName -> env
    }.toMap

    // init
    val reorderedEnvWithDeps = new ArrayBuffer[EnvVar]()
    val visited = new mutable.HashSet[String]()

    // reorder envWithDeps so that env variables that rely on other variables are placed later.
    def dfsVisit(env: EnvVar): Unit = {
      if (!visited.contains(env.getName)) {
        visited += env.getName // added to visited earlier to avoid circle reference here
        mappings.get(env.getName).foreach { deps =>
          // filter itself, so PATH=$PATH:/usr/bin would be considered independent.
          deps.filter(_ != env.getName).foreach { dep =>
            envWithDepsMap.get(dep).foreach { depEnv =>
              dfsVisit(depEnv)
            }
          }
        }
        reorderedEnvWithDeps += env
      }
    }
    // try to reorder all the env variables
    envWithDeps.foreach(dfsVisit)
    // returns k=v pairs
    reorderedEnvWithDeps.map { env =>
      val quotedValue = if (env.getValue.startsWith("\"")) {
        // assumes env value is already quoted
        env.getValue
      } else {
        "\"" + env.getValue + "\""
      }
      s"${env.getName}=$quotedValue"
    }.mkString("\n")
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    // extract all the environment variables from the pod that relies on some variables including
    // itself, such as PATH=$PATH:/usr/bin, or LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib
    val envWithDepsMappings = pod.container.getEnv.asScala
      .flatMap { env =>
        // get all variables in the value of the environment variable
        val variables = Option(env.getValue).map { envValue =>
          ENV_VARIABLE_NAME_REGEX.findAllIn(envValue)
            .map(_.stripPrefix("$").stripPrefix("{").stripSuffix("}")).toSet
        }.getOrElse(Set.empty)
        if (variables.nonEmpty) {
          // this environment variable relies on some other variables
          Some(env.getName -> variables)
        } else {
          // this is a simple environment variable, which could be set directly by K8S.
          None
        }
      }.toMap

    if (envWithDepsMappings.nonEmpty) {
      val (envWithDeps, envWithoutDeps) = pod.container.getEnv.asScala
        .partition(env => envWithDepsMappings.contains(env.getName))
      val containerWithNewEnvAndVolumes = new ContainerBuilder(pod.container)
        .withEnv(envWithoutDeps.asJava)
        .addNewVolumeMount()
          .withName(POD_SHELL_PROFILE_VOLUME)
          .withMountPath(POD_SHELL_PROFILE_MOUNTPATH)
        .endVolumeMount()
        .build()
      val podWithVolume = new PodBuilder(pod.pod)
        .editSpec()
          .addNewVolume()
              .withName(POD_SHELL_PROFILE_VOLUME)
              .withNewConfigMap()
                .withName(configMapName)
                .addNewItem()
                  .withKey(POD_SHELL_PROFILE_KEY)
                  .withPath(POD_SHELL_PROFILE_FILE_NAME)
                .endItem()
            .endConfigMap()
          .endVolume()
        .endSpec()
        .build()
      // add the config map as additional resource
      val content = generateProfileContent(envWithDeps, envWithDepsMappings)
      additionalResources += new ConfigMapBuilder()
        .withNewMetadata()
          .withName(configMapName)
        .endMetadata()
        .withImmutable(true)
        .addToData(POD_SHELL_PROFILE_KEY, content)
        .build()
      new SparkPod(podWithVolume, containerWithNewEnvAndVolumes)
    } else {
      // no environment variable relies on others, no need to reconstruct
      pod
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    additionalResources
  }
}

private[spark] object ReconstructEnvFeatureStep {
  // according to opengroup:
  //   https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_235
  // environment variable name can only contain underscores, digits, and alphabetics from the
  // portable character set. And the first character of a name is not a digit.
  private val ENV_VARIABLE_NAME_REGEX =
    "\\$([A-Za-z_]+[A-Za-z0-9_]*|\\{[A-Za-z_]+[A-Za-z0-9_]*\\})".r
}
