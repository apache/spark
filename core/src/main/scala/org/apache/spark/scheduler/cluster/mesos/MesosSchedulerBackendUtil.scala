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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.Protos.{ContainerInfo, Volume}
import org.apache.mesos.Protos.ContainerInfo.DockerInfo

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * A collection of utility functions which can be used by both the
 * MesosSchedulerBackend and the [[MesosFineGrainedSchedulerBackend]].
 */
private[mesos] object MesosSchedulerBackendUtil extends Logging {
  /**
   * Parse a comma-delimited list of volume specs, each of which
   * takes the form [host-dir:]container-dir[:rw|:ro].
   */
  def parseVolumesSpec(volumes: String): List[Volume] = {
    volumes.split(",").map(_.split(":")).flatMap { spec =>
        val vol: Volume.Builder = Volume
          .newBuilder()
          .setMode(Volume.Mode.RW)
        spec match {
          case Array(container_path) =>
            Some(vol.setContainerPath(container_path))
          case Array(container_path, "rw") =>
            Some(vol.setContainerPath(container_path))
          case Array(container_path, "ro") =>
            Some(vol.setContainerPath(container_path)
              .setMode(Volume.Mode.RO))
          case Array(host_path, container_path) =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path))
          case Array(host_path, container_path, "rw") =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path))
          case Array(host_path, container_path, "ro") =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path)
              .setMode(Volume.Mode.RO))
          case spec =>
            logWarning(s"Unable to parse volume specs: $volumes. "
              + "Expected form: \"[host-dir:]container-dir[:rw|:ro](, ...)\"")
            None
      }
    }
    .map { _.build() }
    .toList
  }

  /**
   * Parse a comma-delimited list of port mapping specs, each of which
   * takes the form host_port:container_port[:udp|:tcp]
   *
   * Note:
   * the docker form is [ip:]host_port:container_port, but the DockerInfo
   * message has no field for 'ip', and instead has a 'protocol' field.
   * Docker itself only appears to support TCP, so this alternative form
   * anticipates the expansion of the docker form to allow for a protocol
   * and leaves open the chance for mesos to begin to accept an 'ip' field
   */
  def parsePortMappingsSpec(portmaps: String): List[DockerInfo.PortMapping] = {
    portmaps.split(",").map(_.split(":")).flatMap { spec: Array[String] =>
      val portmap: DockerInfo.PortMapping.Builder = DockerInfo.PortMapping
        .newBuilder()
        .setProtocol("tcp")
      spec match {
        case Array(host_port, container_port) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt))
        case Array(host_port, container_port, protocol) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt)
            .setProtocol(protocol))
        case spec =>
          logWarning(s"Unable to parse port mapping specs: $portmaps. "
            + "Expected form: \"host_port:container_port[:udp|:tcp](, ...)\"")
          None
      }
    }
    .map { _.build() }
    .toList
  }

  /**
   * Construct a DockerInfo structure and insert it into a ContainerInfo
   */
  def addDockerInfo(
      container: ContainerInfo.Builder,
      image: String,
      volumes: Option[List[Volume]] = None,
      network: Option[ContainerInfo.DockerInfo.Network] = None,
      portmaps: Option[List[ContainerInfo.DockerInfo.PortMapping]] = None): Unit = {

    val docker = ContainerInfo.DockerInfo.newBuilder().setImage(image)

    network.foreach(docker.setNetwork)
    portmaps.foreach(_.foreach(docker.addPortMappings))
    container.setType(ContainerInfo.Type.DOCKER)
    container.setDocker(docker.build())
    volumes.foreach(_.foreach(container.addVolumes))
  }

  /**
   * Setup a docker containerizer
   */
  def setupContainerBuilderDockerInfo(
    imageName: String,
    conf: SparkConf,
    builder: ContainerInfo.Builder): Unit = {
    val volumes = conf
      .getOption("spark.mesos.executor.docker.volumes")
      .map(parseVolumesSpec)
    val portmaps = conf
      .getOption("spark.mesos.executor.docker.portmaps")
      .map(parsePortMappingsSpec)
    addDockerInfo(
      builder,
      imageName,
      volumes = volumes,
      portmaps = portmaps)
    logDebug("setupContainerDockerInfo: using docker image: " + imageName)
  }
}
