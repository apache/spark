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

import org.apache.mesos.Protos.{ContainerInfo, Image, NetworkInfo, Parameter, Volume}
import org.apache.mesos.Protos.ContainerInfo.{DockerInfo, MesosInfo}

import org.apache.spark.{SparkConf, SparkException}
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
   * Parse a list of docker parameters, each of which
   * takes the form key=value
   */
  private def parseParamsSpec(params: String): List[Parameter] = {
    // split with limit of 2 to avoid parsing error when '='
    // exists in the parameter value
    params.split(",").map(_.split("=", 2)).flatMap { spec: Array[String] =>
      val param: Parameter.Builder = Parameter.newBuilder()
      spec match {
        case Array(key, value) =>
          Some(param.setKey(key).setValue(value))
        case spec =>
          logWarning(s"Unable to parse arbitary parameters: $params. "
            + "Expected form: \"key=value(, ...)\"")
          None
      }
    }
    .map { _.build() }
    .toList
  }

  def containerInfo(conf: SparkConf): ContainerInfo = {
    val containerType = if (conf.contains("spark.mesos.executor.docker.image") &&
      conf.get("spark.mesos.containerizer", "docker") == "docker") {
      ContainerInfo.Type.DOCKER
    } else {
      ContainerInfo.Type.MESOS
    }

    val containerInfo = ContainerInfo.newBuilder()
      .setType(containerType)

    conf.getOption("spark.mesos.executor.docker.image").map { image =>
      val forcePullImage = conf
        .getOption("spark.mesos.executor.docker.forcePullImage")
        .exists(_.equals("true"))

      val portMaps = conf
        .getOption("spark.mesos.executor.docker.portmaps")
        .map(parsePortMappingsSpec)
        .getOrElse(List.empty)

      val params = conf
        .getOption("spark.mesos.executor.docker.parameters")
        .map(parseParamsSpec)
        .getOrElse(List.empty)

      if (containerType == ContainerInfo.Type.DOCKER) {
        containerInfo
          .setDocker(dockerInfo(image, forcePullImage, portMaps, params))
      } else {
        containerInfo.setMesos(mesosInfo(image, forcePullImage))
      }

      val volumes = conf
        .getOption("spark.mesos.executor.docker.volumes")
        .map(parseVolumesSpec)

      volumes.foreach(_.foreach(containerInfo.addVolumes(_)))
    }

    conf.getOption("spark.mesos.network.name").map { name =>
      val info = NetworkInfo.newBuilder().setName(name).build()
      containerInfo.addNetworkInfos(info)
    }

    containerInfo.build()
  }

  private def dockerInfo(
      image: String,
      forcePullImage: Boolean,
      portMaps: List[ContainerInfo.DockerInfo.PortMapping],
      params: List[Parameter]): DockerInfo = {
    val dockerBuilder = ContainerInfo.DockerInfo.newBuilder()
      .setImage(image)
      .setForcePullImage(forcePullImage)
    portMaps.foreach(dockerBuilder.addPortMappings(_))
    params.foreach(dockerBuilder.addParameters(_))

    dockerBuilder.build
  }

  private def mesosInfo(image: String, forcePullImage: Boolean): MesosInfo = {
    val imageProto = Image.newBuilder()
      .setType(Image.Type.DOCKER)
      .setDocker(Image.Docker.newBuilder().setName(image))
      .setCached(!forcePullImage)
    ContainerInfo.MesosInfo.newBuilder()
      .setImage(imageProto)
      .build
  }
}
