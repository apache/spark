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
import org.apache.mesos.Protos.ContainerInfo.Builder

/**
 * A collection of utility functions which can be used by both the
 * MesosSchedulerBackend and the CoarseMesosSchedulerBackend.
 */
private[spark] object MesosSchedulerBackendUtil {
  def withDockerInfo(container: ContainerInfo.Builder, image: String, volumes: Option[String]) = {
    container.setType(ContainerInfo.Type.DOCKER)
    container.setDocker(ContainerInfo.DockerInfo.newBuilder().setImage(image).build())

    def mkVol(p: String, m:Volume.Mode) {
      container.addVolumesBuilder().setContainerPath(p).setMode(m)
    }

    def mkMnt(s: String, d: String, m: Volume.Mode) {
      container.addVolumesBuilder().setContainerPath(d).setHostPath(s).setMode(m)
    }

    volumes.map {
      _.split(",").map(_.split(":")).map {
        _ match {
          case Array(container_path) =>
            mkVol(container_path, Volume.Mode.RW)
          case Array(container_path, "rw") =>
            mkVol(container_path, Volume.Mode.RW)
          case Array(container_path, "ro") =>
            mkVol(container_path, Volume.Mode.RO)
          case Array(host_path, container_path) =>
            mkMnt(host_path, container_path, Volume.Mode.RW)
          case Array(host_path, container_path, "rw") =>
            mkMnt(host_path, container_path, Volume.Mode.RW)
          case Array(host_path, container_path, "ro") =>
            mkMnt(host_path, container_path, Volume.Mode.RO)
          case _ => ()
        }
      }
    }
  }
}
