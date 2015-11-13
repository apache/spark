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

package org.apache.spark.util

import java.net.{Inet4Address, NetworkInterface, InetAddress}

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.Try

private[spark] object DockerUtils {

  def getDockerIp(): String = {
    /** If docker-machine is setup on this box, attempts to find the ip from it. */
    def findFromDockerMachine(): Option[String] = {
      sys.env.get("DOCKER_MACHINE_NAME").flatMap { name =>
        Try(Seq("/bin/bash", "-c", s"docker-machine ip $name 2>/dev/null").!!.trim).toOption
      }
    }
    sys.env.get("DOCKER_IP")
      .orElse(findFromDockerMachine())
      .orElse(Try(Seq("/bin/bash", "-c", "boot2docker ip 2>/dev/null").!!.trim).toOption)
      .getOrElse {
        // This block of code is based on Utils.findLocalInetAddress(), but is modified to blacklist
        // certain interfaces.
        val address = InetAddress.getLocalHost
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val blackListedIFs = Seq(
          "vboxnet0",  // Mac
          "docker0"    // Linux
        )
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq.filter { i =>
          !blackListedIFs.contains(i.getName)
        }
        val reOrderedNetworkIFs = activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            return strippedAddress.getHostAddress
          }
        }
        address.getHostAddress
      }
  }
}
