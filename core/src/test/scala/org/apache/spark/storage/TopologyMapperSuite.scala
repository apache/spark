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

package org.apache.spark.storage

import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark._
import org.apache.spark.util.Utils

class TopologyMapperSuite  extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  test("File based Topology Mapper") {
    val numHosts = 100
    val numRacks = 4
    val props = (1 to numHosts).map{i => s"host-$i" -> s"rack-${i % numRacks}"}.toMap
    val propsFile = createPropertiesFile(props)

    val sparkConf = (new SparkConf(false))
    sparkConf.set("spark.storage.replication.topologyFile", propsFile.getAbsolutePath)
    val topologyMapper = new FileBasedTopologyMapper(sparkConf)

    props.foreach {case (host, topology) =>
      val obtainedTopology = topologyMapper.getTopologyForHost(host)
      assert(obtainedTopology.isDefined)
      assert(obtainedTopology.get === topology)
    }

    // we get None for hosts not in the file
    assert(topologyMapper.getTopologyForHost("host").isEmpty)

    cleanup(propsFile)
  }

  def createPropertiesFile(props: Map[String, String]): File = {
    val testFile = new File(Utils.createTempDir(), "TopologyMapperSuite-test").getAbsoluteFile
    val fileOS = new FileOutputStream(testFile)
    props.foreach{case (k, v) => fileOS.write(s"$k=$v\n".getBytes)}
    fileOS.close
    testFile
  }

  def cleanup(testFile: File): Unit = {
    testFile.getParentFile.listFiles.filter { file =>
      file.getName.startsWith(testFile.getName)
    }.foreach { _.delete() }
  }

}
