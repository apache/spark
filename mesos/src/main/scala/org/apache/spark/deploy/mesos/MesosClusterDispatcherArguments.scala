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

package org.apache.spark.deploy.mesos

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}


private[mesos] class MesosClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 7077
  var name = "Spark Cluster"
  var webUiPort = 8081
  var masterUrl: String = _
  var zookeeperUrl: Option[String] = None
  var propertiesFile: String = _
  val cliSparkConfig = new mutable.ListBuffer[String]()

  parse(args.toList)

  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  updateSparkConfigFromcliConfig()

  private def updateSparkConfigFromcliConfig() : Unit = {
    val properties = Utils.loadPropertiesFromString(cliSparkConfig.mkString("\n"))
    Utils.updateSparkConfigFromProperties(conf, properties)
  }

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--webui-port") :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--zk" | "-z") :: value :: tail =>
      zookeeperUrl = Some(value)
      parse(tail)

    case ("--master" | "-m") :: value :: tail =>
      if (!value.startsWith("mesos://")) {
        // scalastyle:off println
        System.err.println("Cluster dispatcher only supports mesos (uri begins with mesos://)")
        // scalastyle:on println
        System.exit(1)
      }
      masterUrl = value.stripPrefix("mesos://")
      parse(tail)

    case ("--name") :: value :: tail =>
      name = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--conf") :: value :: tail =>
      cliSparkConfig += value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil =>
      if (masterUrl == null) {
        // scalastyle:off println
        System.err.println("--master is required")
        // scalastyle:on println
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      "Usage: MesosClusterDispatcher [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST    Hostname to listen on\n" +
        "  -p PORT, --port PORT    Port to listen on (default: 7077)\n" +
        "  --webui-port WEBUI_PORT WebUI Port to listen on (default: 8081)\n" +
        "  --name NAME             Framework name to show in Mesos UI\n" +
        "  -m --master MASTER      URI for connecting to Mesos master\n" +
        "  -z --zk ZOOKEEPER       Comma delimited URLs for connecting to \n" +
        "                          Zookeeper for persistence\n" +
        "  --properties-file FILE  Path to a custom Spark properties file.\n" +
        "                          Default is conf/spark-defaults.conf \n" +
        "  --conf PROP=VALUE       Arbitrary Spark configuration property.\n" +
        "                          Takes precedence over defined properties in properties-file.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
