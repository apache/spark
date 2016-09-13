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

import org.apache.spark.util.{IntParam, Utils}
import org.apache.spark.SparkConf

private[mesos] class MesosClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
  var host: String = Utils.localHostName()
  var port: Int = 7077
  var name: String = "Spark Cluster"
  var webUiPort: Int = 8081
  var verbose: Boolean = false
  var masterUrl: String = _
  var zookeeperUrl: Option[String] = None
  var propertiesFile: String = _
  val confProperties: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  parse(args.toList)

  // scalastyle:on println
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)
  Utils.updateSparkConfigFromProperties(conf, confProperties)

  // scalastyle:off println
  if (verbose) {
    MesosClusterDispatcher.printStream.println(s"Using host: $host")
    MesosClusterDispatcher.printStream.println(s"Using port: $port")
    MesosClusterDispatcher.printStream.println(s"Using webUiPort: $webUiPort")
    MesosClusterDispatcher.printStream.println(s"Framework Name: $name")

    Option(propertiesFile).foreach { file =>
      MesosClusterDispatcher.printStream.println(s"Using properties file: $file")
    }

    MesosClusterDispatcher.printStream.println(s"Spark Config properties set:")
    conf.getAll.foreach(println)
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
        MesosClusterDispatcher.printStream
          .println("Cluster dispatcher only supports mesos (uri begins with mesos://)")
        // scalastyle:on println
        MesosClusterDispatcher.exitFn(1)
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
      MesosClusterDispatcher.
        parseSparkConfProperty(value, (k: String, v: String) => confProperties(k) = v)
      parse(tail)

    case ("--help") :: tail =>
        printUsageAndExit(0)

    case ("--verbose") :: tail =>
      verbose = true

    case Nil =>
      if (Option(masterUrl).isEmpty) {
        // scalastyle:off println
        MesosClusterDispatcher.printStream.println("--master is required")
        // scalastyle:on println
        printUsageAndExit(1)
      }

    case value@_ =>
      // scalastyle:off println
      MesosClusterDispatcher.printStream.println(s"Unrecognized option: '$value'")
      // scalastyle:on println
      printUsageAndExit(1)
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    val outStream = MesosClusterDispatcher.printStream

    // scalastyle:off println
    outStream.println(
      "Usage: MesosClusterDispatcher [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST    Hostname to listen on\n" +
        "  --help                  Show this help message and exit.\n" +
        "  --verbose,              Print additional debug output.\n" +
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
    MesosClusterDispatcher.exitFn(exitCode)
  }
}
