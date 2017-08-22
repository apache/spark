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
package org.apache.spark.deploy.kubernetes.integrationtest.jobs

import java.io.{File, FileInputStream}
import java.util.Properties

import com.google.common.collect.Maps
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

private[spark] object JavaOptionsTest {

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    if (args.length != 1) {
      println(s"Invalid arguments: ${args.mkString(",")}." +
        s"Usage: JavaOptionsTest <driver-java-options-list-file>")
      System.exit(1)
    }
    val expectedDriverJavaOptions = loadPropertiesFromFile(args(0))
    val nonMatchingDriverOptions = expectedDriverJavaOptions.filter {
      case (optKey, optValue) => System.getProperty(optKey) != optValue
    }
    if (nonMatchingDriverOptions.nonEmpty) {
      println(s"The driver's JVM options did not match. Expected $expectedDriverJavaOptions." +
        s" But these options did not match: $nonMatchingDriverOptions.")
      val sysProps = Maps.fromProperties(System.getProperties).asScala
      println("System properties are:")
      for (prop <- sysProps) {
        println(s"Key: ${prop._1}, Value: ${prop._2}")
      }
      System.exit(1)
    }

    // TODO support spark.executor.extraJavaOptions and test here.
    println(s"All expected JVM options were present on the driver and executors.")
    // scalastyle:on println
  }

  private def loadPropertiesFromFile(filePath: String): Map[String, String] = {
    val file = new File(filePath)
    if (!file.isFile) {
      throw new IllegalArgumentException(s"File not found at $filePath or is not a file.")
    }
    val properties = new Properties()
    Utils.tryWithResource(new FileInputStream(file)) { is =>
      properties.load(is)
    }
    Maps.fromProperties(properties).asScala.toMap
  }
}
