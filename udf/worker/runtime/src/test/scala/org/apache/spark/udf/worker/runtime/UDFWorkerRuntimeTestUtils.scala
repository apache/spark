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

package org.apache.spark.udf.worker.runtime

import java.io.File

import scala.util.Properties.versionNumberString

import org.scalatest.Assertions._

/**
 * Locates the jars directory that `assembly/package` produces, which is what a
 * packaged Spark puts on its classpath, and the UDF worker runtime jar in it.
 *
 * sbt packages the assembly before these tests run. Maven builds the assembly after
 * this module, so a Maven test run cancels the suites unless the assembly was
 * packaged beforehand.
 */
private[runtime] object UDFWorkerRuntimeTestUtils {

  val RUNTIME_JAR_PREFIX = "spark-udf-worker-runtime_"

  lazy val sparkHome: File = new File(
    sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!")))

  lazy val jarsDir: File = {
    val scalaVersion = versionNumberString.split('.') match {
      case Array(major, minor, _*) => s"$major.$minor"
      case _ => versionNumberString
    }
    new File(sparkHome, s"assembly/target/scala-$scalaVersion/jars")
  }

  /** Cancels the calling test when the assembly has not been built. */
  def assumeAssemblyBuilt(): Unit = {
    assume(jarsDir.isDirectory,
      s"$jarsDir does not exist; build the assembly (`assembly/package`) first")
  }

  /** The runtime jar in the jars directory. Fails if the assembly left it out. */
  def runtimeJar(): File = {
    val jars = jarsDir.listFiles().filter { f =>
      f.getName.startsWith(RUNTIME_JAR_PREFIX) && f.getName.endsWith(".jar")
    }
    assert(jars.length == 1,
      s"Expected one $RUNTIME_JAR_PREFIX*.jar in $jarsDir, found: ${jars.mkString(", ")}")
    jars.head
  }
}
