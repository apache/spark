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

package org.apache.spark.api.python

import java.io.File
import java.net.{URL, URI}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}
import org.apache.spark.util.{MutableURLClassLoader, Utils}

private[spark] object PythonUtils extends Logging {
  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.9-src.zip").mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(File.pathSeparator)
  }

  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }

  def generateRDDWithNull(sc: JavaSparkContext): JavaRDD[String] = {
    sc.parallelize(List("a", null, "b"))
  }

  /**
   * Convert list of T into seq of T (for calling API with varargs)
   */
  def toSeq[T](vs: JList[T]): Seq[T] = {
    vs.asScala
  }

  /**
   * Convert list of T into a (Scala) List of T
   */
  def toList[T](vs: JList[T]): List[T] = {
    vs.asScala.toList
  }

  /**
   * Convert list of T into array of T (for calling API with array)
   */
  def toArray[T](vs: JList[T]): Array[T] = {
    vs.toArray().asInstanceOf[Array[T]]
  }

  /**
   * Convert java map of K, V into Map of K, V (for calling API with varargs)
   */
  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }

  /**
   * Update the current threads class loader.
   * Requires the current class loader is a MutableURLClassLoader, otherwise skips updating with a
   * warning. Intended for use by addJar(), although constructing an instance of the class will
   * still require:
   * sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("class name")
   * as described in SPARK-5185.
   */
  def updatePrimaryClassLoader(jsc: JavaSparkContext) {
    val sc = jsc.sc
    val jars = sc.addedJars.keys
    val currentCL = Utils.getContextOrSparkClassLoader
    currentCL match {
      case cl: MutableURLClassLoader => {
        val existingJars = cl.getURLs().map(_.toString).toSet
        val newJars = (jars.toSet -- existingJars)
        logDebug(s"Adding jars ${newJars} to ${existingJars}")
        val newJarURLs = newJars.map(new URI(_).toURL())
        newJarURLs.foreach(cl.addURL(_))
      }
      case _ => logWarning(s"Unsupported class loader $currentCL will not update jars")
    }
  }
}
