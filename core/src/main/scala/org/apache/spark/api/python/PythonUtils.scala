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
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

private[spark] object PythonUtils {
  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath +=
        Seq(sparkHome, "python", "lib", "py4j-0.10.8.1-src.zip").mkString(File.separator)
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

  def isEncryptionEnabled(sc: JavaSparkContext): Boolean = {
    sc.conf.get(org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED)
  }
}
