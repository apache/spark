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

package org.apache.spark.sql.hive.security

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.util.Utils

class HiveHadoopDelegationTokenManagerSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  test("default configuration") {
    val manager = new HadoopDelegationTokenManager(new SparkConf(false), hadoopConf, null)
    assert(manager.isProviderLoaded("hive"))
  }

  test("using deprecated configurations") {
    val sparkConf = new SparkConf(false)
      .set("spark.yarn.security.credentials.hive.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(!manager.isProviderLoaded("hive"))
  }

  test("SPARK-23209: obtain tokens when Hive classes are not available") {
    // This test needs a custom class loader to hide Hive classes which are in the classpath.
    // Because the manager code loads the Hive provider directly instead of using reflection, we
    // need to drive the test through the custom class loader so a new copy that cannot find
    // Hive classes is loaded.
    val currentLoader = Thread.currentThread().getContextClassLoader()
    val noHive = new ClassLoader() {
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (name.startsWith("org.apache.hive") || name.startsWith("org.apache.hadoop.hive")) {
          throw new ClassNotFoundException(name)
        }

        val prefixBlacklist = Seq("java", "scala", "com.sun.", "sun.")
        if (prefixBlacklist.exists(name.startsWith(_))) {
          return currentLoader.loadClass(name)
        }

        val found = findLoadedClass(name)
        if (found != null) {
          return found
        }

        val classFileName = name.replaceAll("\\.", "/") + ".class"
        val in = currentLoader.getResourceAsStream(classFileName)
        if (in != null) {
          val bytes = IOUtils.toByteArray(in)
          return defineClass(name, bytes, 0, bytes.length)
        }

        throw new ClassNotFoundException(name)
      }
    }

    Utils.withContextClassLoader(noHive) {
      val test = noHive.loadClass(NoHiveTest.getClass.getName().stripSuffix("$"))
      test.getMethod("runTest").invoke(null)
    }
  }
}

/** Test code for SPARK-23209 to avoid using too much reflection above. */
private object NoHiveTest {

  def runTest(): Unit = {
    try {
      val manager = new HadoopDelegationTokenManager(new SparkConf(), new Configuration(), null)
      assert(manager.isProviderLoaded("hadoopfs"))
      assert(manager.isProviderLoaded("hbase"))
      require(!manager.isProviderLoaded("hive"))
    } catch {
      case e: Throwable =>
        // Throw a better exception in case the test fails, since there may be a lot of nesting.
        var cause = e
        while (cause.getCause() != null) {
          cause = cause.getCause()
        }
        throw cause
    }
  }

}
