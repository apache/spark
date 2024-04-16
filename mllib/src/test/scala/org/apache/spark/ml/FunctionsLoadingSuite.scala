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

package org.apache.spark.ml

import org.apache.spark._
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.functions.col

class FunctionsLoadingSuite extends SparkFunSuite with LocalSparkContext {

  test("SPARK-45859: 'functions$' should not be affected by a broken class loader") {
    quietly {
      val conf = new SparkConf()
        .setAppName("FunctionsLoadingSuite")
        .setMaster("local-cluster[1,1,1024]")
      sc = new SparkContext(conf)
      // Make `functions$` be loaded by a broken class loader
      intercept[SparkException] {
        sc.parallelize(1 to 1).foreach { _ =>
          val originalClassLoader = Thread.currentThread.getContextClassLoader
          try {
            Thread.currentThread.setContextClassLoader(new BrokenClassLoader)
            vector_to_array(col("vector"))
            array_to_vector(col("array"))
          } finally {
            Thread.currentThread.setContextClassLoader(originalClassLoader)
          }
        }
      }

      // We should be able to use `functions$` even it was loaded by a broken class loader
      sc.parallelize(1 to 1).foreach { _ =>
        vector_to_array(col("vector"))
        array_to_vector(col("array"))
      }
    }
  }
}

class BrokenClassLoader extends ClassLoader {
  override def findClass(name: String): Class[_] = {
    throw new Error(s"class $name")
  }
}
