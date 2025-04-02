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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class SerDeUtilSuite extends SparkFunSuite with SharedSparkContext {

  test("Converting an empty pair RDD to python does not throw an exception (SPARK-5441)") {
    val emptyRdd = sc.makeRDD(Seq[(Any, Any)]())
    SerDeUtil.pairRDDToPython(emptyRdd, 10)
  }

  test("Converting an empty python RDD to pair RDD does not throw an exception (SPARK-5441)") {
    val emptyRdd = sc.makeRDD(Seq[(Any, Any)]())
    val javaRdd = emptyRdd.toJavaRDD()
    val pythonRdd = SerDeUtil.javaToPython(javaRdd)
    SerDeUtil.pythonToPairRDD(pythonRdd, false)
  }
}

