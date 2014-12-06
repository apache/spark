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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.SQLContext

/**
 * Test [[LabeledPoint]]
 */
class LabeledPointSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  test("LabeledPoint default weight 1.0") {
    val label = 1.0
    val features = Vectors.dense(1.0, 2.0, 3.0)
    val lp1 = LabeledPoint(label, features)
    val lp2 = LabeledPoint(label, features, weight = 1.0)
    assert(lp1 === lp2)
  }

  test("Create SchemaRDD from RDD[LabeledPoint]") {
    val sqlContext = this.sqlContext
    import sqlContext._
    val arr = Seq(
      LabeledPoint(0.0, Vectors.dense(1.0, 2.0, 3.0)),
      LabeledPoint(1.0, Vectors.dense(1.1, 2.1, 3.1)),
      LabeledPoint(0.0, Vectors.dense(1.2, 2.2, 3.2)),
      LabeledPoint(1.0, Vectors.dense(1.3, 2.3, 3.3)))
    val rdd = sc.parallelize(arr)
    val schemaRDD = rdd.select('label, 'features)
    val points = schemaRDD.collect()
    assert(points.size === arr.size)
  }
}
