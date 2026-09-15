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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils
import org.apache.spark.graphframes.examples.Graphs
import org.apache.spark.sql.types.DataTypes

class LabelPropagationSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  val n = 5

  test("Toy example") {
    val g = Graphs.twoBlobs(n)
    val labels = g.labelPropagation.maxIter(4 * n).run()
    TestUtils.testSchemaInvariants(g, labels)
    TestUtils.checkColumnType(labels.schema, "label", DataTypes.LongType)
    val clique1 =
      labels.filter(s"id < $n").select("label").collect().toSeq.map(_.getLong(0)).toSet
    assert(clique1.size === 1)
    val clique2 =
      labels.filter(s"id >= $n").select("label").collect().toSeq.map(_.getLong(0)).toSet
    assert(clique2.size === 1)
    assert(clique1 !== clique2)
    labels.unpersist()
  }
}
