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

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils
import org.apache.spark.graphframes.examples.Graphs
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes

class SVDPlusPlusSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Test SVD++ with mean square error on training set") {
    val svdppErr = 8.0
    val g = Graphs.ALSSyntheticData()

    val v2 = g.svdPlusPlus.maxIter(2).run()
    TestUtils.testSchemaInvariants(g, v2)
    Seq(SVDPlusPlus.COLUMN1, SVDPlusPlus.COLUMN2).foreach { case c =>
      TestUtils.checkColumnType(
        v2.schema,
        c,
        DataTypes.createArrayType(DataTypes.DoubleType, false))
    }
    Seq(SVDPlusPlus.COLUMN3, SVDPlusPlus.COLUMN4).foreach { case c =>
      TestUtils.checkColumnType(v2.schema, c, DataTypes.DoubleType)
    }
    val err = v2
      .select(GraphFrame.ID, SVDPlusPlus.COLUMN4)
      .rdd
      .map {
        case Row(vid: Long, vd: Double) =>
          if (vid % 2 == 1) vd else 0.0
        case _ => throw new GraphFramesUnreachableException()
      }
      .reduce(_ + _) / g.edges.count()
    assert(err <= svdppErr)
    v2.unpersist()
  }

  Seq(
    ("int", "float"),
    ("short", "double"),
    ("long", "float"),
    ("byte", "double"),
    ("string", "float")).foreach(types =>
    test(s"Test SVD++ with mean square error on training set, ${types._1}/${types._2} types") {
      val svdppErr = 8.0
      val g = {
        val gg = Graphs.ALSSyntheticData()
        GraphFrame(
          gg.vertices.select(col(GraphFrame.ID).cast(types._1)),
          gg.edges.select(
            col(GraphFrame.SRC).cast(types._1),
            col(GraphFrame.DST).cast(types._1),
            col("weight").cast(types._2)))
      }

      val v2 = g.svdPlusPlus.maxIter(2).run()
      TestUtils.testSchemaInvariants(g, v2)
      Seq(SVDPlusPlus.COLUMN1, SVDPlusPlus.COLUMN2).foreach { case c =>
        TestUtils.checkColumnType(
          v2.schema,
          c,
          DataTypes.createArrayType(DataTypes.DoubleType, false))
      }
      Seq(SVDPlusPlus.COLUMN3, SVDPlusPlus.COLUMN4).foreach { case c =>
        TestUtils.checkColumnType(v2.schema, c, DataTypes.DoubleType)
      }
      val err = v2
        .select(GraphFrame.ID, SVDPlusPlus.COLUMN4)
        .rdd
        .map { row =>
          {
            val vid = if (types._1 == "string") { row.getAs[String](0).toLong }
            else { row.getAs[Number](0).longValue() }
            val vd = row.getAs[Number](1).doubleValue()
            if (vid % 2 == 1) vd else 0.0
          }
        }
        .reduce(_ + _) / g.edges.count()
      assert(err <= svdppErr)
      v2.unpersist()
    })
}
