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

package org.apache.spark.mllib.regression

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.feature.{LabeledPoint => NewLabeledPoint}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer

class LabeledPointSuite extends SparkFunSuite {

  test("parse labeled points") {
    val points = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))
    points.foreach { p =>
      assert(p === LabeledPoint.parse(p.toString))
    }
  }

  test("parse labeled points with whitespaces") {
    val point = LabeledPoint.parse("(0.0, [1.0, 2.0])")
    assert(point === LabeledPoint(0.0, Vectors.dense(1.0, 2.0)))
  }

  test("parse labeled points with v0.9 format") {
    val point = LabeledPoint.parse("1.0,1.0 0.0 -2.0")
    assert(point === LabeledPoint(1.0, Vectors.dense(1.0, 0.0, -2.0)))
  }

  test("conversions between new ml LabeledPoint and mllib LabeledPoint") {
    val points: Seq[LabeledPoint] = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))

    val newPoints: Seq[NewLabeledPoint] = points.map(_.asML)

    points.zip(newPoints).foreach { case (p1, p2) =>
      assert(p1 === LabeledPoint.fromML(p2))
    }
  }

  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val labeled1 = LabeledPoint(1.0, Vectors.dense(Array(1.0, 2.0)))
    val labeled2 = LabeledPoint(1.0, Vectors.sparse(10, Array(5, 7), Array(1.0, 2.0)))

    Seq(labeled1, labeled2).foreach { l =>
      val l2 = ser.deserialize[LabeledPoint](ser.serialize(l))
      assert(l === l2)
    }
  }
}
