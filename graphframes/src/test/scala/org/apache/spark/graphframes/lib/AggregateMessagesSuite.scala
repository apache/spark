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

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils
import org.apache.spark.graphframes.examples.Graphs

import scala.collection.mutable

class AggregateMessagesSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("aggregateMessages") {
    val AM = AggregateMessages
    val g = Graphs.friends
    // For each user, sum the ages of the adjacent users,
    // plus 1 for the src's sum if the edge is "friend".
    val msgToSrc = AM.dst("age") +
      when(AM.edge("relationship") === "friend", lit(1)).otherwise(0)
    val msgToDst = AM.src("age")
    val agg = g.aggregateMessages
      .sendToSrc(msgToSrc)
      .sendToDst(msgToDst)
      .agg(sum(AM.msg).as("summedAges"))
    // Convert agg to a Map.
    import org.apache.spark.sql._
    val aggMap: Map[String, Long] = agg
      .select("id", "summedAges")
      .collect()
      .map {
        case Row(id: String, s: Long) =>
          id -> s
        case _: Row => throw new GraphFramesUnreachableException()
      }
      .toMap
    agg.unpersist()
    // Compute the truth via brute force for comparison.
    val trueAgg: Map[String, Int] = {
      val user2age = g.vertices
        .select("id", "age")
        .collect()
        .map {
          case Row(id: String, age: Int) =>
            id -> age
          case _: Row => throw new GraphFramesUnreachableException()
        }
        .toMap
      val a = mutable.HashMap.empty[String, Int]
      g.edges.select("src", "dst", "relationship").collect().foreach {
        case Row(src: String, dst: String, relationship: String) =>
          a.put(
            src,
            a.getOrElse(src, 0) + user2age(dst) + (if (relationship == "friend") 1 else 0))
          a.put(dst, a.getOrElse(dst, 0) + user2age(src))
        case _ => throw new GraphFramesUnreachableException()
      }
      a.toMap
    }
    // Compare to the true values.
    aggMap.keys.foreach { case user =>
      assert(aggMap(user) === trueAgg(user), s"Failure on user $user")
    }
    // Perform the aggregation again, this time providing the messages as Strings instead.
    val msgToSrc2 = "(dst['age'] + CASE WHEN (edge['relationship'] = 'friend') THEN 1 ELSE 0 END)"
    val msgToDst2 = "src['age']"
    val agg2 = g.aggregateMessages
      .sendToSrc(msgToSrc2)
      .sendToDst(msgToDst2)
      .agg("sum(MSG) AS `summedAges`")
    // Convert agg2 to a Map.
    val agg2Map: Map[String, Long] = agg2
      .select("id", "summedAges")
      .collect()
      .map {
        case Row(id: String, s: Long) =>
          id -> s
        case _: Row => throw new GraphFramesUnreachableException()
      }
      .toMap
    agg2.unpersist()
    // Compare to the true values.
    agg2Map.keys.foreach { case user =>
      assert(agg2Map(user) === trueAgg(user), s"Failure on user $user")
    }
  }

  test("aggregateMessages with multiple message and aggregation columns") {
    val AM = AggregateMessages
    val vertices =
      sqlContext.createDataFrame(List((1, 30), (2, 40), (3, 50), (4, 60))).toDF("id", "att1")
    val edges =
      sqlContext.createDataFrame(List((1, 2, 4), (2, 3, 5), (1, 4, 6))).toDF("src", "dst", "att2")
    val expectedValues = Map(
      1 -> Tuple2(100L, 5.0),
      2 -> Tuple2(80L, 4.5),
      3 -> Tuple2(40L, 5.0),
      4 -> Tuple2(30L, 6.0))

    val g = GraphFrame(vertices, edges)
    // aggregateMessages with column aliases
    val agg = g.aggregateMessages
      .sendToDst(AM.src("att1").as("att1"), AM.edge("att2").as("att2"))
      .sendToSrc(AM.dst("att1").as("att1"), AM.edge("att2").as("att2"))
      .agg(sum(AM.msg("att1")).as("sum_att1"), avg(AM.msg("att2")).as("avg_att2"))

    // aggregateMessages with columns and no aliases
    val agg2 = g.aggregateMessages
      .sendToDst(AM.src("att1"), AM.edge("att2"))
      .sendToSrc(AM.dst("att1"), AM.edge("att2"))
      .agg(sum(AM.msg("att1")).as("sum_att1"), avg(AM.msg("att2")).as("avg_att2"))

    // aggregateMessages with column expressions
    val agg3 = g.aggregateMessages
      .sendToDst("src['att1'] as att1", "edge['att2'] as att2")
      .sendToSrc("dst['att1'] as att1", "edge['att2'] as att2")
      .agg("sum(MSG['att1']) AS sum_att1", "avg(MSG['att2']) AS avg_att2")

    // validate schema
    assert(agg.schema.size === 3)
    TestUtils.checkColumnType(agg.schema, "id", IntegerType)
    TestUtils.checkColumnType(agg.schema, "sum_att1", LongType)
    TestUtils.checkColumnType(agg.schema, "avg_att2", DoubleType)

    assert(agg.schema === agg2.schema)
    assert(agg.schema === agg3.schema)

    // validate content
    val output1 = agg
      .collect()
      .map {
        case Row(id: Int, sumAtt1: Long, avgAtt2: Double) =>
          id -> Tuple2(sumAtt1, avgAtt2)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    val output2 = agg2
      .collect()
      .map {
        case Row(id: Int, sumAtt1: Long, avgAtt2: Double) =>
          id -> Tuple2(sumAtt1, avgAtt2)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    val output3 = agg3
      .collect()
      .map {
        case Row(id: Int, sumAtt1: Long, avgAtt2: Double) =>
          id -> Tuple2(sumAtt1, avgAtt2)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(output1 === expectedValues)
    assert(output2 === expectedValues)
    assert(output3 === expectedValues)
    agg.unpersist()
    agg2.unpersist()
    agg3.unpersist()
  }
}
