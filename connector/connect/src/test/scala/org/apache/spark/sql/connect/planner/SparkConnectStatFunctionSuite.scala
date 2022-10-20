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

package org.apache.spark.sql.connect.planner

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.connect.command.SparkConnectCommandPlanner
import org.apache.spark.sql.connect.dsl.commands._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class SparkConnectStatFunctionSuite
    extends SQLTestUtils
    with SparkConnectPlanTest
    with SharedSparkSession {

  lazy val localRelation = createLocalRelationProto(Seq($"id".int))

  def transform(cmd: proto.Command): Unit = {
    new SparkConnectCommandPlanner(spark, cmd).process()
  }

  test("test StatFunctions.summary") {
    withTempDir { f =>
      val summary1 = proto.Relation
        .newBuilder()
        .setStatFunction(
          proto.StatFunction
            .newBuilder()
            .setInput(localRelation)
            .setSummary(proto.StatFunction.Summary.newBuilder().build())
            .build())
        .build()

      transform(
        summary1
          .write(format = Some("parquet"), path = Some(f.getPath), mode = Some("Overwrite")))

      val statistics1 = spark.read
        .parquet(f.getPath)
        .select("summary")
        .collect()
        .map(_.getString(0))
        .toSet

      assert(statistics1 === Set("count", "75%", "mean", "min", "25%", "max", "stddev", "50%"))

      val summary2 = proto.Relation
        .newBuilder()
        .setStatFunction(
          proto.StatFunction
            .newBuilder()
            .setInput(localRelation)
            .setSummary(proto.StatFunction.Summary
              .newBuilder()
              .addAllStatistics(Seq("mean", "66%").asJava)
              .build())
            .build())
        .build()

      transform(
        summary2
          .write(format = Some("parquet"), path = Some(f.getPath), mode = Some("Overwrite")))

      val statistics2 = spark.read
        .parquet(f.getPath)
        .select("summary")
        .collect()
        .map(_.getString(0))
        .toSet

      assert(statistics2 === Set("mean", "66%"))
    }
  }
}
