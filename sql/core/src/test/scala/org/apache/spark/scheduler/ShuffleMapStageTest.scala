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

package org.apache.spark.scheduler

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class ShuffleMapStageTest extends SharedSparkSession {

  test("SPARK-51016: ShuffleMapStage using indeterministic join keys should be INDETERMINATE") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val leftDfBase = spark.createDataset(
        Seq((1L, "aa")))(
        Encoders.tuple(Encoders.scalaLong, Encoders.STRING)).toDF("pkLeftt", "strleft")

      val rightDf = spark.createDataset(
        Seq((1L, "11"), (2L, "22")))(
        Encoders.tuple(Encoders.scalaLong, Encoders.STRING)).toDF("pkRight", "strright")

      val leftDf = leftDfBase.select(
        col("strleft"), when(isnull(col("pkLeftt")), floor(rand() * Literal(10000000L)).
          cast(LongType)).
          otherwise(col("pkLeftt")).as("pkLeft"))

      val join = leftDf.hint("shuffle_hash").
        join(rightDf, col("pkLeft") === col("pkRight"), "inner")
      val shuffleStages: Array[ShuffleMapStage] = Array.ofDim(2)
      spark.sparkContext.addSparkListener(new SparkListener() {
        var i = 0
        override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
          if (stageSubmitted.stageInfo.shuffleDepId.isDefined) {
            shuffleStages(i) =
              spark.sparkContext.dagScheduler.shuffleIdToMapStage(stageSubmitted.stageInfo.stageId)
            i +=1
          }
        }
      });
      join.collect()
      assert(shuffleStages.filter(_.isIndeterminate).size == 1)
    }
  }
}
