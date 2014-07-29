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

package org.apache.spark.sql.hive

import scala.reflect.ClassTag

import org.apache.spark.sql.{SQLConf, QueryTest}
import org.apache.spark.sql.execution.{BroadcastHashJoin, ShuffledHashJoin}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._

class StatisticsSuite extends QueryTest {

  test("estimates the size of a test MetastoreRelation") {
    val rdd = hql("""SELECT * FROM src""")
    val sizes = rdd.queryExecution.analyzed.collect { case mr: MetastoreRelation =>
      mr.statistics.sizeInBytes
    }
    assert(sizes.size === 1)
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Any],
        ct: ClassTag[_]) = {
      before()

      var rdd = hql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = rdd.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.statistics.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = rdd.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${rdd.queryExecution}")

      checkAnswer(rdd, expectedAnswer) // check correctness of output

      TestHive.settings.synchronized {
        val tmp = autoBroadcastJoinThreshold

        hql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD}=-1""")
        rdd = hql(query)
        bhj = rdd.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = rdd.queryExecution.sparkPlan.collect { case j: ShuffledHashJoin => j }
        assert(shj.size === 1,
          "ShuffledHashJoin should be planned when BroadcastHashJoin is turned off")

        hql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD}=$tmp""")
      }

      after()
    }

    /** Tests for MetastoreRelation */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)((238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[MetastoreRelation]]
    )
  }

}
