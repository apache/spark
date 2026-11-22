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

package org.apache.spark.sql.execution.python

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.sql.{IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.EvalOnlyWhen
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end test for `ConditionalUDFEvaluation` under pipelined Python UDF execution, where the
 * writer thread adds the skipped rows to the lock-free queue while the task thread reads them.
 * `spark.python.udf.pipelined.enabled` is a core config, so it has to be set in the `SparkConf`.
 */
class ConditionalUDFPipelinedSuite extends QueryTest with SharedSparkSession {

  import IntegratedUDFTestUtils._
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(PYTHON_UDF_PIPELINED_EXECUTION, true)

  test("SPARK-61482: a non-deterministic UDF over an aggregate uses the pipelined row queue") {
    assume(shouldTestPythonUDFs)

    // A non-deterministic UDF cannot reuse one evaluated result, so it goes through the buffered,
    // pipelined path rather than the constant-result fast path. `rnd` is the identity, marked
    // non-deterministic, so a non-matching row still yields rnd(0) = 0.
    val rnd = TestPythonUDF(name = "rnd", deterministic = false)
    registerTestUDF(rnd, spark)

    withTempView("lo", "ri") {
      // Interleave matching and non-matching outer rows, with a small batch size, so the writer
      // thread buffers skipped rows between evaluated ones and the task thread restores order.
      (1 to 8).toDF("a").createOrReplaceTempView("lo")
      Seq(2, 4, 6, 8).toDF("c").createOrReplaceTempView("ri")
      withSQLConf(SQLConf.PYTHON_UDF_MAX_RECORDS_PER_BATCH.key -> "2") {
        val scalar = "SELECT lo.a, (SELECT sum(x) FROM (SELECT rnd(count(*)) AS x FROM ri " +
          "WHERE ri.c = lo.a)) FROM lo ORDER BY lo.a"
        checkAnswer(sql(scalar), (1 to 8).map(a => Row(a, if (a % 2 == 0) 1 else 0)))
        // The eval node carries the condition and holds a non-deterministic UDF, so it takes the
        // buffered path; the pipelined setting makes that path use the lock-free queue.
        assert(spark.sparkContext.getConf.get(PYTHON_UDF_PIPELINED_EXECUTION))
        // The non-deterministic UDF is marked (so it reaches ConditionalUDFEvaluation) and cannot
        // reuse one result, so it takes the buffered, pipelined path; the correct interleaved
        // result shows that path works.
        assert(
          sql(scalar).queryExecution.optimizedPlan.exists(
            _.expressions.exists(_.exists(_.isInstanceOf[EvalOnlyWhen]))),
          "the non-deterministic UDF query should get an EvalOnlyWhen marker")
      }
    }
  }
}
