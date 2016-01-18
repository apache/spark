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

package org.apache.spark


// This is moved to its own file because many more things will be added to it in SPARK-10620.
private[spark] object InternalAccumulator {
  val PEAK_EXECUTION_MEMORY = "peakExecutionMemory"
  val TEST_ACCUMULATOR = "testAccumulator"

  // For testing only.
  // This needs to be a def since we don't want to reuse the same accumulator across stages.
  private def maybeTestAccumulator: Option[Accumulator[Long]] = {
    if (sys.props.contains("spark.testing")) {
      Some(new Accumulator(
        0L, AccumulatorParam.LongAccumulatorParam, Some(TEST_ACCUMULATOR), internal = true))
    } else {
      None
    }
  }

  /**
   * Accumulators for tracking internal metrics.
   *
   * These accumulators are created with the stage such that all tasks in the stage will
   * add to the same set of accumulators. We do this to report the distribution of accumulator
   * values across all tasks within each stage.
   */
  def create(sc: SparkContext): Seq[Accumulator[Long]] = {
    val internalAccumulators = Seq(
      // Execution memory refers to the memory used by internal data structures created
      // during shuffles, aggregations and joins. The value of this accumulator should be
      // approximately the sum of the peak sizes across all such data structures created
      // in this task. For SQL jobs, this only tracks all unsafe operators and ExternalSort.
      new Accumulator(
        0L, AccumulatorParam.LongAccumulatorParam, Some(PEAK_EXECUTION_MEMORY), internal = true)
    ) ++ maybeTestAccumulator.toSeq
    internalAccumulators.foreach { accumulator =>
      sc.cleaner.foreach(_.registerAccumulatorForCleanup(accumulator))
    }
    internalAccumulators
  }
}
