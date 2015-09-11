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

package org.apache.spark.mllib.stat.test

import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter

/**
 * :: Experimental ::
 * Performs online 2-sample significance testing for a stream of (Boolean, Double) pairs. The
 * Boolean identifies which sample each observation comes from, and the Double is the numeric value
 * of the observation.
 *
 * To address novelty affects, the `peacePeriod` specifies a set number of initial
 * [[org.apache.spark.rdd.RDD]] batches of the [[DStream]] to be dropped from significance testing.
 *
 * The `windowSize` sets the number of batches each significance test is to be performed over. The
 * window is sliding with a stride length of 1 batch. Setting windowSize to 0 will perform
 * cumulative processing, using all batches seen so far.
 *
 * Different tests may be used for assessing statistical significance depending on assumptions
 * satisfied by data. For more details, see [[StreamingTestMethod]]. The `testMethod` specifies
 * which test will be used.
 *
 * Use a builder pattern to construct a streaming test in an application, for example:
 *   ```
 *   val model = new OnlineABTest()
 *     .setPeacePeriod(10)
 *     .setWindowSize(0)
 *     .setTestMethod("welch")
 *     .registerStream(DStream)
 *   ```
 */
@Experimental
@Since("1.6.0")
class StreamingTest(
    @Since("1.6.0") var peacePeriod: Int = 0,
    @Since("1.6.0") var windowSize: Int = 0,
    @Since("1.6.0") var testMethod: StreamingTestMethod  = WelchTTest)
  extends Logging with Serializable {

  /** Set the number of initial batches to ignore. */
  @Since("1.6.0")
  def setPeacePeriod(peacePeriod: Int): this.type = {
    this.peacePeriod = peacePeriod
    this
  }

  /**
   * Set the number of batches to compute significance tests over.
   * A value of 0 will use all batches seen so far.
   */
  @Since("1.6.0")
  def setWindowSize(windowSize: Int): this.type = {
    this.windowSize = windowSize
    this
  }

  /** Set the statistical method used for significance testing. */
  @Since("1.6.0")
  def setTestMethod(method: String): this.type = {
    this.testMethod = StreamingTestMethod.getTestMethodFromName(method)
    this
  }

  /**
   * Register a [[DStream]] of values for significance testing.
   *
   * @param data stream of (key,value) pairs where the key is the group membership (control or
   *             treatment) and the value is the numerical metric to test for significance
   * @return stream of significance testing results
   */
  @Since("1.6.0")
  def registerStream(data: DStream[(Boolean, Double)]): DStream[StreamingTestResult] = {
    val dataAfterPeacePeriod = dropPeacePeriod(data)
    val summarizedData = summarizeByKeyAndWindow(dataAfterPeacePeriod)
    val pairedSummaries = pairSummaries(summarizedData)
    val testResults = testMethod.doTest(pairedSummaries)

    testResults
  }

  /** Drop all batches inside the peace period. */
  private[stat] def dropPeacePeriod(
      data: DStream[(Boolean, Double)]): DStream[(Boolean, Double)] = {
    data.transform { (rdd, time) =>
      if (time.milliseconds > data.slideDuration.milliseconds * peacePeriod) {
        rdd
      } else {
        rdd.filter(_ => false) // TODO: Is there a better way to drop a RDD from a DStream?
      }
    }
  }

  /** Compute summary statistics over each key and the specified test window size. */
  private[stat] def summarizeByKeyAndWindow(
      data: DStream[(Boolean, Double)]): DStream[(Boolean, StatCounter)] = {
    if (this.windowSize == 0) {
      data.updateStateByKey[StatCounter](
        (newValues: Seq[Double], oldSummary: Option[StatCounter]) => {
          val newSummary = oldSummary.getOrElse(new StatCounter())
          newSummary.merge(newValues)
          Some(newSummary)
        })
    } else {
      val windowDuration = data.slideDuration * this.windowSize
      data
        .groupByKeyAndWindow(windowDuration)
        .mapValues { values =>
          val summary = new StatCounter()
          values.foreach(value => summary.merge(value))
          summary
        }
    }
  }

  /**
   * Transform a stream of summaries into pairs representing summary statistics for group A and
   * group B up to this batch.
   */
  private[stat] def pairSummaries(summarizedData: DStream[(Boolean, StatCounter)])
      : DStream[(StatCounter, StatCounter)] = {
    summarizedData
      .map[(Int, StatCounter)](x => (0, x._2))
      .groupByKey()  // Iterable[StatCounter] should be length two, one for each A/B group
      .map(x => (x._2.head, x._2.last) )
  }
}
