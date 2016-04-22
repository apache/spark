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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This is a test for SPARK-7727 if the Optimizer is kept being extendable
 */
class OptimizerExtendableSuite extends SparkFunSuite {

  /**
   * Dummy rule for test batches
   */
  object DummyRule extends Rule[LogicalPlan] {
    def apply(p: LogicalPlan): LogicalPlan = p
  }

  /**
   * This class represents a dummy extended optimizer that takes the batches of the
   * Optimizer and adds custom ones.
   */
  class ExtendedOptimizer extends SimpleTestOptimizer {

    // rules set to DummyRule, would not be executed anyways
    val myBatches: Seq[Batch] = {
      Batch("once", Once,
        DummyRule) ::
      Batch("fixedPoint", FixedPoint(100),
        DummyRule) :: Nil
    }

    override def batches: Seq[Batch] = super.batches ++ myBatches
  }

  test("Extending batches possible") {
    // test simply instantiates the new extended optimizer
    val extendedOptimizer = new ExtendedOptimizer()
  }
}
