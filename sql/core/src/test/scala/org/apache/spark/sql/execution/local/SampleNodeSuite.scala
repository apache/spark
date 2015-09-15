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

package org.apache.spark.sql.execution.local

class SampleNodeSuite extends LocalNodeTest {

  import testImplicits._

  private def testSample(withReplacement: Boolean): Unit = {
    test(s"withReplacement: $withReplacement") {
      val seed = 0L
      val input = sqlContext.sparkContext.
        parallelize((1 to 10).map(i => (i, i.toString)), 1). // Should be only 1 partition
        toDF("key", "value")
      checkAnswer(
        input,
        node => SampleNode(conf, 0.0, 0.3, withReplacement, seed, node),
        input.sample(withReplacement, 0.3, seed).collect()
      )
    }
  }

  testSample(withReplacement = true)
  testSample(withReplacement = false)
}
