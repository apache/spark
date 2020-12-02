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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.SparkFunSuite

class FetchIteratorSuite extends SparkFunSuite {

  test("Test setRelativePosition, setAbsolutePosition, fetchNext FetchIterator") {
    val testData = 0 until 10

    def iteratorTest(fetchIter: FetchIterator[Int]): Unit = {
      def getRows(maxRowCount: Int): Seq[Int] = {
        for (_ <- 0 until maxRowCount if fetchIter.hasNext) yield fetchIter.next()
      }

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(3))

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 3)
      assertResult(3 until 6)(getRows(3))

      fetchIter.setRelativePosition(-2)
      assert(fetchIter.getPosition == 1)
      assertResult(1 until 4)(getRows(3))

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 4)
      assertResult(4 until 10)(getRows(10))

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(1))

      fetchIter.setRelativePosition(-3)
      assert(fetchIter.getPosition == 7)
      assertResult(7 until 10)(getRows(3))

      fetchIter.setAbsolutePosition(0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 1)(getRows(1))

      fetchIter.setAbsolutePosition(20)
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(1))

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(1))

      fetchIter.setRelativePosition(3)
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(1))

      fetchIter.setRelativePosition(-20)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(3))

      fetchIter.setAbsolutePosition(-20)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(3))

      fetchIter.fetchNext()
      assert(fetchIter.getPosition == 3)
      assertResult(3 until 10)(getRows(10))
    }

    iteratorTest(new ArrayFetchIterator[Int](testData.toArray))
    iteratorTest(new IterableFetchIterator[Int](testData))
  }
}
