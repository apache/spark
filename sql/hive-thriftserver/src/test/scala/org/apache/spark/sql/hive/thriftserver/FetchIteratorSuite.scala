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

  private def getRows(fetchIter: FetchIterator[Int], maxRowCount: Int): Seq[Int] = {
    for (_ <- 0 until maxRowCount if fetchIter.hasNext) yield fetchIter.next()
  }

  test("SPARK-33655: Test fetchNext and fetchPrior") {
    val testData = 0 until 10

    def iteratorTest(fetchIter: FetchIterator[Int]): Unit = {
      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 2)(getRows(fetchIter, 2))
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 2)

      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 2)
      assert(fetchIter.getPosition == 2)
      assertResult(2 until 3)(getRows(fetchIter, 1))
      assert(fetchIter.getFetchStart == 2)
      assert(fetchIter.getPosition == 3)

      fetchIter.fetchPrior(2)
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(fetchIter, 3))
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 3)

      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 3)
      assert(fetchIter.getPosition == 3)
      assertResult(3 until 8)(getRows(fetchIter, 5))
      assert(fetchIter.getFetchStart == 3)
      assert(fetchIter.getPosition == 8)

      fetchIter.fetchPrior(2)
      assert(fetchIter.getFetchStart == 1)
      assert(fetchIter.getPosition == 1)
      assertResult(1 until 4)(getRows(fetchIter, 3))
      assert(fetchIter.getFetchStart == 1)
      assert(fetchIter.getPosition == 4)

      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 4)
      assert(fetchIter.getPosition == 4)
      assertResult(4 until 10)(getRows(fetchIter, 10))
      assert(fetchIter.getFetchStart == 4)
      assert(fetchIter.getPosition == 10)

      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 10)
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(fetchIter, 10))
      assert(fetchIter.getFetchStart == 10)
      assert(fetchIter.getPosition == 10)

      fetchIter.fetchPrior(20)
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(fetchIter, 3))
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 3)
    }
    iteratorTest(new ArrayFetchIterator[Int](testData.toArray))
    iteratorTest(new IterableFetchIterator[Int](testData))
  }

  test("SPARK-33655: Test fetchAbsolute") {
    val testData = 0 until 10

    def iteratorTest(fetchIter: FetchIterator[Int]): Unit = {
      fetchIter.fetchNext()
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 5)(getRows(fetchIter, 5))
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 5)

      fetchIter.fetchAbsolute(2)
      assert(fetchIter.getFetchStart == 2)
      assert(fetchIter.getPosition == 2)
      assertResult(2 until 5)(getRows(fetchIter, 3))
      assert(fetchIter.getFetchStart == 2)
      assert(fetchIter.getPosition == 5)

      fetchIter.fetchAbsolute(7)
      assert(fetchIter.getFetchStart == 7)
      assert(fetchIter.getPosition == 7)
      assertResult(7 until 8)(getRows(fetchIter, 1))
      assert(fetchIter.getFetchStart == 7)
      assert(fetchIter.getPosition == 8)

      fetchIter.fetchAbsolute(20)
      assert(fetchIter.getFetchStart == 10)
      assert(fetchIter.getPosition == 10)
      assertResult(Seq.empty[Int])(getRows(fetchIter, 1))
      assert(fetchIter.getFetchStart == 10)
      assert(fetchIter.getPosition == 10)

      fetchIter.fetchAbsolute(0)
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 0)
      assertResult(0 until 3)(getRows(fetchIter, 3))
      assert(fetchIter.getFetchStart == 0)
      assert(fetchIter.getPosition == 3)
    }
    iteratorTest(new ArrayFetchIterator[Int](testData.toArray))
    iteratorTest(new IterableFetchIterator[Int](testData))
  }
}
