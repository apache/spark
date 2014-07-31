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

package org.apache.spark.storage

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.PrivateMethodTester._

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import org.apache.spark._
import org.apache.spark.storage.BlockFetcherIterator._
import org.apache.spark.network.{ConnectionManager, ConnectionManagerId,
                                 Message}

class BlockFetcherIteratorSuite extends FunSuite with Matchers {

  test("block fetch from local fails using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    doReturn(connManager).when(blockManager).connectionManager
    doReturn(BlockManagerId("test-client", "test-client", 1, 0)).when(blockManager).blockManagerId

    doReturn((48 * 1024 * 1024).asInstanceOf[Long]).when(blockManager).maxBytesInFlight

    val blIds = Array[BlockId](
      ShuffleBlockId(0,0,0),
      ShuffleBlockId(0,1,0),
      ShuffleBlockId(0,2,0),
      ShuffleBlockId(0,3,0),
      ShuffleBlockId(0,4,0))

    val optItr = mock(classOf[Option[Iterator[Any]]])
    val answer = new Answer[Option[Iterator[Any]]] {
      override def answer(invocation: InvocationOnMock) = Option[Iterator[Any]] {
        throw new Exception
      }
    }

    // 3rd block is going to fail
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(1)), any())
    doAnswer(answer).when(blockManager).getLocalFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client",1 , 0)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null)

    iterator.initialize()

    // 3rd getLocalFromDisk invocation should be failed
    verify(blockManager, times(3)).getLocalFromDisk(any(), any())

    (iterator.hasNext) should be(true)
    // the 2nd element of the tuple returned by iterator.next should be defined when fetching successfully
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    // 3rd fetch should be failed
    (iterator.next._2.isDefined) should be(false)
    (iterator.hasNext) should be(true)
    // And then, all of local fetches should be failed
    (iterator.next._2.isDefined) should be(false)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(false)
  }


  test("block fetch from local succeed using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    doReturn(connManager).when(blockManager).connectionManager
    doReturn(BlockManagerId("test-client", "test-client", 1, 0)).when(blockManager).blockManagerId

    doReturn((48 * 1024 * 1024).asInstanceOf[Long]).when(blockManager).maxBytesInFlight

    val blIds = Array[BlockId](
      ShuffleBlockId(0,0,0),
      ShuffleBlockId(0,1,0),
      ShuffleBlockId(0,2,0),
      ShuffleBlockId(0,3,0),
      ShuffleBlockId(0,4,0))

    val optItr = mock(classOf[Option[Iterator[Any]]])
 
   // All blocks should be fetched successfully
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(1)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client",1 , 0)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null)

    iterator.initialize()

    // getLocalFromDis should be invoked for all of 5 blocks
    verify(blockManager, times(5)).getLocalFromDisk(any(), any())

    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
    (iterator.hasNext) should be(true)
    (iterator.next._2.isDefined) should be(true)
  }

}
