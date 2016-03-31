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

package org.apache.spark.shuffle.sort

import java.io.{File, FileInputStream, FileOutputStream}

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage._
import org.apache.spark.util.Utils


class IndexShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  private var tempDir: File = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
    MockitoAnnotations.initMocks(this)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      new Answer[File] {
        override def answer(invocation: InvocationOnMock): File = {
          new File(tempDir, invocation.getArguments.head.toString)
        }
      })
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
  }

  test("commit shuffle files multiple times") {
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val out = new FileOutputStream(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(1, 2, lengths, dataTmp)

    val dataFile = resolver.getDataFile(1, 2)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp.exists())

    val lengths2 = new Array[Long](3)
    val dataTmp2 = File.createTempFile("shuffle", null, tempDir)
    val out2 = new FileOutputStream(dataTmp2)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(1, 2, lengths2, dataTmp2)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp2.exists())

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val in = new FileInputStream(dataFile)
    Utils.tryWithSafeFinally {
      in.read(firstByte)
    } {
      in.close()
    }
    assert(firstByte(0) === 0)

    // remove data file
    dataFile.delete()

    val lengths3 = Array[Long](10, 10, 15)
    val dataTmp3 = File.createTempFile("shuffle", null, tempDir)
    val out3 = new FileOutputStream(dataTmp3)
    Utils.tryWithSafeFinally {
      out3.write(Array[Byte](2))
      out3.write(new Array[Byte](34))
    } {
      out3.close()
    }
    resolver.writeIndexFileAndCommit(1, 2, lengths3, dataTmp3)
    assert(lengths3.toSeq != lengths.toSeq)
    assert(dataFile.exists())
    assert(dataFile.length() === 35)
    assert(!dataTmp2.exists())

    // The dataFile should be the previous one
    val firstByte2 = new Array[Byte](1)
    val in2 = new FileInputStream(dataFile)
    Utils.tryWithSafeFinally {
      in2.read(firstByte2)
    } {
      in2.close()
    }
    assert(firstByte2(0) === 2)
  }
}
