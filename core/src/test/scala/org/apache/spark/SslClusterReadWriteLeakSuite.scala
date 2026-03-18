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

import java.util.concurrent.atomic.AtomicInteger

import _root_.io.netty.buffer.UnpooledByteBufAllocator
import _root_.io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}

import org.apache.spark.util.SslTestUtils

/**
 * Integration test verifying that the SslMessageEncoder memory-leak fix holds under a realistic
 * cluster workload with TLS enabled.
 *
 * The test installs a custom Netty ResourceLeakDetector in PARANOID mode before the SparkContext
 * starts, runs a shuffle with payloads large enough to require multi-record TLS framing (> 16 KB),
 * then forces GC and asserts that the detector reported zero leaks.
 */
class SslClusterReadWriteLeakSuite extends SparkFunSuite with LocalSparkContext {

  private val reportedLeaks = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    installLeakCountingDetector()
  }

  override def afterAll(): Unit = {
    try {
      ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED)
    } finally {
      super.afterAll()
    }
  }

  test("SSL shuffle with large data produces no ByteBuf memory leaks") {
    val myConf = new SparkConf()
      .setMaster("local-cluster[2,1,1024]")
      .setAppName("ssl-leak-test")
    SslTestUtils.updateWithSSLConfig(myConf)

    sc = new SparkContext(myConf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    // Payloads > 16 KB force multi-record TLS framing, exercising the code path
    // where SslMessageEncoder.encode() previously leaked direct ByteBufs.
    val numElements = 500
    val largeValue = "x" * (32 * 1024) // 32 KB per element

    val rdd = sc.parallelize(1 to numElements, 4).map(i => (i % 10, largeValue))
    val result = rdd.reduceByKey(_ + _).collect()
    assert(result.length === 10)

    // Stop the context to flush all in-flight network traffic.
    resetSparkContext()

    // Trigger GC and make Netty poll its PhantomReference queue.
    triggerLeakDetection()

    assert(reportedLeaks.get() === 0, s"Detected ${reportedLeaks.get()} ByteBuf leak(s)")
  }

  /**
   * Installs a custom ResourceLeakDetectorFactory whose detectors override
   * reportTracedLeak/reportUntracedLeak to count every leak report.
   * Must be called before any ByteBuf is allocated so that AbstractByteBuf.leakDetector
   * (static final) is initialised with our instance.
   */
  private def installLeakCountingDetector(): Unit = {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    val counter = reportedLeaks
    ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
      new ResourceLeakDetectorFactory() {
        override def newResourceLeakDetector[T](
            resource: Class[T],
            samplingInterval: Int,
            maxActive: Long): ResourceLeakDetector[T] = {
          new ResourceLeakDetector[T](resource, samplingInterval) {
            override protected def reportTracedLeak(
                resourceType: String,
                records: String): Unit = {
              super.reportTracedLeak(resourceType, records)
              counter.incrementAndGet()
            }
            override protected def reportUntracedLeak(resourceType: String): Unit = {
              super.reportUntracedLeak(resourceType)
              counter.incrementAndGet()
            }
          }
        }
      })
  }

  /**
   * Forces several rounds of GC and allocates direct buffers in between so that Netty's
   * ResourceLeakDetector (PARANOID mode) polls its PhantomReference queue and reports any
   * buffers that were GC'd without being released.
   */
  private def triggerLeakDetection(): Unit = {
    for (_ <- 1 to 5) {
      System.gc()
      System.runFinalization()
      Thread.sleep(500)
      // Each directBuffer() allocation causes the detector to drain its ref queue.
      val bufs = (1 to 200).map(_ => UnpooledByteBufAllocator.DEFAULT.directBuffer(1))
      bufs.foreach(_.release())
      Thread.sleep(200)
    }
  }
}
