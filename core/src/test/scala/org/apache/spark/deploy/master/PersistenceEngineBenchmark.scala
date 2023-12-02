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

package org.apache.spark.deploy.master

import java.net.ServerSocket
import java.util.concurrent.ThreadLocalRandom

import org.apache.curator.test.TestingServer

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.Deploy.ZOOKEEPER_URL
import org.apache.spark.io.CompressionCodec
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.util.Utils


/**
 * Benchmark for PersistenceEngines.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars `find ~/.cache/coursier \
 *        -name 'curator-test-*.jar'` <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/PersistenceEngineBenchmark-results.txt".
 * }}}
 * */
object PersistenceEngineBenchmark extends BenchmarkBase {

  val conf = new SparkConf()
  val serializers = Seq(new JavaSerializer(conf), new KryoSerializer(conf))
  val zkTestServer = new TestingServer(findFreePort(conf))

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val numIters = 3
    val numWorkers = 2000
    val workers = (1 to numWorkers).map(createWorkerInfo).toArray

    conf.set(ZOOKEEPER_URL, zkTestServer.getConnectString)

    runBenchmark("PersistenceEngineBenchmark") {
      val benchmark = new Benchmark(s"$numWorkers Workers", numWorkers, output = output)

      serializers.foreach { serializer =>
        val serializerName = serializer.getClass.getSimpleName
        benchmark.addCase(s"ZooKeeperPersistenceEngine with $serializerName", numIters) { _ =>
          val engine = new ZooKeeperPersistenceEngine(conf, serializer)
          workers.foreach(engine.addWorker)
          engine.read[WorkerInfo]("worker_")
          workers.foreach(engine.removeWorker)
          engine.close()
        }
      }

      serializers.foreach { serializer =>
        val serializerName = serializer.getClass.getSimpleName
        val name = s"FileSystemPersistenceEngine with $serializerName"
        benchmark.addCase(name, numIters) { _ =>
          val dir = Utils.createTempDir().getAbsolutePath
          val engine = new FileSystemPersistenceEngine(dir, serializer)
          workers.foreach(engine.addWorker)
          engine.read[WorkerInfo]("worker_")
          workers.foreach(engine.removeWorker)
          engine.close()
        }
        CompressionCodec.ALL_COMPRESSION_CODECS.foreach { c =>
          val codec = CompressionCodec.createCodec(conf, c)
          val shortCodecName = CompressionCodec.getShortName(c)
          val name = s"FileSystemPersistenceEngine with $serializerName ($shortCodecName)"
          benchmark.addCase(name, numIters) { _ =>
            val dir = Utils.createTempDir().getAbsolutePath
            val engine = new FileSystemPersistenceEngine(dir, serializer, Some(codec))
            workers.foreach(engine.addWorker)
            engine.read[WorkerInfo]("worker_")
            workers.foreach(engine.removeWorker)
            engine.close()
          }
        }
      }

      benchmark.addCase("BlackHolePersistenceEngine", numIters) { _ =>
        val engine = new BlackHolePersistenceEngine()
        workers.foreach(engine.addWorker)
        engine.read[WorkerInfo]("worker_")
        workers.foreach(engine.removeWorker)
        engine.close()
      }

      benchmark.run()
    }
  }

  override def afterAll(): Unit = {
    zkTestServer.stop()
  }

  private def createWorkerInfo(id: Int): WorkerInfo = {
    val gpuResource = new WorkerResourceInfo(GPU, Seq("0", "1", "2"))
    val fpgaResource = new WorkerResourceInfo(FPGA, Seq("3", "4", "5"))
    val resources = Map(GPU -> gpuResource, FPGA -> fpgaResource)
    val workerInfo = new WorkerInfo(s"worker-20231201000000-255.255.255.255-$id", "host", 8080, 4,
      1234, null, "http://publicAddress:80", resources)
    workerInfo.lastHeartbeat = System.currentTimeMillis()
    workerInfo
  }

  def findFreePort(conf: SparkConf): Int = {
    val candidatePort = ThreadLocalRandom.current().nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }
}
