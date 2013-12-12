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

import akka.actor._

import java.util.concurrent.ArrayBlockingQueue
import util.Random
import org.apache.spark.serializer.KryoSerializer

/**
 * This class tests the BlockManager and MemoryStore for thread safety and
 * deadlocks. It spawns a number of producer and consumer threads. Producer
 * threads continuously pushes blocks into the BlockManager and consumer
 * threads continuously retrieves the blocks form the BlockManager and tests
 * whether the block is correct or not.
 */
private[spark] object ThreadingTest {

  val numProducers = 5
  val numBlocksPerProducer = 20000

  private[spark] class ProducerThread(manager: BlockManager, id: Int) extends Thread {
    val queue = new ArrayBlockingQueue[(BlockId, Seq[Int])](100)

    override def run() {
      for (i <- 1 to numBlocksPerProducer) {
        val blockId = TestBlockId("b-" + id + "-" + i)
        val blockSize = Random.nextInt(1000)
        val block = (1 to blockSize).map(_ => Random.nextInt())
        val level = randomLevel()
        val startTime = System.currentTimeMillis()
        manager.put(blockId, block.iterator, level, true)
        println("Pushed block " + blockId + " in " + (System.currentTimeMillis - startTime) + " ms")
        queue.add((blockId, block))
      }
      println("Producer thread " + id + " terminated")
    }

    def randomLevel(): StorageLevel = {
      math.abs(Random.nextInt()) % 4 match {
        case 0 => StorageLevel.MEMORY_ONLY
        case 1 => StorageLevel.MEMORY_ONLY_SER
        case 2 => StorageLevel.MEMORY_AND_DISK
        case 3 => StorageLevel.MEMORY_AND_DISK_SER
      }
    }
  }

  private[spark] class ConsumerThread(
      manager: BlockManager,
      queue: ArrayBlockingQueue[(BlockId, Seq[Int])]
    ) extends Thread {
    var numBlockConsumed = 0

    override def run() {
      println("Consumer thread started")
      while(numBlockConsumed < numBlocksPerProducer) {
        val (blockId, block) = queue.take()
        val startTime = System.currentTimeMillis()
        manager.get(blockId) match {
          case Some(retrievedBlock) =>
            assert(retrievedBlock.toList.asInstanceOf[List[Int]] == block.toList,
              "Block " + blockId + " did not match")
            println("Got block " + blockId + " in " +
              (System.currentTimeMillis - startTime) + " ms")
          case None =>
            assert(false, "Block " + blockId + " could not be retrieved")
        }
        numBlockConsumed += 1
      }
      println("Consumer thread terminated")
    }
  }

  def main(args: Array[String]) {
    System.setProperty("spark.kryoserializer.buffer.mb", "1")
    val actorSystem = ActorSystem("test")
    val serializer = new KryoSerializer
    val blockManagerMaster = new BlockManagerMaster(
      Left(actorSystem.actorOf(Props(new BlockManagerMasterActor(true)))))
    val blockManager = new BlockManager(
      "<driver>", actorSystem, blockManagerMaster, serializer, 1024 * 1024)
    val producers = (1 to numProducers).map(i => new ProducerThread(blockManager, i))
    val consumers = producers.map(p => new ConsumerThread(blockManager, p.queue))
    producers.foreach(_.start)
    consumers.foreach(_.start)
    producers.foreach(_.join)
    consumers.foreach(_.join)
    blockManager.stop()
    blockManagerMaster.stop()
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    println("Everything stopped.")
    println(
      "It will take sometime for the JVM to clean all temporary files and shutdown. Sit tight.")
  }
}
