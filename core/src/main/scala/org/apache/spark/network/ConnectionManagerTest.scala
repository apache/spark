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

package org.apache.spark.network

import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.io.Source

import java.nio.ByteBuffer
import java.net.InetAddress

import scala.concurrent.Await
import scala.concurrent.duration._

private[spark] object ConnectionManagerTest extends Logging{
  def main(args: Array[String]) {
    // <mesos cluster> - the master URL <slaves file> - a list slaves to run connectionTest on
    // [num of tasks] - the number of parallel tasks to be initiated default is number of slave
    // hosts [size of msg in MB (integer)] - the size of messages to be sent in each task,
    // default is 10 [count] - how many times to run, default is 3 [await time in seconds] :
    // await time (in seconds), default is 600
    if (args.length < 2) {
      println("Usage: ConnectionManagerTest <mesos cluster> <slaves file> [num of tasks] " +
        "[size of msg in MB (integer)] [count] [await time in seconds)] ")
      System.exit(1)
    }
    
    if (args(0).startsWith("local")) {
      println("This runs only on a mesos cluster")
    }
    
    val sc = new SparkContext(args(0), "ConnectionManagerTest")
    val slavesFile = Source.fromFile(args(1))
    val slaves = slavesFile.mkString.split("\n")
    slavesFile.close()

    /*println("Slaves")*/
    /*slaves.foreach(println)*/
    val tasknum = if (args.length > 2) args(2).toInt else slaves.length
    val size = ( if (args.length > 3) (args(3).toInt) else 10 ) * 1024 * 1024 
    val count = if (args.length > 4) args(4).toInt else 3
    val awaitTime = (if (args.length > 5) args(5).toInt else 600 ).second
    println("Running " + count + " rounds of test: " + "parallel tasks = " + tasknum + ", " +
      "msg size = " + size/1024/1024 + " MB, awaitTime = " + awaitTime)
    val slaveConnManagerIds = sc.parallelize(0 until tasknum, tasknum).map(
        i => SparkEnv.get.connectionManager.id).collect()
    println("\nSlave ConnectionManagerIds")
    slaveConnManagerIds.foreach(println)
    println

    (0 until count).foreach(i => {
      val resultStrs = sc.parallelize(0 until tasknum, tasknum).map(i => {
        val connManager = SparkEnv.get.connectionManager
        val thisConnManagerId = connManager.id 
        connManager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => { 
          logInfo("Received [" + msg + "] from [" + id + "]")
          None
        })

        val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
        buffer.flip
        
        val startTime = System.currentTimeMillis  
        val futures = slaveConnManagerIds.filter(_ != thisConnManagerId).map{ slaveConnManagerId =>
          {
            val bufferMessage = Message.createBufferMessage(buffer.duplicate)
            logInfo("Sending [" + bufferMessage + "] to [" + slaveConnManagerId + "]")
            connManager.sendMessageReliably(slaveConnManagerId, bufferMessage)
          }
        }
        val results = futures.map(f => Await.result(f, awaitTime))
        val finishTime = System.currentTimeMillis
        Thread.sleep(5000)
        
        val mb = size * results.size / 1024.0 / 1024.0
        val ms = finishTime - startTime
        val resultStr = thisConnManagerId + " Sent " + mb + " MB in " + ms + " ms at " + (mb / ms *
          1000.0) + " MB/s"
        logInfo(resultStr)
        resultStr
      }).collect()
      
      println("---------------------") 
      println("Run " + i) 
      resultStrs.foreach(println)
      println("---------------------") 
    })
  }
}

