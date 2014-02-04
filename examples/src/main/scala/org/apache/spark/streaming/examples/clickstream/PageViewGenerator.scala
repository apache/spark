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

package org.apache.spark.streaming.examples.clickstream

import java.net.ServerSocket
import java.io.PrintWriter
import util.Random

/** Represents a page view on a website with associated dimension data.*/
class PageView(val url : String, val status : Int, val zipCode : Int, val userID : Int)
    extends Serializable {
  override def toString() : String = {
    "%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID)
  }
}

object PageView extends Serializable {
  def fromString(in : String) : PageView = {
    val parts = in.split("\t")
    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
  }
}

/** Generates streaming events to simulate page views on a website.
  *
  * This should be used in tandem with PageViewStream.scala. Example:
  * $ ./bin/run-example org.apache.spark.streaming.examples.clickstream.PageViewGenerator 44444 10
  * $ ./bin/run-example org.apache.spark.streaming.examples.clickstream.PageViewStream errorRatePerZipCode localhost 44444
  *
  * When running this, you may want to set the root logging level to ERROR in
  * conf/log4j.properties to reduce the verbosity of the output.
  * */
object PageViewGenerator {
  val pages = Map("http://foo.com/"        -> .7,
                  "http://foo.com/news"    -> 0.2,
                  "http://foo.com/contact" -> .1)
  val httpStatus = Map(200 -> .95,
                       404 -> .05)
  val userZipCode = Map(94709 -> .5,
                        94117 -> .5)
  val userID = Map((1 to 100).map(_ -> .01):_*)


  def pickFromDistribution[T](inputMap : Map[T, Double]) : T = {
    val rand = new Random().nextDouble()
    var total = 0.0
    for ((item, prob) <- inputMap) {
      total = total + prob
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1 // Shouldn't get here if probabilities add up to 1.0
  }

  def getNextClickEvent() : String = {
    val id = pickFromDistribution(userID)
    val page = pickFromDistribution(pages)
    val status = pickFromDistribution(httpStatus)
    val zipCode = pickFromDistribution(userZipCode)
    new PageView(page, status, zipCode, id).toString()
  }

  def main(args : Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PageViewGenerator <port> <viewsPerSecond>")
      System.exit(1)
    }
    val port = args(0).toInt
    val viewsPerSecond = args(1).toFloat
    val sleepDelayMs = (1000.0 / viewsPerSecond).toInt
    val listener = new ServerSocket(port)
    println("Listening on port: " + port)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)
            out.write(getNextClickEvent())
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
