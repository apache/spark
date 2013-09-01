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

package org.apache.spark.streaming.util

import java.nio.ByteBuffer
import org.apache.spark.util.{RateLimitedOutputStream, IntParam}
import java.net.ServerSocket
import org.apache.spark.{Logging}
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream
import scala.io.Source
import java.io.IOException
import org.apache.spark.serializer.KryoSerializer

/**
 * A helper program that sends blocks of Kryo-serialized text strings out on a socket at a
 * specified rate. Used to feed data into RawInputDStream.
 */
object RawTextSender extends Logging {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: RawTextSender <port> <file> <blockSize> <bytesPerSec>")
      System.exit(1)
    }
    // Parse the arguments using a pattern match
    val Array(IntParam(port), file, IntParam(blockSize), IntParam(bytesPerSec)) = args

    // Repeat the input data multiple times to fill in a buffer
    val lines = Source.fromFile(file).getLines().toArray
    val bufferStream = new FastByteArrayOutputStream(blockSize + 1000)
    val ser = new KryoSerializer().newInstance()
    val serStream = ser.serializeStream(bufferStream)
    var i = 0
    while (bufferStream.position < blockSize) {
      serStream.writeObject(lines(i))
      i = (i + 1) % lines.length
    }
    bufferStream.trim()
    val array = bufferStream.array

    val countBuf = ByteBuffer.wrap(new Array[Byte](4))
    countBuf.putInt(array.length)
    countBuf.flip()

    val serverSocket = new ServerSocket(port)
    logInfo("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      logInfo("Got a new connection")
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
      try {
        while (true) {
          out.write(countBuf.array)
          out.write(array)
        }
      } catch {
        case e: IOException =>
          logError("Client disconnected")
          socket.close()
      }
    }
  }
}
