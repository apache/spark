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

package org.apache.spark.api.python

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.util

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_ENABLED
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.security.{SocketAuthHelper, SocketAuthServer}
import org.apache.spark.util.Utils

class PythonRDDSuite extends SparkFunSuite with LocalSparkContext {

  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  test("Writing large strings to the worker") {
    val input: List[String] = List("a"*100000)
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    PythonRDD.writeIteratorToStream(input.iterator, buffer)
  }

  test("Handle nulls gracefully") {
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    // Should not have NPE when write an Iterator with null in it
    // The correctness will be tested in Python
    PythonRDD.writeIteratorToStream(Iterator("a", null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a"), buffer)
    PythonRDD.writeIteratorToStream(Iterator("a".getBytes(StandardCharsets.UTF_8), null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a".getBytes(StandardCharsets.UTF_8)), buffer)
    PythonRDD.writeIteratorToStream(Iterator((null, null), ("a", null), (null, "b")), buffer)
    PythonRDD.writeIteratorToStream(Iterator(
      (null, null),
      ("a".getBytes(StandardCharsets.UTF_8), null),
      (null, "b".getBytes(StandardCharsets.UTF_8))), buffer)
  }

  test("python server error handling") {
    val conf = new SparkConf()
    conf.set(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED.key, false.toString)
    val authHelper = new SocketAuthHelper(conf)
    val errorServer = new ExceptionPythonServer(authHelper)
    val socketChannel = SocketChannel.open(
      new InetSocketAddress(InetAddress.getLoopbackAddress(),
        errorServer.connInfo.asInstanceOf[Int]))
    authHelper.authToServer(socketChannel)
    val ex = intercept[Exception] { errorServer.getResult(Duration(1, "second")) }
    assert(ex.getCause().getMessage().contains("exception within handleConnection"))
  }

  class ExceptionPythonServer(authHelper: SocketAuthHelper)
      extends SocketAuthServer[Unit](authHelper, "error-server") {

    override def handleConnection(sock: SocketChannel): Unit = {
      throw new Exception("exception within handleConnection")
    }
  }

  test("mapToConf should not load defaults") {
    val map = Map("key" -> "value")
    val conf = PythonHadoopUtil.mapToConf(map.asJava)
    assert(conf.size() === map.size)
    assert(conf.get("key") === map("key"))
  }

  test("SparkContext's hadoop configuration should be respected in PythonRDD") {
    // hadoop conf with default configurations
    val hadoopConf = new Configuration()
    assert(hadoopConf.size() > 0)
    val headEntry = hadoopConf.asScala.head
    val (firstKey, firstValue) = (headEntry.getKey, headEntry.getValue)

    // passed to spark conf with a different value(prefixed by spark.)
    val conf = new SparkConf().setAppName("test").setMaster("local")
    conf.set("spark.hadoop." + firstKey, "spark." + firstValue)

    sc = new SparkContext(conf)
    val outDir = new File(tempDir, "output").getAbsolutePath
    // write output as HadoopRDD's input
    sc.makeRDD(1 to 1000, 10).saveAsTextFile(outDir)

    val javaSparkContext = new JavaSparkContext(sc)
    val confMap = new util.HashMap[String, String]()
    // set input path in job conf
    confMap.put(FileInputFormat.INPUT_DIR, outDir)

    val pythonRDD = PythonRDD.hadoopRDD(
      javaSparkContext,
      classOf[TextInputFormat].getCanonicalName,
      classOf[LongWritable].getCanonicalName,
      classOf[Text].getCanonicalName,
      null,
      null,
      confMap,
      0
    )

    @tailrec
    def getRootRDD(rdd: RDD[_]): RDD[_] = {
      rdd.dependencies match {
       case Nil => rdd
       case dependency :: _ => getRootRDD(dependency.rdd)
      }
    }

    // retrieve hadoopRDD as it's a root RDD
    val hadoopRDD = getRootRDD(pythonRDD).asInstanceOf[HadoopRDD[_, _]]
    val jobConf = hadoopRDD.getConf
    // the jobConf passed to HadoopRDD should contain SparkContext's hadoop items rather the default
    // configs in client's Configuration
    assert(jobConf.get(firstKey) === "spark." + firstValue)
  }
}
