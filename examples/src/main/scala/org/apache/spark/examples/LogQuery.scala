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

package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Executes a roll up-style query against Apache logs.
 *  
 * Usage: LogQuery [logFile]
 */
object LogQuery {
  val exampleApacheLogs = List(
    """10.10.10.10 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg
      | HTTP/1.1" 304 315 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1;
      | GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR
      | 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR
      | 3.5.30729; Release=ARP)" "UD-1" - "image/jpeg" "whatever" 0.350 "-" - "" 265 923 934 ""
      | 62.24.11.25 images.com 1358492167 - Whatup""".stripMargin.lines.mkString,
    """10.10.10.10 - "FRED" [18/Jan/2013:18:02:37 +1100] "GET http://images.com/2013/Generic.jpg
      | HTTP/1.1" 304 306 "http:/referall.com" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1;
      | GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR
      | 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR
      | 3.5.30729; Release=ARP)" "UD-1" - "image/jpeg" "whatever" 0.352 "-" - "" 256 977 988 ""
      | 0 73.23.2.15 images.com 1358492557 - Whatup""".stripMargin.lines.mkString
  )

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Log Query")
    val sc = new SparkContext(sparkConf)

    val dataSet =
      if (args.length == 1) sc.textFile(args(0)) else sc.parallelize(exampleApacheLogs)
    // scalastyle:off
    val apacheLogRegex =
      """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)".*""".r
    // scalastyle:on
    /** Tracks the total query count and number of aggregate bytes for a particular group. */
    class Stats(val count: Int, val numBytes: Int) extends Serializable {
      def merge(other: Stats): Stats = new Stats(count + other.count, numBytes + other.numBytes)
      override def toString: String = "bytes=%s\tn=%s".format(numBytes, count)
    }

    def extractKey(line: String): (String, String, String) = {
      apacheLogRegex.findFirstIn(line) match {
        case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua)) =>
          if (user != "\"-\"") (ip, user, query)
          else (null, null, null)
        case _ => (null, null, null)
      }
    }

    def extractStats(line: String): Stats = {
      apacheLogRegex.findFirstIn(line) match {
        case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua)) =>
          new Stats(1, bytes.toInt)
        case _ => new Stats(1, 0)
      }
    }

    dataSet.map(line => (extractKey(line), extractStats(line)))
      .reduceByKey((a, b) => a.merge(b))
      .collect().foreach{
        case (user, query) => println("%s\t%s".format(user, query))}

    sc.stop()
  }
}
