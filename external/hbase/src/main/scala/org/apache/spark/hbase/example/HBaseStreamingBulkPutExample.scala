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

package org.apache.spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.hbase.HBaseContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

object HBaseStreamingBulkPutExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
        System.out.println("HBaseStreamingBulkPutExample {master} {host} {port} {tableName} {columnFamily}");
        return;
      }
      
      val master = args(0);
      val host = args(1);
      val port = args(2);
      val tableName = args(3);
      val columnFamily = args(4);
      
      System.out.println("master:" + master)
      System.out.println("host:" + host)
      System.out.println("port:" + Integer.parseInt(port))
      System.out.println("tableName:" + tableName)
      System.out.println("columnFamily:" + columnFamily)
      
      val sparkConf = new SparkConf();
      
      sparkConf.set("spark.cleaner.ttl", "120000");
      
      val sc = new SparkContext(master, "HBaseStreamingBulkPutExample", sparkConf)
      sc.addJar("SparkHBase.jar")
      
      val ssc = new StreamingContext(sc, Seconds(1))
      
      val lines = ssc.socketTextStream(host, Integer.parseInt(port))
      
      val conf = HBaseConfiguration.create();
      conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
      conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
      
      val hbaseContext = new HBaseContext(sc, conf);
      
      hbaseContext.streamBulkPut[String](lines, 
          tableName,
          (putRecord) => {
            if (putRecord.length() > 0) {
              val put = new Put(Bytes.toBytes(putRecord))
              put.add(Bytes.toBytes("c"), Bytes.toBytes("foo"), Bytes.toBytes("bar"))
              put
            } else {
              null
            }
          },
          false);
      
      ssc.start();
      
      
  }
}