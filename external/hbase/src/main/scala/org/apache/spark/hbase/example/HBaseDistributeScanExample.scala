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
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Scan
import java.util.ArrayList


object HBaseDistributedScanExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("GenerateGraphs {master} {tableName}")
      return ;
    }

    val master = args(0);
    val tableName = args(1);

    val sc = new SparkContext(master, "HBaseDistributedScanExample")
    sc.addJar("SparkHBase.jar")

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf)
    
    var scan = new Scan()
    scan.setCaching(100)
    
    var getRdd = hbaseContext.hbaseRDD(tableName, scan)
    
    getRdd.collect.foreach(v => System.out.println(Bytes.toString(v._1)))
    
  }
}