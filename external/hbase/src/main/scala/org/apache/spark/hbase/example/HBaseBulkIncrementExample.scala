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
import org.apache.hadoop.hbase.client.Increment
import org.apache.spark.hbase.HBaseContext

object HBaseBulkIncrementExample {
  def main(args: Array[String]) {
	  if (args.length == 0) {
    		System.out.println("GenerateGraphs {master} {tableName} {columnFamily}");
    		return;
      }
    	
      val master = args(0);
      val tableName = args(1);
      val columnFamily = args(2);
    	
      val sc = new SparkContext(master, "HBaseBulkIncrementsExample");
      sc.addJar("SparkHBase.jar")
      
      //[(Array[Byte], Array[(Array[Byte], Array[Byte], Long)])]
      val rdd = sc.parallelize(Array(
            (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 1L))),
            (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 2L))),
            (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 3L))),
            (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 4L))),
            (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 5L)))
           )
          );
    	
      val conf = HBaseConfiguration.create();
	    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    	
      val hbaseContext = new HBaseContext(sc, conf);
      hbaseContext.bulkIncrement[(Array[Byte], Array[(Array[Byte], Array[Byte], Long)])](rdd, 
          tableName,
          (incrementRecord) => {
            val increment = new Increment(incrementRecord._1)
            incrementRecord._2.foreach((incrementValue) => 
              increment.addColumn(incrementValue._1, incrementValue._2, incrementValue._3))
            increment
          },
          4);
	}
}