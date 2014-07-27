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
import org.apache.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Delete

object HBaseBulkDeleteExample {
  def main(args: Array[String]) {
	  if (args.length == 0) {
    		System.out.println("HBaseBulkDeletesExample {master} {tableName} {columnFamily}");
    		return;
      }
    	
      val master = args(0);
      val tableName = args(1);
    	
      val sc = new SparkContext(master, "HBaseBulkDeletesExample");
      sc.addJar("SparkHBase.jar")
      
      //[Array[Byte]]
      val rdd = sc.parallelize(Array(
            (Bytes.toBytes("1")),
            (Bytes.toBytes("2")),
            (Bytes.toBytes("3")),
            (Bytes.toBytes("4")),
            (Bytes.toBytes("5"))
           )
          )
    	
      val conf = HBaseConfiguration.create();
	    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    	
      val hbaseContext = new HBaseContext(sc, conf);
      hbaseContext.bulkDelete[Array[Byte]](rdd, 
          tableName,
          putRecord => new Delete(putRecord),
          4);
	}
}