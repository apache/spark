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

package org.apache.spark.nosql.hbase

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SchemaRDD}
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.hadoop.hbase.util.Bytes

/**
 * A public object that provides HBase support.
 * You could save RDD into HBase through
 * [[org.apache.spark.nosql.hbase.HBaseUtils.saveAsHBaseTable]] method.
 */
object HBaseUtils
  extends Logging {

  /**
   * Save [[org.apache.spark.rdd.RDD[Text]]] as a HBase table
   *
   * The format of records in RDD should look like this:
   *   rowkey|delimiter|column|delimiter|column|delimiter|...
   * For example (if delimiter is ","):
   *   0001,apple,banana
   * "0001" is rowkey field while "apple" and "banana" are column fields.
   *
   * @param rdd [[org.apache.spark.rdd.RDD[Text]]]
   * @param zkHost the zookeeper hosts. e.g. "10.232.98.10,10.232.98.11,10.232.98.12"
   * @param zkPort the zookeeper client listening port. e.g. "2181"
   * @param zkNode the zookeeper znode of HBase. e.g. "hbase-apache"
   * @param table the name of table which we save records
   * @param rowkeyType the type of rowkey. [[org.apache.spark.sql.catalyst.types.DataType]]
   * @param columns the column list. [[org.apache.spark.nosql.hbase.HBaseColumn]]
   * @param delimiter the delimiter which used to split record into fields
   */
  def saveAsHBaseTable(rdd: RDD[Text],
                       zkHost: String, zkPort: String, zkNode: String, table: String,
                       rowkeyType: DataType, columns: List[HBaseColumn], delimiter: Char) {
    val conf = new HBaseConf(zkHost, zkPort, zkNode, table, rowkeyType, columns, delimiter)

    def writeToHBase(iter: Iterator[Text]) {
      val writer = new SparkHBaseWriter(conf)

      try {
        writer.init()

        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record)
        }
      } finally {
        try {
          writer.close()
        } catch {
          case ex: Exception => logWarning("Close HBase table failed.", ex)
        }
      }
    }

    rdd.foreachPartition(writeToHBase)
  }

  /**
   * Save [[org.apache.spark.sql.SchemaRDD]] as a HBase table
   *
   * The first field of Row would be save as rowkey in HBase.
   * All fields of Row use the @param family as the column family.
   *
   * @param rdd [[org.apache.spark.sql.SchemaRDD]]
   * @param zkHost the zookeeper hosts. e.g. "10.232.98.10,10.232.98.11,10.232.98.12"
   * @param zkPort the zookeeper client listening port. e.g. "2181"
   * @param zkNode the zookeeper znode of HBase. e.g. "hbase-apache"
   * @param table the name of table which we save records
   * @param family the fixed column family which we save records
   */
  def saveAsHBaseTable(rdd: SchemaRDD,
                       zkHost: String, zkPort: String, zkNode: String,
                       table: String, family: Array[Byte]) {
    // Convert attributes informations in SchemaRDD to List[HBaseColumn]
    val attributes = rdd.logicalPlan.output
    var i = 0
    // Assume first field in Row is rowkey
    val rowkeyType = attributes(i).dataType

    var columns = List.empty[HBaseColumn]
    for (i <- 1 to attributes.length - 1) {
      val attribute = attributes(i)
      val qualifier = Bytes.toBytes(attribute.name)
      columns = columns :+ new HBaseColumn(family, qualifier, attribute.dataType)
    }

    val conf = new HBaseConf(zkHost, zkPort, zkNode, table, rowkeyType, columns, ',')

    def writeToHBase(iter: Iterator[Row]) {
      val writer = new SparkHBaseWriter(conf)

      try {
        writer.init()

        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record)
        }
      } finally {
        try {
          writer.close()
        } catch {
          case ex: Exception => logWarning("Close HBase table failed.", ex)
        }
      }
    }

    rdd.foreachPartition(writeToHBase)
  }
}
