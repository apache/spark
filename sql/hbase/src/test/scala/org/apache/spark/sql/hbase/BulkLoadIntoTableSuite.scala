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

package org.apache.spark.sql.hbase

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.hbase.execution._
import org.apache.spark.sql.hbase.util.InsertWappers._
import org.apache.spark.sql.hbase.util.{BytesUtils, Util}
import org.apache.spark.{Logging, SerializableWritable, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ArrayBuffer

class BulkLoadIntoTableSuite extends FunSuite with BeforeAndAfterAll with Logging {

  val (hbc: HBaseSQLContext, sc: SparkContext) = {
    HBaseMainTest.setupData(true)
    (HBaseMainTest.hbc, HBaseMainTest.sc)
  }

  // Test if we can parse 'LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb'
  test("bulkload parser test, local file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb"
    //val sql = "select"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"./usr/file.txt"))
    assert(l.isLocal)
    assert(l.tableName.equals("tb"))
  }

  // Test if we can parse 'LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb'
  test("bulkload parser test, load hdfs file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb"
    //val sql = "select"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb"))
  }

  test("bulkload parser test, using delimiter") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb FIELDS TERMINATED BY '|' "

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb"))
    assert(l.delimiter.get.equals("|"))
  }

  test("write data to HFile") {
    val colums = Seq(new KeyColumn("k1", IntegerType, 0), new NonKeyColumn("v1", IntegerType, "cf1", "c1"))
    val hbaseRelation = HBaseRelation("testtablename", "hbasenamespace", "hbasetablename", colums)(hbc)
    val bulkLoad = BulkLoadIntoTableCommand("./sql/hbase/src/test/resources/test.txt", "hbasetablename",
      isLocal = true, Option(","))
    val splitKeys = (1 to 40).filter(_ % 5 == 0).map { r =>
      val bytesUtils = BytesUtils.create(IntegerType)
      new ImmutableBytesWritableWrapper(bytesUtils.toBytes(r))
    }
    val conf = hbc.sparkContext.hadoopConfiguration

    val job = Job.getInstance(conf)

    val hadoopReader = {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(bulkLoad.path)).getCanonicalPath
      new HadoopReader(hbc.sparkContext, pathString, bulkLoad.delimiter)(hbaseRelation)
    }
    val tmpPath = Util.getTempFilePath(conf, hbaseRelation.tableName)
    bulkLoad.makeBulkLoadRDD(splitKeys.toArray, hadoopReader, job, tmpPath, hbaseRelation)
  }

  test("write data to HFile with optimized bulk loading") {
    val colums = Seq(new KeyColumn("k1", IntegerType, 0), new NonKeyColumn("v1", IntegerType, "cf1", "c1"))
    val hbaseRelation = HBaseRelation("testtablename", "hbasenamespace", "hbasetablename", colums)(hbc)
    val bulkLoad =
      ParallelizedBulkLoadIntoTableCommand(
        "./sql/hbase/src/test/resources/test.txt",
        "hbasetablename",
        isLocal = true,
        Option(","))
    val splitKeys = (1 to 40).filter(_ % 5 == 0).map { r =>
      val bytesUtils = BytesUtils.create(IntegerType)
      new ImmutableBytesWritableWrapper(bytesUtils.toBytes(r))
    }
    val conf = hbc.sparkContext.hadoopConfiguration

    val hadoopReader = {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(bulkLoad.path)).getCanonicalPath
      new HadoopReader(hbc.sparkContext, pathString, bulkLoad.delimiter)(hbaseRelation)
    }
    val tmpPath = Util.getTempFilePath(conf, hbaseRelation.tableName)
    val wrappedConf = new SerializableWritable(conf)

    val result = bulkLoad.makeBulkLoadRDD(
      splitKeys.toArray,
      hadoopReader,
      wrappedConf,
      "./hfileoutput")(hbaseRelation)

    result.foreach(println)
    for (i <- 0 to 7)
      FileSystem.get(conf).delete(new Path("./hfileoutput" + i), true)
  }

  test("hfile output format, delete me when ready") {
    import org.apache.spark.sql.catalyst.types._
    val splitRegex = ","
    val conf = hbc.sparkContext.hadoopConfiguration

    val rdd = sc.textFile("./sql/hbase/src/test/resources/test.txt", 1).mapPartitions { iter =>
      val keyBytes = new Array[(Array[Byte], DataType)](1)
      val valueBytes = new Array[(Array[Byte], Array[Byte], Array[Byte])](1)
      val bytesUtils = BytesUtils.create(IntegerType)
      val family = Bytes.toBytes("cf")
      val qualyfier = Bytes.toBytes("c1")

      iter.map { line =>
        val splits = line.split(splitRegex)
        keyBytes(0) = (bytesUtils.toBytes(splits(0).toInt), IntegerType)
        //println("test1 " + BytesUtils.toInt(bytesUtils.toBytes(splits(0).toInt), 0)) // here revert 1 is 1
        valueBytes(0) = (family, qualyfier, bytesUtils.toBytes(splits(1).toInt))
        //val rowKeyData = HBaseKVHelper.encodingRawKeyColumns(keyBytes) // here revert 1 is 6
        val rowKeyData = bytesUtils.toBytes(splits(0).toInt)
        //println("test2 " + BytesUtils.toInt(rowKeyData, 0))

        val rowKey = new ImmutableBytesWritableWrapper(rowKeyData)
        val put = new PutWrapper(rowKeyData)
        valueBytes.foreach { case (fam, qualifier, value) =>
          put.add(fam, qualifier, value)
        }
        (rowKey, put)
      }
    }

    import org.apache.hadoop.hbase._
    import org.apache.hadoop.hbase.io._

    import scala.collection.JavaConversions._
    val splitKeys = (1 to 40).filter(_ % 5 == 0).map { r =>
      val bytesUtils = BytesUtils.create(IntegerType)
      new ImmutableBytesWritableWrapper(bytesUtils.toBytes(r))
    }
    //    splitKeys += (new ImmutableBytesWritableWrapper(Bytes.toBytes(100)))
    val ordering = implicitly[Ordering[ImmutableBytesWritableWrapper]]
    val partitioner = new HBasePartitioner(splitKeys.toArray)
    val shuffled =
      new ShuffledRDD[ImmutableBytesWritableWrapper, PutWrapper, PutWrapper](rdd, partitioner)
        .setKeyOrdering(ordering)

    val bulkLoadRDD = shuffled.mapPartitions { iter =>
      // the rdd now already sort by key, to sort by value
      val map = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)
      var preKV: (ImmutableBytesWritableWrapper, PutWrapper) = null
      var nowKV: (ImmutableBytesWritableWrapper, PutWrapper) = null
      val ret = new ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
      if (iter.hasNext) {
        preKV = iter.next()
        var cellsIter = preKV._2.toPut.getFamilyCellMap.values().iterator()
        while (cellsIter.hasNext) {
          cellsIter.next().foreach { cell =>
            val kv = KeyValueUtil.ensureKeyValue(cell)
            map.add(kv)
          }
        }
        while (iter.hasNext) {
          nowKV = iter.next()
          if (0 == (nowKV._1 compareTo preKV._1)) {
            cellsIter = nowKV._2.toPut.getFamilyCellMap.values().iterator()
            while (cellsIter.hasNext) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          } else {
            ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable, _))
            preKV = nowKV
            map.clear()
            cellsIter = preKV._2.toPut.getFamilyCellMap.values().iterator()
            while (cellsIter.hasNext) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          }
        }
        ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable, _))
        map.clear()
        ret.iterator
      } else {
        Iterator.empty
      }
    }
    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    job.getConfiguration.set("mapred.output.dir", "./hfileoutput")
    bulkLoadRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    FileSystem.get(conf).delete(new Path("./hfileoutput"), true)
  }

  test("load data into hbase") {

    val drop = "drop table testblk"
    val executeSql0 = hbc.executeSql(drop)
    try {
      executeSql0.toRdd.collect().foreach(println)
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        println(e.getMessage)
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (wf, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = hbc.executeSql(sql1)
    executeSql1.toRdd.collect().foreach(println)

    val executeSql2 = hbc.executeSql(sql2)
    executeSql2.toRdd.collect().foreach(println)

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH './sql/hbase/src/test/resources/loadData.txt' INTO TABLE testblk"

    val executeSql3 = hbc.executeSql(loadSql)
    executeSql3.toRdd.collect().foreach(println)
    hbc.sql("select * from testblk").collect().foreach(println)
  }

  override def afterAll() {
    sc.stop()
  }
}
