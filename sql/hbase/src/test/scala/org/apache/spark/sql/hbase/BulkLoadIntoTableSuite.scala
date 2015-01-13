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
import org.apache.spark.sql.hbase.util.InsertWrappers._
import org.apache.spark.sql.hbase.util.{BytesUtils, Util}
import org.apache.spark.{SerializableWritable, SparkContext}

import scala.collection.mutable.ArrayBuffer

class BulkLoadIntoTableSuite extends HBaseIntegrationTestBase {
  val sc: SparkContext = TestHbase.sparkContext
  val sparkHome = TestHbase.sparkContext.getSparkHome().orNull
  if (sparkHome == null || sparkHome.isEmpty)
    logError("Spark Home is not defined; may lead to unexpected error!")

  // Test if we can parse 'LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1'
  test("bulkload parser test, local file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"./usr/file.txt"))
    assert(l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  // Test if we can parse 'LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1'
  test("bulkload parser test, load hdfs file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1"
    //val sql = "select"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  test("bulkload parser test, using delimiter") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1 FIELDS TERMINATED BY '|' "

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[ParallelizedBulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[ParallelizedBulkLoadIntoTableCommand]
    assert(l.path.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
    assert(l.delimiter.get.equals("|"))
  }

  test("write data to HFile") {
    val columns = Seq(new KeyColumn("k1", IntegerType, 0), new NonKeyColumn("v1", IntegerType, "cf1", "c1"))
    val hbaseRelation = HBaseRelation("testtablename", "hbasenamespace", "hbasetablename", columns)(TestHbase)
    val bulkLoad = BulkLoadIntoTableCommand(sparkHome + "/sql/hbase/src/test/resources/test.txt", "hbasetablename",
      isLocal = true, Option(","))
    val splitKeys = (1 to 40).filter(_ % 5 == 0).map { r =>
      val bytesUtils = BytesUtils.create(IntegerType)
      new ImmutableBytesWritableWrapper(bytesUtils.toBytes(r))
    }
    val conf = TestHbase.sparkContext.hadoopConfiguration

    val job = Job.getInstance(conf)

    val hadoopReader = {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(bulkLoad.path)).getCanonicalPath
      new HadoopReader(TestHbase.sparkContext, pathString, bulkLoad.delimiter)(hbaseRelation)
    }
    val tmpPath = Util.getTempFilePath(conf, hbaseRelation.tableName)
    bulkLoad.makeBulkLoadRDD(splitKeys.toArray, hadoopReader, job, tmpPath, hbaseRelation)
    FileSystem.get(conf).delete(new Path(tmpPath), true)
  }

  test("write data to HFile with optimized bulk loading") {
    val columns = Seq(new KeyColumn("k1", IntegerType, 0), new NonKeyColumn("v1", IntegerType, "cf1", "c1"))
    val hbaseRelation = HBaseRelation("testtablename", "hbasenamespace", "hbasetablename", columns)(TestHbase)
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
    val conf = TestHbase.sparkContext.hadoopConfiguration

    val hadoopReader = {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(bulkLoad.path)).getCanonicalPath
      new HadoopReader(TestHbase.sparkContext, pathString, bulkLoad.delimiter)(hbaseRelation)
    }
    val tmpPath = Util.getTempFilePath(conf, hbaseRelation.tableName)
    val wrappedConf = new SerializableWritable(conf)

    val result = bulkLoad.makeBulkLoadRDD(
      splitKeys.toArray,
      hadoopReader,
      wrappedConf,
      "./hfileoutput")(hbaseRelation)

    for (i <- 0 to 7)
      FileSystem.get(conf).delete(new Path("./hfileoutput" + i), true)
    FileSystem.get(conf).delete(new Path("testtablename*"), true)
  }

  test("hfile output format, delete me when ready") {
    import org.apache.spark.sql.catalyst.types._
    val splitRegex = ","
    val conf = TestHbase.sparkContext.hadoopConfiguration
    val inputFile = sparkHome + "/sql/hbase/src/test/resources/test.txt"

    FileSystem.get(conf).delete(new Path("./hfileoutput"), true)

    val rdd = sc.textFile(inputFile, 1).mapPartitions { iter =>
      val keyBytes = new Array[(Array[Byte], DataType)](1)
      val valueBytes = new Array[(Array[Byte], Array[Byte], Array[Byte])](1)
      val bytesUtils = BytesUtils.create(IntegerType)
      val family = Bytes.toBytes("cf")
      val qualifier = Bytes.toBytes("c1")

      iter.map { line =>
        val splits = line.split(splitRegex)
        keyBytes(0) = (bytesUtils.toBytes(splits(0).toInt), IntegerType)
        valueBytes(0) = (family, qualifier, bytesUtils.toBytes(splits(1).toInt))
        val rowKeyData = bytesUtils.toBytes(splits(0).toInt)

        val rowKey = new ImmutableBytesWritableWrapper(rowKeyData)
        val put = new PutWrapper(rowKeyData)
        valueBytes.foreach { case (fam, qual, value) =>
          put.add(fam, qual, value)
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
    val executeSql0 = TestHbase.executeSql(drop)
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

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect().foreach(println)

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect().foreach(println)

    val inputFile = "'" + sparkHome + "/sql/hbase/src/test/resources/loadData.txt'"

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect().foreach(println)
    val rows = TestHbase.sql("select * from testblk").collect()
    assert(rows.length == 3, s"load data into hbase test failed to bulkload data into htable")

    // cleanup
    TestHbase.sql(drop)

    // delete the temp files
    val fileSystem = FileSystem.get(TestHbase.sparkContext.hadoopConfiguration)
    val files = fileSystem.listStatus(new Path(sparkHome))
    for (file <- files) {
      println(file.getPath.getName)
      if (file.getPath.getName.indexOf("testblk") != -1) {
        fileSystem.delete(file.getPath, true)
      }
    }
  }
}
