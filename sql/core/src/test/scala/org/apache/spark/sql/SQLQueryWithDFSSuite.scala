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

package org.apache.spark.sql

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{DistributedFileSystem, HdfsConfiguration, MiniDFSCluster}

import org.apache.spark.{InsertFileSourceConflictException, SparkConf}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class SQLQueryWithDFSSuite extends QueryTest with SQLTestUtils{
  import InsertIntoHadoopFsRelationCommand._

  var dataDir: File = _
  var cluster: MiniDFSCluster = _
  var fs: DistributedFileSystem = _

  override def beforeAll(): Unit = {
    dataDir = Utils.createTempDir()
    val hdfsConf = new HdfsConfiguration
    hdfsConf.set("fs.hdfs.impl.disable.cache", "true")
    hdfsConf.set("hdfs.minidfs.basedir", dataDir.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(hdfsConf).build()
    cluster.waitClusterUp()
    fs = cluster.getFileSystem
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    fs.close()
    cluster.shutdown()
    Utils.deleteRecursively(dataDir)
    spark.stop()
    super.afterAll()
  }

  protected def sparkConf = {
    new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .setMaster("local")
      .set("spark.driver.host", "127.0.0.1")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
  }

  override def spark: SparkSession = sparkSession

  private lazy val sparkSession = {
    val conf = sparkConf
    cluster.getConfiguration(0).iterator().asScala.foreach { kv =>
      conf.set(kv.getKey, kv.getValue)
    }
    val warehouse = new Path(fs.getHomeDirectory, "spark-warehouse")
    conf.set(StaticSQLConf.WAREHOUSE_PATH, warehouse.toUri.getPath)
    SparkSession.builder().config(conf).getOrCreate()
  }

  test("HADP-33041: automatically recognize whether app is running") {
    withTempDfsDir { dir =>
      withTable("ta") {
        sql(
          s"""
             |create table ta (id int, p1 int, p2 int) using parquet partitioned by (p1, p2)
             |location '${dir.toUri.toString}'
             |""".stripMargin
        )

        val conflictedPath1 = new Path(dir, ".spark-staging-1/p1=1/app_1")
        fs.mkdirs(conflictedPath1)
        val appThread1 = new OperationLockThread(new Path(dir, ".spark-staging-1/p1=1" ), false)
        appThread1.start()
        intercept[InsertFileSourceConflictException](
          sql("insert overwrite table ta partition(p1=1,p2) select 1,2"))
        appThread1.interrupt()
        appThread1.join()
        sql("insert overwrite table ta partition(p1=1,p2) select 1,2")

        val conflictedPath2 = new Path(dir, ".spark-staging-1/p1=2/app_2")
        fs.mkdirs(conflictedPath2)
        val appThread2 = new OperationLockThread(new Path(dir, ".spark-staging-1/p1=2"), true)
        appThread2.start()
        Thread.sleep(1 * 1000)
        appThread2.interrupt()
        appThread2.join()
        sql("insert overwrite table ta partition(p1=2,p2) select 1,2")

        val conflictedPath3 = new Path(dir, ".spark-staging-2/p1=1/p2=2/app_3")
        fs.mkdirs(conflictedPath3)
        val appThread3 = new OperationLockThread(new Path(dir, ".spark-staging-1/p1=1/p2=2"), true)
        appThread3.start()
        Thread.sleep(1 * 1000)
        appThread3.interrupt()
        appThread3.join()
        sql("insert overwrite table ta partition(p1=1,p2) select 1,2")
      }
    }
  }

  class OperationLockThread(
                             stagingPartitionPath: Path,
                             abort: Boolean = false) extends Thread {
    override def run(): Unit = {
      var fs: Option[DistributedFileSystem] = None
      val lock = new Path(stagingPartitionPath, getLockName)
      try {
        fs = Some(cluster.getFileSystem)
        fs.get.create(lock)
        Thread.sleep(60 * 60 * 1000)
      } catch {
        case _: InterruptedException =>
          try {
            // Here is an reflection implementation of DistributedFileSystem.close()
            fs.foreach(_.getClient.closeOutputStreams(abort))
            invokeSuperMethod(fs.get, "close")
          } finally {
            fs.foreach(_.getClient.close())
          }
      }
    }
  }

  def withTempDfsDir(f: Path => Unit): Unit = {
    val namePrefix = "spark-"
    val dirName = namePrefix + UUID.randomUUID().toString
    val dir = new Path(fs.getHomeDirectory, dirName)
    fs.mkdirs(dir)
    try {
      f(dir)
    } finally {
      fs.delete(dir, true)
    }
  }

  /**
   * Invoke a super method of an object via reflection.
   */
  private def invokeSuperMethod(o: Any, name: String): Any = {
    Try {
      val method = try {
        o.getClass.getSuperclass.getDeclaredMethod(name)
      } catch {
        case e: NoSuchMethodException =>
          o.getClass.getMethod(name)
      }
      method.setAccessible(true)
      method.invoke(o)
    } match {
      case Success(value) => value
      case Failure(e) => throw e
    }
  }
}