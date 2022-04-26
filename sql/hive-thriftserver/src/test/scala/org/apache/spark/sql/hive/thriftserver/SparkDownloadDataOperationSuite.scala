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

package org.apache.spark.sql.hive.thriftserver

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.OperationState
import org.apache.hive.service.cli.session.HiveSessionImpl
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.mockito.Mockito.mock

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.thriftserver.ui.HiveThriftServer2EventManager
import org.apache.spark.sql.test.SharedSparkSession

class SparkDownloadDataOperationSuite extends QueryTest with SharedSparkSession {

  private def getFile(path: String, localdst: String) = {
    val strs = path.split(File.separator)
    val fileName = strs(strs.length - 1)
    new File(localdst + File.separator + fileName)
  }

  private def writeToDir(iter: util.Iterator[Array[AnyRef]], localdst: String) = {
    while (iter.hasNext) {
      val row = iter.next()
      val file = row(0).asInstanceOf[String]
      val size = row(2).asInstanceOf[Long]
      if (size > 0) {
        val f = getFile(file, localdst)
        val fop = new FileOutputStream(f, true)
        if (!f.exists()) f.createNewFile()

        val buff = new Array[Byte](size.toInt)
        new ByteArrayInputStream(row(1).asInstanceOf[Array[Byte]]).read(buff)

        fop.write(buff)
        fop.flush()
        fop.close()
      }
    }
  }

  test("Download data through thrift server") {
    HiveThriftServer2.eventManager = new HiveThriftServer2EventManager(sqlContext.sparkContext)
    val hiveConf = new HiveConf()
    val hiveSession = new HiveSessionImpl(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "username", "password", new HiveConf, "ip address")
    hiveSession.open(new util.HashMap)

    HiveThriftServer2.eventManager = mock(classOf[HiveThriftServer2EventManager])

    withTempDir { dir =>
      val sqlStr = "SELECT id AS a, id / 10 AS b FROM range(100)"
      val operation = new SparkDownloadDataOperation(
        sqlContext, hiveSession, null, sqlStr, "parquet", new util.HashMap, false)
      operation.runInternal()

      assert(operation.getStatus.getState === OperationState.FINISHED)
      operation.getResultSetSchema

      val rowSet = operation.getNextRowSet()
      val iter = rowSet.iterator()

      if (iter.hasNext) {
        val totalDataSize = iter.next()(2).asInstanceOf[Long]
        assert(totalDataSize > 0)
      }

      writeToDir(iter, dir.getCanonicalPath)
      assert(dir.listFiles().forall(_.getName.endsWith("gz.parquet")))

      checkAnswer(spark.read.parquet(dir.getCanonicalPath), spark.sql(sqlStr))
    }


  }
}
