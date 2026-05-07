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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{BitwiseAnd, Expression, HiveHash, Literal, Pmod}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHive.sparkSession.implicits._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class BucketedWriteWithHiveSupportSuite extends BucketedWriteSuite with TestHiveSingleton {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assert(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive")
  }

  override protected def fileFormatsToTest: Seq[String] = Seq("parquet", "orc")

  test("write hive bucketed table") {
    def bucketIdExpression(expressions: Seq[Expression], numBuckets: Int): Expression =
      Pmod(BitwiseAnd(HiveHash(expressions), Literal(Int.MaxValue)), Literal(8))

    def getBucketIdFromFileName(fileName: String): Option[Int] = {
      val hiveBucketedFileName = """^(\d+)_0_.*$""".r
      fileName match {
        case hiveBucketedFileName(bucketId) => Some(bucketId.toInt)
        case _ => None
      }
    }

    val table = "hive_bucketed_table"

    fileFormatsToTest.foreach { format =>
      Seq("true", "false").foreach { enableConvertMetastore =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> enableConvertMetastore,
          HiveUtils.CONVERT_METASTORE_ORC.key -> enableConvertMetastore) {
          withTable(table) {
            sql(
              s"""
                 |CREATE TABLE IF NOT EXISTS $table (i int, j string)
                 |PARTITIONED BY(k string)
                 |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                 |STORED AS $format
               """.stripMargin)

            val df =
              (0 until 50).map(i => (i % 13, i.toString, i % 5)).toDF("i", "j", "k")

            withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
              df.write.mode(SaveMode.Overwrite).insertInto(table)
            }

            for (k <- 0 until 5) {
              testBucketing(
                new File(tableDir(table), s"k=$k"),
                format,
                8,
                Seq("i", "j"),
                Seq("i"),
                df,
                bucketIdExpression,
                getBucketIdFromFileName)
            }
          }
        }
      }
    }
  }
}
