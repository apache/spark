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

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.storage.StorageLevel

class CacheTableInKryoSuite extends QueryTest
  with SQLTestUtils
  with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  test("SPARK-51777: sql.columnar.* classes registered in KryoSerializer") {
    withTable("t1") {
      sql("CREATE TABLE t1 AS SELECT 1 AS a")
      checkAnswer(sql("SELECT * FROM t1").persist(StorageLevel.DISK_ONLY), Seq(Row(1)))
    }
  }

  test("SPARK-51790: UTF8String should be registered in KryoSerializer") {
    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 AS
          |  SELECT * from
          |  VALUES ('apache', 'spark', 'community'),
          |         ('Apache', 'Spark', 'Community')
          |  as t(a, b, c)
          |""".stripMargin)
        checkAnswer(sql("SELECT a, b, c FROM t1").persist(StorageLevel.DISK_ONLY),
            Seq(Row("apache", "spark", "community"), Row("Apache", "Spark", "Community")))
    }
  }
}
