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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PruneFileSourcePartitions}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HivePruneFileSourcePartitionsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton{

  private def executeTestBody(testBody: => Unit): Unit = {
    val oldDynamicPartitionOpt = spark.conf.getOption("hive.exec.dynamic.partition")
    val oldDynamicPartitionModeOpt = spark.conf.getOption("hive.exec.dynamic.partition.mode")
    val oldExcludedRulesOpt = spark.conf.getOption(SQLConf.OPTIMIZER_EXCLUDED_RULES.key)
    try {
      spark.conf.set("hive.exec.dynamic.partition", "true")
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      testBody
    } finally {
      if (oldExcludedRulesOpt.isDefined) {
        oldExcludedRulesOpt.foreach(spark.conf.set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, _))
      } else {
        spark.conf.unset(SQLConf.OPTIMIZER_EXCLUDED_RULES.key)
      }

      if (oldDynamicPartitionOpt.isDefined) {
        oldDynamicPartitionOpt.foreach(spark.conf.set("hive.exec.dynamic.partition", _))
      } else {
        spark.conf.unset("hive.exec.dynamic.partition")
      }
      if (oldDynamicPartitionModeOpt.isDefined) {
        oldDynamicPartitionModeOpt.foreach(spark.conf.set("hive.exec.dynamic.partition.mode",
          _))
      } else {
        spark.conf.unset("hive.exec.dynamic.partition.mode")
      }
    }
  }

  def excludePartitionPruneRule(): Unit = {
    val currentExclusions = spark.conf.getOption(SQLConf.OPTIMIZER_EXCLUDED_RULES.key)
    val newExclusion = currentExclusions.map(old => s"$old,${PruneFileSourcePartitions.ruleName}").
      getOrElse(PruneFileSourcePartitions.ruleName)
    spark.conf.set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, newExclusion)
  }

  test("partition file source pruning with union of partitions") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("test").as("test1").where("test1.p > 15")
      val df2 = spark.table("test").as("test2").where("test2.p < 5")
      val res = df1.join(df2, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
             |CREATE EXTERNAL TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet
             |LOCATION '${dir.toURI}'""".stripMargin)
        executeTestBody(
          {
            spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("test")
            // Test total partitions = p1 union p2
            val (numPartitionFetches, resWithRule) = test()
            excludePartitionPruneRule()
            val (_, resWoRule) = test()
            resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
            assert(numPartitionFetches == 9)
          }
        )
      }
    }
  }

  test("partition file source pruning with intersection of partitions") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("test").as("test1").where("test1.p < 10")
      val df2 = spark.table("test").as("test2").where("test2.p < 5")
      val res = df1.join(df2, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
             |CREATE EXTERNAL TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet
             |LOCATION '${dir.toURI}'""".stripMargin)
        executeTestBody({
          spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("test")
          // Test total partitions = p1 superset of p2
          val (numPartitionFetches, resWithRule) = test()
          excludePartitionPruneRule()
          val (_, resWoRule) = test()
          resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
          assert(numPartitionFetches == 10)
        })
      }
    }
  }

  test("partition file source no pruning partition fetch") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("test").as("test1").where("test1.i > 10 and test1.p is not null")
      val df2 = spark.table("test").as("test2").where("test2.i < 50 and test2.p is not null")
      val res = df1.join(df2, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
             |CREATE EXTERNAL TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet
             |LOCATION '${dir.toURI}'""".stripMargin)
        executeTestBody({
          spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("test")
          val (numPartitionFetches, resWithRule) = test()
          excludePartitionPruneRule()
          val (_, resWoRule) = test()
          resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
          assert(numPartitionFetches == 20)
        })
      }
    }
  }

  test("partition file source no pruning partition fetch despite having some" +
    " prunable partitions") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("test").as("test1").where("test1.i > 10 and test1.p is not null")
      val df2 = spark.table("test").as("test2").where("test2.i < 50 and test2.p > 10")
      val df3 = spark.table("test").as("test3").where("test3.i < 17 and test3.p < 3")
      val res = df1.join(df2, "i").join(df3, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
             |CREATE EXTERNAL TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet
             |LOCATION '${dir.toURI}'""".stripMargin)
        executeTestBody({
          spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("test")
          val (numPartitionFetches, resWithRule) = test()
          excludePartitionPruneRule()
          val (_, resWoRule) = test()
          resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
          assert(numPartitionFetches == 20)
        })
      }
    }
  }

  test("partition file source pruning with multi table with reduction in hms calls") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("testA").as("test2").where("test2.i < 50 and test2.p > 10")
      val df2 = spark.table("testA").as("test3").where("test3.i < 17 and test3.p < 3")
      val df3 = spark.table("testB").as("test4").where("test4.i < 10 and test4.p < 8")
      val df4 = spark.table("testB").as("test5").where("test5.i > 100 and test5.p > 16")
      val res = df1.join(df2, "i").join(df3, "i").join(df4, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("testA") {
      withTable("testB") {
        withTempDir { dir1 =>
          sql(
            s"""
               |CREATE EXTERNAL TABLE testA(i int)
               |PARTITIONED BY (p int)
               |STORED AS parquet
               |LOCATION '${dir1.toURI}'""".stripMargin)
          withTempDir { dir2 =>
            sql(
              s"""
                 |CREATE EXTERNAL TABLE testB(i int)
                 |PARTITIONED BY (p int)
                 |STORED AS parquet
                 |LOCATION '${dir2.toURI}'""".stripMargin)
            executeTestBody({
              spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("testA")
              spark.range(1000).selectExpr("id as i", "id % 20 as p").write.insertInto("testB")
              val (numPartitionFetches, resWithRule) = test()
              excludePartitionPruneRule()
              val (_, resWoRule) = test()
              resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
              assert(numPartitionFetches == 23)
            })
          }
        }
      }
    }
  }

  test("partition file source pruning with multi table with reduction in hms and partition" +
    " fetch calls") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("testA").as("test2").where("test2.i < 50 and test2.p is not" +
        " null")
      val df2 = spark.table("testA").as("test3").where("test3.i < 17 and test3.p < 3")
      val df3 = spark.table("testB").as("test4").where("test4.i < 10 and test4.p is" +
        " not null")
      val df4 = spark.table("testB").as("test5").where("test5.i > 100 and test5.p > 16")
      val res = df1.join(df2, "i").join(df3, "i").join(df4, "i").collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, res)
    }
    withTable("testA") {
      withTable("testB") {
        withTempDir { dir1 =>
          sql(
            s"""
               |CREATE EXTERNAL TABLE testA(i int)
               |PARTITIONED BY (p int)
               |STORED AS parquet
               |LOCATION '${dir1.toURI}'""".stripMargin)
          withTempDir { dir2 =>
            sql(
              s"""
                 |CREATE EXTERNAL TABLE testB(i int)
                 |PARTITIONED BY (p int)
                 |STORED AS parquet
                 |LOCATION '${dir2.toURI}'""".stripMargin)
            executeTestBody({
              spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("testA")
              spark.range(1000).selectExpr("id as i", "id % 20 as p").write.insertInto("testB")
              val (numPartitionFetches, resWithRule) = test()
              excludePartitionPruneRule()
              val (_, resWoRule) = test()
              resWithRule.zip(resWoRule).foreach(tup => assertResult(tup._1)(tup._2))
              assert(numPartitionFetches == 40)
            })
          }
        }
      }
    }
  }

  test("partition pruning with common filters should reuse InMemoryFileIndex") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("test").as("test1").where("test1.p < 10 and test1.i > 5")
      val df2 = spark.table("test").as("test2").where("test2.p < 10 and test2.i > 10")
      val df = df1.join(df2, "i")
      val leaves = df.queryExecution.optimizedPlan.collectLeaves().map(lp =>
        lp.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location)
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      (numPartitionFetches, leaves)
    }
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
             |CREATE EXTERNAL TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet
             |LOCATION '${dir.toURI}'""".stripMargin)
        executeTestBody({
          spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("test")
          // Test total partitions = p1 superset of p2
          val (numPartitionFetches, leavesWithOpt) = test()
          assert(numPartitionFetches == 10)
          assert(leavesWithOpt.head eq leavesWithOpt(1))
        })
      }
    }
  }

  test("non repeated non filtered tables should remain as CatalogFileIndex") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("testA").as("test2").where("test2.i < 50 ")
      val df2 = spark.table("testB").as("test4").where("test4.i < 10 ")
      val df = df1.join(df2, "i")
      val res = df.collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      val numHiveClientCalls = HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount
      (res, numPartitionFetches, numHiveClientCalls, df.queryExecution.optimizedPlan)
    }
    withTable("testA") {
      withTable("testB") {
        withTempDir { dir1 =>
          sql(
            s"""
               |CREATE EXTERNAL TABLE testA(i int)
               |PARTITIONED BY (p int)
               |STORED AS parquet
               |LOCATION '${dir1.toURI}'""".stripMargin)
          withTempDir { dir2 =>
            sql(
              s"""
                 |CREATE EXTERNAL TABLE testB(i int)
                 |PARTITIONED BY (p int)
                 |STORED AS parquet
                 |LOCATION '${dir2.toURI}'""".stripMargin)
            executeTestBody({
              spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("testA")
              spark.range(1000).selectExpr("id as i", "id % 20 as p").write.insertInto("testB")
              val (res1, numPartitionFetchesWithOpt, numHiveClientCallsOpt, optmPlan) = test()
              excludePartitionPruneRule()
              val (res2, numPartitionFetchesWithoutOpt, numHiveClientCallsWithoutOpt, _) = test()
              res1.zip(res2).foreach(tup => assertResult(tup._1)(tup._2))
              assert(numPartitionFetchesWithoutOpt == numPartitionFetchesWithOpt)
              assert(numHiveClientCallsWithoutOpt == numHiveClientCallsOpt)
              optmPlan.collectLeaves().foreach {
                case lr: LogicalRelation =>
                  lr.relation.asInstanceOf[HadoopFsRelation].location.isInstanceOf[CatalogFileIndex]
              }
            })
          }
        }
      }
    }
  }

  test("non repeated non filtered table with filtered table") {
    val test = () => {
      HiveCatalogMetrics.reset()
      val df1 = spark.table("testA").as("test2").where("test2.i < 50 ")
      val df2 = spark.table("testB").as("test4").where("test4.p < 10 ")
      val df = df1.join(df2, "i")
      val res = df.collect().sortBy(_.getInt(0))
      val numPartitionFetches = HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount
      val numHiveClientCalls = HiveCatalogMetrics.METRIC_HIVE_CLIENT_CALLS.getCount
      (res, numPartitionFetches, numHiveClientCalls, df.queryExecution.optimizedPlan)
    }
    withTable("testA") {
      withTable("testB") {
        withTempDir { dir1 =>
          sql(
            s"""
               |CREATE EXTERNAL TABLE testA(i int)
               |PARTITIONED BY (p int)
               |STORED AS parquet
               |LOCATION '${dir1.toURI}'""".stripMargin)
          withTempDir { dir2 =>
            sql(
              s"""
                 |CREATE EXTERNAL TABLE testB(i int)
                 |PARTITIONED BY (p int)
                 |STORED AS parquet
                 |LOCATION '${dir2.toURI}'""".stripMargin)
            executeTestBody({
              spark.range(800).selectExpr("id as i", "id % 20 as p").write.insertInto("testA")
              spark.range(1000).selectExpr("id as i", "id % 20 as p").write.insertInto("testB")
              val (res1, numPartitionFetchesWithOpt, numHiveClientCallsOpt, optmPlan) = test()
              excludePartitionPruneRule()
              val (res2, numPartitionFetchesWithoutOpt, numHiveClientCallsWithoutOpt, _) = test()
              res1.zip(res2).foreach(tup => assertResult(tup._1)(tup._2))
              assert(numPartitionFetchesWithoutOpt == numPartitionFetchesWithOpt)
              assert(numHiveClientCallsWithoutOpt == numHiveClientCallsOpt)
              var foundCFI = false
              var foundPfi = false
              optmPlan.collectLeaves().foreach {
                case lr: LogicalRelation =>
                  lr.relation.asInstanceOf[HadoopFsRelation].location match {
                    case _: CatalogFileIndex => foundCFI = true
                    case _: InMemoryFileIndex => foundPfi = true
                  }
              }
              assert(foundPfi && foundCFI)
            })
          }
        }
      }
    }
  }
}
