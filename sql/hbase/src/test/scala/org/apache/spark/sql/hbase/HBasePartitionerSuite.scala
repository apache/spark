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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.util.{BytesUtils, HBaseKVHelper}

import scala.collection.mutable.ArrayBuffer

class HBasePartitionerSuite extends HBaseIntegrationTestBase {
  val sc = TestHbase.sparkContext

  test("test hbase partitioner") {
    val data = (1 to 40).map { r =>
      val rowKey = Bytes.toBytes(r)
      (rowKey, r)
    }
    val rdd = TestHbase.sparkContext.parallelize(data, 4)
    val splitKeys = (1 to 40).filter(_ % 5 == 0).filter(_ != 40).map { r =>
      Bytes.toBytes(r)
    }
    val partitioner = new HBasePartitioner(splitKeys.toArray)
    val shuffled =
      new ShuffledRDD[HBaseRawType, Int, Int](rdd, partitioner)

    val groups = shuffled.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(x => (x._2, idx))
    }.collect()
    assert(groups.size == 40)
    assert(groups.map(_._2).toSet.size == 8)
    groups.foreach { r =>
      assert(r._1 > 5 * r._2 && r._1 <= 5 * (1 + r._2))
    }
  }

  test("empty string row key encode / decode") {
    val rowkey = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(DoubleType).toBytes(123.456), DoubleType),
        (BytesUtils.create(StringType).toBytes("abcdef"), StringType),
        (BytesUtils.create(StringType).toBytes(""), StringType),
        (BytesUtils.create(IntegerType).toBytes(1234), IntegerType))
    )

    assert(rowkey.length === 8 + 6 + 1 + 1 + 4)

    val keys = HBaseKVHelper.decodingRawKeyColumns(rowkey,
      Seq(KeyColumn("col1", DoubleType, 0), KeyColumn("col2", StringType, 1),
        KeyColumn("col3", StringType, 2), KeyColumn("col4", IntegerType, 3)))

    assert(BytesUtils.toDouble(rowkey, keys(0)._1) === 123.456)
    assert(BytesUtils.toString(rowkey, keys(1)._1, keys(1)._2) === "abcdef")
    assert(BytesUtils.toString(rowkey, keys(2)._1, keys(2)._2) === "")
    assert(BytesUtils.toInt(rowkey, keys(3)._1) === 1234)
  }

  test("row key encode / decode") {
    val rowkey = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(DoubleType).toBytes(123.456), DoubleType),
        (BytesUtils.create(StringType).toBytes("abcdef"), StringType),
        (BytesUtils.create(IntegerType).toBytes(1234), IntegerType))
    )

    assert(rowkey.length === 8 + 6 + 1 + 4)

    val keys = HBaseKVHelper.decodingRawKeyColumns(rowkey,
      Seq(KeyColumn("col1", DoubleType, 0), KeyColumn("col2", StringType, 1),
        KeyColumn("col3", IntegerType, 2)))

    assert(BytesUtils.toDouble(rowkey, keys(0)._1) === 123.456)
    assert(BytesUtils.toString(rowkey, keys(1)._1, keys(1)._2) === "abcdef")
    assert(BytesUtils.toInt(rowkey, keys(2)._1) === 1234)
  }

  test("test computePredicate in HBasePartition") {
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "ht"
    val family1 = "family1"
    val family2 = "family2"

    val hbaseContext = new HBaseSQLContext(sc)

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal(32, IntegerType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal(1024, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      result.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 4)

    val rowkey0 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(1), IntegerType))
    )

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(32), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(16), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(64), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(128), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1024), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(256), IntegerType))
    )

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), None, pred, relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), None, pred, relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), None, pred, relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), None, pred, relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), None, pred, relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, None, pred, relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    val predicate1 = p1.computePredicate(relation)
    assert(predicate1.toString == "Some(false)")

    val predicate2 = p2.computePredicate(relation)
    assert(predicate2.toString == "Some(false)")

    def checkEqualToNode(x: Expression, leftExpected: String, rightExpected: String): Boolean = {
      x match {
        case EqualTo(left, right) => (left.asInstanceOf[AttributeReference].name.equals(leftExpected)
          && right.toString.equals(rightExpected))
        case _ => false
      }
    }

    val predicate3 = p3.computePredicate(relation).get
    assert(predicate3.isInstanceOf[And])
    assert(predicate3.asInstanceOf[And].left.isInstanceOf[Or])
    assert(checkEqualToNode(predicate3.asInstanceOf[And].left.asInstanceOf[Or].left, "column2", "8"))
    assert(checkEqualToNode(predicate3.asInstanceOf[And].left.asInstanceOf[Or].right, "column2", "2048"))
    assert(checkEqualToNode(predicate3.asInstanceOf[And].right, "column1", "32"))

    val predicate4 = p4.computePredicate(relation).get
    assert(predicate4.isInstanceOf[And])
    assert(predicate4.asInstanceOf[And].left.isInstanceOf[Or])
    assert(checkEqualToNode(predicate4.asInstanceOf[And].left.asInstanceOf[Or].left, "column2", "8"))
    assert(checkEqualToNode(predicate4.asInstanceOf[And].left.asInstanceOf[Or].right, "column2", "2048"))
    assert(checkEqualToNode(predicate4.asInstanceOf[And].right, "column1", "32"))

    val predicate5 = p5.computePredicate(relation).get
    assert(predicate5.isInstanceOf[And])
    assert(predicate5.asInstanceOf[And].left.isInstanceOf[Or])
    assert(checkEqualToNode(predicate5.asInstanceOf[And].left.asInstanceOf[Or].left, "column2", "8"))
    assert(checkEqualToNode(predicate5.asInstanceOf[And].left.asInstanceOf[Or].right, "column2", "2048"))
    assert(checkEqualToNode(predicate5.asInstanceOf[And].right, "column1", "1024"))

    val predicate6 = p6.computePredicate(relation).get
    assert(predicate6.isInstanceOf[And])
    assert(predicate6.asInstanceOf[And].left.isInstanceOf[Or])
    assert(checkEqualToNode(predicate6.asInstanceOf[And].left.asInstanceOf[Or].left, "column2", "8"))
    assert(checkEqualToNode(predicate6.asInstanceOf[And].left.asInstanceOf[Or].right, "column2", "2048"))
    assert(checkEqualToNode(predicate6.asInstanceOf[And].right, "column1", "1024"))
  }

  test("test k = 8 OR k > 8 (k is int)") {
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "ht"
    val family1 = "family1"
    val family2 = "family2"

    val hbaseContext = new HBaseSQLContext(sc)

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(8, IntegerType)
    val lr = GreaterThan(lrl, lrr)

    val l = Or(ll, lr)
    val pred = Some(l)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)
  }

  test("test k < 8 AND k > 8 (k is int)") {
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "ht"
    val family1 = "family1"
    val family2 = "family2"

    val hbaseContext = new HBaseSQLContext(sc)

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal(8, IntegerType)
    val ll = LessThan(lll, llr)

    val lrl = lll
    val lrr = Literal(8, IntegerType)
    val lr = GreaterThan(lrl, lrr)

    val l = And(ll, lr)
    val pred = Some(l)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 0)
  }
}
