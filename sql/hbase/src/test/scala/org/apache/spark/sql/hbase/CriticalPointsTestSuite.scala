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

import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.util.{BytesUtils, HBaseKVHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ArrayBuffer

//@Ignore
class CriticalPointsTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
  val namespace = "testNamespace"
  val tableName = "testTable"
  val hbaseTableName = "ht"
  val family1 = "family1"
  val family2 = "family2"

  def partitionEquals(p1: HBasePartition, p2: HBasePartition): Boolean = {
    ((p1.start equals p2.start)
      && (p1.startInclusive equals p2.startInclusive)
      && (p1.end equals p2.end)
      && (p1.endInclusive equals p2.endInclusive))
  }

  test("Generate CP Ranges 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")
    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal(1023, IntegerType)
    val ll = GreaterThan(lll, llr)

    val lrl = lll
    val lrr = Literal(1025, IntegerType)
    val lr = LessThan(lrl, lrr)

    val l = And(ll, lr)

    val rll = lll
    val rlr = Literal(2048, IntegerType)
    val rl = GreaterThanOrEqual(rll, rlr)

    val rrl = lll
    val rrr = Literal(512, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = Or(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 3)
    assert(cprs(0).start.get == 512 && cprs(0).startInclusive
      && cprs(0).end.get == 512 && cprs(0).endInclusive)
    assert(cprs(1).start.get == 1024 && cprs(1).startInclusive
      && cprs(1).end.get == 1024 && cprs(1).endInclusive)
    assert(cprs(2).start.get == 2048 && cprs(2).startInclusive
      && cprs(2).end == None && !cprs(2).endInclusive)
  }

  test("Generate CP Ranges 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", LongType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal(1023L, LongType)
    val ll = GreaterThan(lll, llr)

    val lrl = lll
    val lrr = Literal(1024L, LongType)
    val lr = LessThanOrEqual(lrl, lrr)

    val l_0 = And(ll, lr)
    val l = Not(l_0)

    val rll = lll
    val rlr = Literal(512L, LongType)
    val rl = LessThanOrEqual(rll, rlr)

    val r = Not(rl)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 2)
    assert(cprs(0).start.get == 513L && cprs(0).startInclusive
      && cprs(0).end.get == 1023L && cprs(0).endInclusive)
    assert(cprs(1).start.get == 1025L && cprs(1).startInclusive
      && cprs(1).end == None && !cprs(1).endInclusive)
  }

  test("Generate CP Ranges 2") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal("aaa", StringType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal("bbb", StringType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rl = lll
    val rr = Literal("abc", StringType)
    val r = LessThanOrEqual(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 1)
    assert(cprs(0).start.get == "aaa" && cprs(0).startInclusive
      && cprs(0).end.get == "aaa" && cprs(0).endInclusive)
  }

  test("Generate CP Ranges for Multi-Dimension 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ KeyColumn("column3", ShortType, 2)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column5", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column3").get
    val llr = Literal(8.toShort, ShortType)
    val ll = GreaterThan(lll, llr)

    val lrl = relation.output.find(_.name == "column2").get
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = And(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal("abc", StringType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal("cba", StringType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(cprs.size == 2)
    assert(expandedCPRs.size == 2)

    val prefix0 = expandedCPRs(0).prefix
    assert(prefix0.size == 2
      && prefix0(0) ==("abc", StringType)
      && prefix0(1) ==(2048, IntegerType))
    val lastRange0 = expandedCPRs(0).lastRange
    assert(lastRange0.start.get == 9
      && lastRange0.startInclusive
      && lastRange0.end == None
      && !lastRange0.endInclusive)

    val prefix1 = expandedCPRs(1).prefix
    assert(prefix1.size == 2
      && prefix1(0) ==("cba", StringType)
      && prefix1(1) ==(2048, IntegerType))
    val lastRange1 = expandedCPRs(1).lastRange
    assert(lastRange1.start.get == 9
      && lastRange1.startInclusive
      && lastRange1.end == None
      && !lastRange1.endInclusive)
  }

  test("Generate CP Ranges for Multi-Dimension 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column5", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal("abc", StringType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal("cba", StringType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(cprs.size == 2)
    assert(expandedCPRs.size == 4)

    val prefix0 = expandedCPRs(0).prefix
    assert(prefix0.size == 1 && prefix0(0) ==("abc", StringType))
    val lastRange0 = expandedCPRs(0).lastRange
    assert(lastRange0.start.get == 8 && lastRange0.startInclusive
      && lastRange0.end.get == 8 && lastRange0.endInclusive)

    val prefix1 = expandedCPRs(1).prefix
    assert(prefix1.size == 1 && prefix1(0) ==("abc", StringType))
    val lastRange1 = expandedCPRs(1).lastRange
    assert(lastRange1.start.get == 2048 && lastRange1.startInclusive
      && lastRange1.end.get == 2048 && lastRange1.endInclusive)

    val prefix2 = expandedCPRs(2).prefix
    assert(prefix2.size == 1 && prefix2(0) ==("cba", StringType))
    val lastRange2 = expandedCPRs(2).lastRange
    assert(lastRange2.start.get == 8 && lastRange2.startInclusive
      && lastRange2.end.get == 8 && lastRange2.endInclusive)

    val prefix3 = expandedCPRs(3).prefix
    assert(prefix3.size == 1 && prefix3(0) ==("cba", StringType))
    val lastRange3 = expandedCPRs(3).lastRange
    assert(lastRange3.start.get == 2048 && lastRange3.startInclusive
      && lastRange3.end.get == 2048 && lastRange3.endInclusive)
  }

  test("Get partitions 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

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

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 4)

    val rowkey0 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(0), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(7), IntegerType))
    )

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(3), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(4), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(3), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(6), IntegerType))
    )

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    val prunedPartitions = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
    assert(prunedPartitions.size == 1 && partitionEquals(prunedPartitions(0), p6))
  }

  test("Get partitions 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

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

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

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

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    val prunedPartitions0 = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
    val prunedPartitions1 = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size, 4)
    assert(prunedPartitions0.size == 4
      && partitionEquals(prunedPartitions0(0), p3)
      && partitionEquals(prunedPartitions0(1), p4)
      && partitionEquals(prunedPartitions0(2), p5)
      && partitionEquals(prunedPartitions0(3), p6))
    assert(prunedPartitions1.size == 4
      && partitionEquals(prunedPartitions1(0), p3)
      && partitionEquals(prunedPartitions1(1), p4)
      && partitionEquals(prunedPartitions1(2), p5)
      && partitionEquals(prunedPartitions1(3), p6))
  }

  test("Get partitions 2") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = GreaterThan(lll, llr)

    val lrl = lll
    val lrr = Literal(256, IntegerType)
    val lr = LessThan(lrl, lrr)

    val l = And(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal(32, IntegerType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal(64, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 2)

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

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    val prunedPartitions0 = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
    val prunedPartitions1 = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size, 4)
    assert(prunedPartitions0.size == 3
      && partitionEquals(prunedPartitions0(0), p3)
      && partitionEquals(prunedPartitions0(1), p4)
      && partitionEquals(prunedPartitions0(2), p5))
    assert(prunedPartitions1.size == 3
      && partitionEquals(prunedPartitions1(0), p3)
      && partitionEquals(prunedPartitions1(1), p4)
      && partitionEquals(prunedPartitions1(2), p5))
  }

  test("Get partitions 3") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ KeyColumn("column3", IntegerType, 2)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column5", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(TestHbase)

    val lll = relation.output.find(_.name == "column3").get
    val llr = Literal(32, IntegerType)
    val ll = GreaterThan(lll, llr)

    val lrl = lll
    val lrr = Literal(128, IntegerType)
    val lr = LessThan(lrl, lrr)

    val l = And(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal(2, IntegerType)
    val rl = EqualTo(rll, rlr)

    val rrl = relation.output.find(_.name == "column2").get
    val rrr = Literal(8, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = And(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val cprs = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(cprs.size == 1)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 1)

    val rowkey0 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(16), IntegerType))
    )

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(32), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(64), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(128), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(256), IntegerType))
    )

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    val prunedPartitions = RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size, 4)
    assert(prunedPartitions.size == 2
      && partitionEquals(prunedPartitions(0), p3)
      && partitionEquals(prunedPartitions(1), p4))
  }
}
