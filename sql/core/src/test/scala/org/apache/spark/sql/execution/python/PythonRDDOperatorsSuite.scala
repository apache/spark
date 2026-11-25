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

package org.apache.spark.sql.execution.python

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import net.razorvine.pickle.Pickler

import org.apache.spark.SparkException
import org.apache.spark.resource.{ResourceProfile, ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests the operators of the RDD API of PySpark on Spark Connect (see PythonRDDOperators.scala)
 * that do not run Python: their partitions are the partitions of the RDDs of Spark Classic.
 */
class PythonRDDOperatorsSuite extends QueryTest with SharedSparkSession {

  private def bytes(value: String): Array[Byte] = value.getBytes(UTF_8)

  /** Local data of one binary column. */
  private def values(vs: String*): LogicalPlan =
    LocalRelation(PythonRDDOperators.valueOutput(), vs.map(v => InternalRow(bytes(v))))

  /** Local data of the keys and values of a shuffle. */
  private def pairs(kvs: (Int, String)*): LogicalPlan = LocalRelation(
    PythonRDDOperators.pairsOutput(),
    kvs.map { case (k, v) => InternalRow(k, bytes(v)) })

  private def parallelize(numSlices: Int, vs: String*): LogicalPlan =
    PythonRDDParallelize(numSlices, values(vs: _*))

  private def shuffle(numPartitions: Int, kvs: (Int, String)*): LogicalPlan =
    PythonRDDValues(PythonRDDShuffle(numPartitions, pairs(kvs: _*)))

  /** The rows of each partition of a plan, with the binary columns as strings. */
  private def partitions(plan: LogicalPlan): Seq[Seq[Seq[String]]] = {
    val rdd = Dataset.ofRows(spark, plan).queryExecution.toRdd
    rdd
      .map(row => (0 until row.numFields).map(i => new String(row.getBinary(i), UTF_8)))
      .glom()
      .collect()
      .map(_.toSeq)
      .toSeq
  }

  /** The values of each partition of a plan of one column. */
  private def valuesOf(plan: LogicalPlan): Seq[Seq[String]] = partitions(plan).map(_.map(_.head))

  /** Runs `f` with adaptive query execution enabled and disabled. */
  private def withAndWithoutAQE(f: => Unit): Unit = {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enabled)(f)
    }
  }

  test("parallelize slices the rows as SparkContext.parallelize does") {
    val vs = ('a' to 'j').map(_.toString)
    val expected = spark.sparkContext.parallelize(vs, 3).glom().collect().map(_.toSeq).toSeq
    assert(valuesOf(parallelize(3, vs: _*)) === expected)
    assert(valuesOf(parallelize(4)) === Seq.fill(4)(Seq.empty))
  }

  test("union concatenates the partitions, those of inputs without rows too") {
    withAndWithoutAQE {
      val union = PythonRDDUnion(
        PythonRDDOperators.valueOutput(),
        Seq(parallelize(2), parallelize(3, "a", "b", "c")))
      assert(valuesOf(union) === Seq(Seq(), Seq(), Seq("a"), Seq("b"), Seq("c")))
    }
  }

  test("union of the same shuffle twice keeps the partitions of both") {
    withAndWithoutAQE {
      val shuffled = shuffle(2, 0 -> "a", 1 -> "b")
      val union = PythonRDDUnion(PythonRDDOperators.valueOutput(), Seq(shuffled, shuffled))
      assert(valuesOf(union) === Seq(Seq("a"), Seq("b"), Seq("a"), Seq("b")))
    }
  }

  test("union of no inputs has no partitions, as SparkContext.emptyRDD") {
    assert(valuesOf(PythonRDDUnion(PythonRDDOperators.valueOutput(), Nil)) === Seq.empty)
  }

  test("shuffle moves the rows to the partitions of their keys and keeps empty partitions") {
    withAndWithoutAQE {
      val shuffled = shuffle(4, 0 -> "a", 2 -> "b", 0 -> "c")
      assert(valuesOf(shuffled).map(_.sorted) === Seq(Seq("a", "c"), Seq(), Seq("b"), Seq()))
      assert(valuesOf(shuffle(3)) === Seq(Seq(), Seq(), Seq()))
    }
  }

  test("a shuffle keeps the order of the rows of one input partition") {
    withAndWithoutAQE {
      val input = PythonRDDParallelize(1, pairs((0 until 100).map(i => (i % 2) -> i.toString): _*))
      val shuffled = valuesOf(PythonRDDValues(PythonRDDShuffle(2, input)))
      assert(shuffled === Seq(
        (0 until 100 by 2).map(_.toString),
        (1 until 100 by 2).map(_.toString)))
    }
  }

  test("zip pairs the rows at the same positions of the partitions") {
    val zipped = PythonRDDZipPartitions(
      concatenate = false,
      PythonRDDOperators.joinedOutput(Seq(values(), values())),
      Seq(parallelize(2, "a", "b"), parallelize(2, "c", "d")))
    assert(partitions(zipped) === Seq(Seq(Seq("a", "c")), Seq(Seq("b", "d"))))
  }

  test("zip of partitions of different numbers of rows fails as RDD.zip") {
    val zipped = PythonRDDZipPartitions(
      concatenate = false,
      PythonRDDOperators.joinedOutput(Seq(values(), values())),
      Seq(parallelize(1, "a", "b"), parallelize(1, "c")))
    val e = intercept[SparkException](partitions(zipped))
    assert(e.getMessage.contains("Can only zip RDDs with same number of elements"))
  }

  test("zip with concatenate puts the rows of the partitions one after the other") {
    withAndWithoutAQE {
      val concatenated = PythonRDDZipPartitions(
        concatenate = true,
        PythonRDDOperators.valueOutput(),
        Seq(shuffle(2, 0 -> "a", 1 -> "b"), shuffle(2, 1 -> "c"), parallelize(2)))
      assert(valuesOf(concatenated) === Seq(Seq("a"), Seq("b", "c")))
    }
  }

  test("cartesian makes a partition for each pair of partitions, as RDD.cartesian") {
    val product = PythonRDDCartesian(
      PythonRDDOperators.joinedOutput(Seq(values(), values())),
      parallelize(2, "a", "b"),
      parallelize(2, "x", "y"))
    assert(partitions(product) === Seq(
      Seq(Seq("a", "x")),
      Seq(Seq("a", "y")),
      Seq(Seq("b", "x")),
      Seq(Seq("b", "y"))))
  }

  test("coalesce groups the partitions as RDD.coalesce") {
    val coalesced = PythonRDDCoalesce(2, parallelize(4, "a", "b", "c", "d"))
    assert(valuesOf(coalesced) === Seq(Seq("a", "b"), Seq("c", "d")))
    assert(valuesOf(PythonRDDCoalesce(8, parallelize(2, "a", "b"))) === Seq(Seq("a"), Seq("b")))
    // Unlike the coalesce of a DataFrame, which makes a partition, as an RDD without partitions.
    val empty = PythonRDDUnion(PythonRDDOperators.valueOutput(), Nil)
    assert(valuesOf(PythonRDDCoalesce(1, empty)) === Seq.empty)
  }

  /** The frames of `data`, as PythonRDD writes them. */
  private def frames(data: Array[Byte]): Seq[String] = {
    val in = new DataInputStream(new ByteArrayInputStream(data))
    Iterator.continually(in.available()).takeWhile(_ > 0).map { _ =>
      val value = new Array[Byte](in.readInt())
      in.readFully(value)
      new String(value, UTF_8)
    }.toSeq
  }

  /** The partitions and the frames of the rows of PythonRDDElements. */
  private def elements(plan: LogicalPlan): Seq[(Int, Seq[String])] = {
    Dataset.ofRows(spark, plan).collect().toSeq.map { row =>
      (row.getInt(0), frames(row.getAs[Array[Byte]](1)))
    }
  }

  test("elements writes the rows of the selected partitions as PythonRDD writes elements") {
    val input = parallelize(4, "a", "b", "c", "d")
    assert(elements(PythonRDDElements(None, input)) ===
      Seq(0 -> Seq("a"), 1 -> Seq("b"), 2 -> Seq("c"), 3 -> Seq("d")))
    assert(elements(PythonRDDElements(Some(Seq(2, 0, 2)), input)) ===
      Seq(0 -> Seq("a"), 2 -> Seq("c")))
    // As SparkContext.runJob, the partitions exist.
    Seq(4, -1).foreach { p =>
      val e = intercept[IllegalArgumentException](elements(PythonRDDElements(Some(Seq(p)), input)))
      assert(e.getMessage ===
        s"Attempting to access a non-existent partition: $p. Total number of partitions: 4")
    }

    // The columns of a row in turn, and strings as their UTF-8 bytes: "e" with an acute accent
    // is two bytes.
    val accented = new String(Array(0xc3, 0xa9).map(_.toByte), UTF_8)
    val strings = LocalRelation(
      Seq(AttributeReference("s", StringType)(), AttributeReference("t", StringType)()),
      Seq(InternalRow(UTF8String.fromString("x"), UTF8String.fromString(accented))))
    assert(elements(PythonRDDElements(None, strings)) === Seq(0 -> Seq("x", accented)))
  }

  test("elements splits a partition into rows of about 1 MB") {
    val large = "x" * (PythonRDDElementsExec.ROW_SIZE / 2 + 1)
    val rows = elements(PythonRDDElements(None, parallelize(1, large, large, large)))
    assert(rows.map(_._1) === Seq(0, 0))
    assert(rows.flatMap(_._2).length === 3)
  }

  test("to DataFrame reads the internal values of rows that Python pickles in batches") {
    val pickler = new Pickler()
    val batch = pickler.dumps(Seq[Array[Any]](Array(1, "a"), Array(2, null)).asJava)
    val schema = StructType(Seq(StructField("n", IntegerType), StructField("s", StringType)))
    val df = Dataset.ofRows(
      spark,
      PythonRDDToDataFrame(schema, LocalRelation(PythonRDDOperators.valueOutput(),
        Seq(InternalRow(batch)))))
    checkAnswer(df, Seq(Row(1, "a"), Row(2, null)))
  }

  test("with resources runs the stage that computes its rows with the resource profile") {
    val profile = new ResourceProfileBuilder().require(new TaskResourceRequests().cpus(1)).build()
    spark.sparkContext.resourceProfileManager.addResourceProfile(profile)
    val stageProfiles = new ConcurrentLinkedQueue[Int]()
    val listener = new SparkListener {
      override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
        stageProfiles.add(event.stageInfo.resourceProfileId)
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      // The stage before the shuffle has the profile, and the stage after it does not.
      val input = PythonRDDWithResources(profile, pairs(0 -> "a", 1 -> "b"))
      assert(valuesOf(PythonRDDValues(PythonRDDShuffle(2, input))) === Seq(Seq("a"), Seq("b")))
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(stageProfiles.asScala.toSeq.sorted ===
        Seq(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, profile.id).sorted)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("the number of partitions is known without a job for the operators that decide it") {
    import PythonRDDOperators.numPartitions
    assert(numPartitions(parallelize(3)) === Some(3))
    assert(numPartitions(shuffle(5)) === Some(5))
    assert(numPartitions(PythonRDDUnion(
      PythonRDDOperators.valueOutput(), Seq(parallelize(2), shuffle(3)))) === Some(5))
    assert(numPartitions(PythonRDDCartesian(
      PythonRDDOperators.joinedOutput(Seq(values(), values())),
      parallelize(2),
      parallelize(3))) === Some(6))
    assert(numPartitions(PythonRDDCoalesce(2, parallelize(5))) === Some(2))
    assert(numPartitions(PythonRDDCoalesce(8, parallelize(5))) === Some(5))
    assert(numPartitions(values()) === None)
    assert(numPartitions(PythonRDDCoalesce(2, values())) === None)
    val profile = new ResourceProfileBuilder().require(new TaskResourceRequests().cpus(1)).build()
    assert(numPartitions(PythonRDDWithResources(profile, parallelize(4))) === Some(4))
  }
}
