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

package org.apache.spark.streaming.binning

import scala.Array
import org.apache.spark.streaming.binning._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import scala.language.reflectiveCalls
import scala.language.existentials
import scala.language.implicitConversions

case class ProrateGenerator(bStart: Int, bEnd: Int) {

  type Tuple = (Int, Int)

  //
  // binStart is not inclusive for a bin, hence ensuring that for a binSize 1 i.e from x to x+1 inBin maps a time to x+1
  //
  private def inBin(dur: Int) = (dur - 1) / 2 + 1

  // Event starts before BinStart
  private val evStart1 = bStart - 2
  // Event starts at BinStart
  private val evStart2 = bStart
  // Event starts in Bin
  private val evStart3 = inBin(bStart + bEnd)
  // Event starts at BinEnd
  private val evStart4 = bEnd
  // Event starts after BinEnd. No output case
  private val evStart5 = bEnd + 2

  private def _input(evStart: Int) =
    Seq(
      // No output case
      (evStart, bStart - 2),
      // No output case
      (evStart, bStart - 1),
      // No output case
      (evStart, bStart),
      (evStart, inBin(bStart + inBin(bStart + bEnd))),
      (evStart, inBin(bStart + bEnd)),
      (evStart, inBin(inBin(bStart + bEnd) + bEnd)),
      (evStart, bEnd),
      (evStart, bEnd + 1),
      (evStart, bEnd + 2),
      (evStart, bEnd + 3)
    )

  private def prorate(eDuration: Int, eRelevant: Int) =
    if (eDuration == 0)
      1.0
    else
      (eRelevant.toDouble) / eDuration

  private def valid(evStart: Int, evEnd: Int) =
    if (evStart > evEnd)
      None
    else
      Some((evStart, evEnd))

  private def inside(evStart: Int, evEnd: Int) =
    if (evStart == bEnd)
      None
    else
      Some((evStart, evEnd))

  val input = Seq(_input(evStart1) ++ _input(evStart2) ++ _input(evStart3) ++ _input(evStart4) ++ _input(evStart5))

  case class EvStart() {
    var v: Int = 0

    def set(value: Int) = {
      v = value;
      v
    }
  }

  val evStart = EvStart()

  val output =
    Seq(Seq(
      // bStart - 2
      Some(evStart.set(bStart - 2), inBin(bStart + inBin(bStart + bEnd))).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, inBin(bStart + bEnd)).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, inBin(inBin(bStart + bEnd) + bEnd)).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, bEnd).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 1).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 2).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 3).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),

      // bStart
      Some(evStart.set(bStart), inBin(bStart + inBin(bStart + bEnd))).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, inBin(bStart + bEnd)).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, inBin(inBin(bStart + bEnd) + bEnd)).map(x => (x, prorate(x._2 - x._1, x._2 - bStart))),
      Some(evStart.v, bEnd).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 1).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 2).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),
      Some(evStart.v, bEnd + 3).map(x => (x, prorate(x._2 - x._1, bEnd - bStart))),

      // inBin
      valid(evStart.set(inBin(bStart + bEnd)), inBin(bStart + inBin(bStart + bEnd))).map(x => (x, prorate(x._2 - x._1, x._2 - x._1))),
      Some(evStart.v, inBin(bStart + bEnd)).map((_, 1.0)),
      Some(evStart.v, inBin(inBin(bStart + bEnd) + bEnd)).map((_, 1.0)),
      Some(evStart.v, bEnd).map(x => (x, prorate(x._2 - x._1, bEnd - x._1))),
      inside(evStart.v, bEnd + 1).map(x => (x, prorate(x._2 - x._1, bEnd - x._1))),
      inside(evStart.v, bEnd + 2).map(x => (x, prorate(x._2 - x._1, bEnd - x._1))),
      inside(evStart.v, bEnd + 3).map(x => (x, prorate(x._2 - x._1, bEnd - x._1))),

      // bEnd
      valid(evStart.set(bEnd), inBin(bStart + inBin(bStart + bEnd))).map((_, 1.0)),
      valid(evStart.v, inBin(bStart + bEnd)).map((_, 1.0)),
      valid(evStart.v, inBin(inBin(bStart + bEnd) + bEnd)).map((_, 1.0)),
      Some(evStart.v, bEnd).map((_, 1.0))

      // bEnd + 2

    ).filter({
      case Some(x) => true
      case None => false
    }).map(_.get))

  def getStartTime(x: Tuple) = Time(x._1 * 1000)

  def getEndTime(x: Tuple) = Time(x._2 * 1000)

  def proratedStream(in: DStream[Tuple]) = {

    val b = new BinStreamer[Tuple](in, getStartTime, getEndTime)

    b.incrementalStreams(bEnd - bStart, 0)(0).getDStream
  }
}

case class DataGenerator(seed: Seq[Seq[Int]], batchSize: Int, binSize: Int) {

  type Tuple = (Int, Int, Int, Int)

  def getBin(batch: Int, _delay: Int): Option[Tuple] = {

    val start = ((batch * batchSize) / (binSize * batchSize)) * (binSize * batchSize) + _delay * binSize * batchSize

    if (start < 0)
      None
    else
      Some((start, batch * batchSize + _delay * binSize * batchSize + batchSize, batch, _delay))

  }

  val batches = seed.size

  val input = Array.tabulate(seed.size)(
    batchId => seed(batchId).map(
      _relBin => getBin(batchId, _relBin)
    ).filter({
      case Some(x) => true
      case None => false
    }).map(
      _.get
    )
  ).toSeq

  val delay = {
    var d: Int = 0
    Array.tabulate(seed.size)({
      batchId => seed(batchId).map(
        _relBin => {
          val c = (batchId * batchSize) % (binSize * batchSize) / batchSize + 1 - ((1 + _relBin) * (binSize * batchSize) / batchSize);
          d = if (c > d) c else d
        }
      )
    })
    d
  }

  val numStreams = ((binSize + delay) * batchSize - 1) / (binSize * batchSize) + 1

  val output = Array.tabulate(numStreams)(
    delayNumBins => Array.tabulate(seed.size)(
      batchId => seed(batchId).filter(
        _relBin => if (_relBin == -delayNumBins) true else false
      ).map(
        _relBin => getBin(batchId, _relBin)
      ).filter({
        case Some(x) => true;
        case None => false
      }).map(
        _.get
      ).map(
        (_, 1.0)
      )
    ).toSeq
  )

  val incrementalStreamOutput = output

  val updatedStreamOutput = for (u <- 0 until numStreams) yield {

    for (batchId <- 0 until seed.size) yield {

      var ud: Seq[(Tuple, Double)] = Seq()

      for (i <- 0 to u) {

        val floor = (batchId / binSize) * binSize

        for (l <- floor - binSize * (u - i) until {
          if (u == i) batchId + 1 else floor - binSize * (u - i - 1)
        } by 1) {
          if (l >= 0)
            ud ++= output(i)(l)
          else
            Nil
        }
      }

      ud
    }
  }

  val finalStreamOutput = updatedStreamOutput(numStreams - 1).zipWithIndex.filter(x => (x._2 + 1) % binSize == 0).unzip._1

  def getStartTime(x: Tuple) = Time(x._1 * 1000)

  def getEndTime(x: Tuple) = Time(x._2 * 1000)

  def incrementalStream(in: DStream[Tuple], delayNumBins: Int) = {

    val b = new BinStreamer[Tuple](in, getStartTime, getEndTime)

    b.incrementalStreams(binSize, delay)(delayNumBins).getDStream
  }

  def finalStream(in: DStream[Tuple]) = {

    val b = new BinStreamer[Tuple](in, getStartTime, getEndTime)

    b.finalStream(binSize, delay).getDStream
  }

  def updatedStream(in: DStream[Tuple], delayNumBins: Int) = {

    val b = new BinStreamer[Tuple](in, getStartTime, getEndTime)

    b.updatedStreams(binSize, delay)(delayNumBins).getDStream
  }

}


class BinStreamerTestSuite extends TestSuiteBase {

  implicit def toDataGenerator(seq: Seq[Seq[Int]]) = {
    new {
      var batchSz: Int = _
      var binSz: Int = _

      def batchSize(batchSz: Int) = {
        this.batchSz = batchSz;
        this
      }

      def binSize(binSz: Int) = {
        this.binSz = binSz;
        this
      }

      def generator = DataGenerator(seq, batchSz, binSz)
    }
  }

  override def framework() = "BinStreamerTestSuite"

  System.setProperty("testing.batch.duration", "1")
  
  override def batchDuration = Seconds(System.getProperty("testing.batch.duration").toLong)

  def testProrate(data: ProrateGenerator) = testOperation(
    data.input,
    (r: DStream[data.Tuple]) => data.proratedStream(r),
    data.output
  )


  test("Prorate events for bin starting at 0 and ending at 1") {

    testProrate(ProrateGenerator(0, 1))
  }

  test("Prorate events for bin starting at 0 and ending at 2") {

    testProrate(ProrateGenerator(0, 2))
  }

  test("Prorate events for bin starting at 0 and ending at 3") {

    testProrate(ProrateGenerator(0, 3))
  }


  /*
   This data set represents a streaming data. Where the sequence elements represents the batches.
   Each batch is a sequence of numbers which represents records pertaining to the
   bin relative to this batch. So 0 represents data belonging to bin corresponding to this batch.
   -1 represents the bin prior to the bin belonging to this batch. And so on ...

   This data set is an input to a DataGenerator which provides the input and the output, which is
   can then tested for the various binning cases.

   The data set generates records which fit the complete bin. The tuple generated is (start,end)
   where start is the binStart in Seconds and the end is the binStart + relative batch number for a bin.
   In addition it also has a record which identifies batch in with it was generated.
   */

  val dataSet = Seq(
    Seq(),
    Seq(0, -1),
    Seq(0, -1),
    Seq(0, -1),
    Seq(0, -1),
    Seq(0, -1),
    Seq(0, -2),
    Seq(),
    Seq(),
    Seq(),
    Seq(),
    Seq(),
    Seq()
  )

  for (binSize <- 1 to 4; batchSize <- 1 to 3) {

    val dataGenerator = dataSet.batchSize(batchSize).binSize(binSize).generator

    for (delayNumBins <- 0 until dataGenerator.numStreams) {

      test("Incremental BinStream(" + delayNumBins + "), for batchSize(" + batchSize + "), binSize(" + binSize + ")") {

        System.setProperty("testing.batch.duration", dataGenerator.batchSize.toString())

        testOperation(
          dataGenerator.input,
          (r: DStream[dataGenerator.Tuple]) => dataGenerator.incrementalStream(r, delayNumBins),
          dataGenerator.incrementalStreamOutput(delayNumBins)
        )
      }

      test("Updated BinStream(" + delayNumBins + "),     for batchSize(" + batchSize + "), binSize(" + binSize + ")") {

        System.setProperty("testing.batch.duration", dataGenerator.batchSize.toString())

        testOperation(
          dataGenerator.input,
          (r: DStream[dataGenerator.Tuple]) => dataGenerator.updatedStream(r, delayNumBins),
          dataGenerator.updatedStreamOutput(delayNumBins)
        )
      }

    }
    test("Final BinStream,          for batchSize(" + batchSize + "), binSize(" + binSize + ")") {

      System.setProperty("testing.batch.duration", dataGenerator.batchSize.toString())

      testOperation(
        dataGenerator.input,
        (r: DStream[dataGenerator.Tuple]) => dataGenerator.finalStream(r),
        dataGenerator.finalStreamOutput,
        dataGenerator.batches,
        false
      )
    }

  }

}

