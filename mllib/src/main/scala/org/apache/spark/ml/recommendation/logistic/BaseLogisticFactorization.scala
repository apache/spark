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
package org.apache.spark.ml.recommendation.logistic

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.recommendation.logistic.local.{ItemData, Optimizer, Opts}
import org.apache.spark.ml.recommendation.logistic.pair.LongPairMulti
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.storage.StorageLevel

private[ml] object BaseLogisticFactorization {
  private val PART_TABLE_TOTAL_SIZE = 10000000

  private def shuffle(arr: Array[Int], rnd: Random) = {
    var i = 0
    val n = arr.length
    var t = 0
    while (i < n - 1) {
      val j = i + rnd.nextInt(n - i)
      t = arr(j)
      arr(j) = arr(i)
      arr(i) = t
      i += 1
    }
    arr
  }

  def createPartitionTable(numPartitions: Int, rnd: Random): Array[Array[Int]] = {
    val nBuckets = PART_TABLE_TOTAL_SIZE / numPartitions
    val result = Array.fill(nBuckets)((0 until numPartitions).toArray)
    (0 until nBuckets).foreach(i => shuffle(result(i), rnd))
    result
  }

  def hash(i: Long, salt: Int, n: Int): Int = {
    var h = (i.hashCode.toLong << 32) | salt
    h ^= h >>> 33
    h *= 0xff51afd7ed558ccdL
    h ^= h >>> 33
    h *= 0xc4ceb9fe1a85ec53L
    h ^= h >>> 33
    (Math.abs(h) % n).toInt
  }
}

private[ml] abstract class BaseLogisticFactorization extends Serializable with Logging {

  protected var dotVectorSize: Int = 100
  protected var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025
  private var minLearningRate: Option[Double] = None
  protected var numThread: Int = 1
  private var numPartitions: Int = 1
  private var pow: Double = 0
  private var lambda: Double = 0
  protected var useBias: Boolean = false
  protected var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  protected var checkpointPath: String = _
  protected var checkpointInterval: Int = 0

  protected def gamma: Float = 1f
  protected def implicitPref: Boolean = true

  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.dotVectorSize = vectorSize
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  def setMinLearningRate(minLearningRate: Option[Double]): this.type = {
    require(minLearningRate.forall(_ > 0),
      s"Initial learning rate must be positive but got ${minLearningRate}")
    this.minLearningRate = minLearningRate
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  def setCheckpointPath(path: String): this.type = {
    this.checkpointPath = path
    this
  }

  def setCheckpointInterval(interval: Int): this.type = {
    this.checkpointInterval = interval
    this
  }

  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  def setUseBias(useBias: Boolean): this.type = {
    this.useBias = useBias
    this
  }

  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow
    this
  }

  def setLambda(lambda: Double): this.type = {
    require(lambda >= 0,
      s"Lambda must be positive but got ${lambda}")
    this.lambda = lambda
    this
  }

  def setNegative(negative: Int): this.type = {
    require(negative > 0)
    this.negative = negative
    this
  }

  def setNumThread(numThread: Int): this.type = {
    require(numThread >= 0,
      s"Number of threads ${numThread}")
    this.numThread = numThread
    this
  }

  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "SkipGram is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  protected def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(intermediateRDDStorageLevel)
    r.count()
    r
  }

  private def checkpoint(emb: RDD[ItemData],
                         path: String)(implicit sc: SparkContext): RDD[ItemData] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    if (emb != null) {
      emb.map(itemData => (itemData.t, itemData.id, itemData.cn, itemData.f))
        .toDF("t", "id", "cn", "f")
        .write.mode(SaveMode.Overwrite).parquet(path)
      emb.unpersist()
    }

    cacheAndCount(sqlc.read.parquet(path)
      .as[(Boolean, Long, Long, Array[Float])].rdd
      .map(e => new ItemData(e._1, e._2, e._3, e._4))
    )
  }

  private def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }

  protected def pairsFromSeq(sent: RDD[Array[Long]],
                             partitioner1: Partitioner,
                             partitioner2: Partitioner,
                             seed: Long): RDD[LongPairMulti] = {
    throw new NotImplementedException()
  }

  protected def pairsFromRat(sent: RDD[(Long, Long, Float)],
                             partitioner1: Partitioner,
                             partitioner2: Partitioner,
                             seed: Long): RDD[LongPairMulti] = {
    throw new NotImplementedException()
  }

  protected def initializeFromSeq(sent: RDD[Array[Long]]): RDD[ItemData] = {
    throw new NotImplementedException()
  }

  protected def initializeFromRat(sent: RDD[(Long, Long, Float)]): RDD[ItemData] = {
    throw new NotImplementedException()
  }

  protected def doFit(sent: Either[RDD[Array[Long]], RDD[(Long, Long, Float)]]): RDD[ItemData] = {
    val sparkContext = sent.fold(_.sparkContext, _.sparkContext)

    val latest = if (checkpointPath != null) {
      listFiles(checkpointPath)
        .filter(file => listFiles(checkpointPath + "/" + file).contains("_SUCCESS"))
        .filter(!_.contains("run_params")).filter(_.contains("_"))
        .map(_.split("_").map(_.toInt)).map{case Array(a, b) => (a, b)}
        .sorted.lastOption
    } else {
      None
    }

    latest.foreach(x => log.info(s"Continue training from epoch = ${x._1}, iteration = ${x._2}"))

    var emb = latest
      .map(x => checkpoint(null, checkpointPath + "/" + x._1 + "_" + x._2)(sparkContext))
      .getOrElse{cacheAndCount(sent.fold(initializeFromSeq, initializeFromRat))}

    var checkpointIter = 0
    val (startEpoch, startIter) = latest.getOrElse((0, 0))
    val cached = ArrayBuffer.empty[RDD[ItemData]]
    val partitionTable = sparkContext.broadcast(BaseLogisticFactorization
      .createPartitionTable(numPartitions, new Random(0)))

    (startEpoch until numIterations).foreach {curEpoch =>

      val partitioner1 = new HashPartitioner(numPartitions) {
        override def getPartition(item: Any): Int = {
          BaseLogisticFactorization.hash(item.asInstanceOf[Long], curEpoch, numPartitions)
        }
      }

      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val progress = (1.0 * curEpoch.toDouble * numPartitions + pI) /
          (numIterations * numPartitions)

        val curLearningRate = minLearningRate.fold(learningRate)(e =>
          Math.exp(Math.log(learningRate) -
            (Math.log(learningRate) - Math.log(e)) * progress))

        val partitioner2 = new HashPartitioner(numPartitions) {
          override def getPartition(item: Any): Int = {
            val bucket = BaseLogisticFactorization.hash(
              item.asInstanceOf[Long],
              curEpoch,
              partitionTable.value.length)
            partitionTable.value.apply(bucket).apply(pI)
          }
        }

        val partitionerKey = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

        val embLR = emb
          .keyBy(i => if (i.t == ItemData.TYPE_LEFT) {
            partitioner1.getPartition(i.id)
          } else {
            partitioner2.getPartition(i.id)
          }).partitionBy(partitionerKey).values

        val cur = sent.fold(
          pairsFromSeq(_, partitioner1, partitioner2,
            (1L * curEpoch * numPartitions + pI) * numPartitions),
          pairsFromRat(_, partitioner1, partitioner2,
            (1L * curEpoch * numPartitions + pI) * numPartitions)
        ).map(e => e.part -> e).partitionBy(partitionerKey).values

        emb = cur.zipPartitions(embLR) { case (sIt, eItLR) =>
          val sg = new Optimizer(new Opts(dotVectorSize, useBias,
            negative, pow, curLearningRate, lambda, gamma, implicitPref), eItLR)

          sg.optimize(sIt, numThread)

          log.debug(
            "LOSS: " + sg.loss.doubleValue() / sg.lossn.longValue() +
            " (" + sg.loss.doubleValue() + " / " + sg.lossn.longValue() + ")" + "\t"  +
            sg.lossReg.doubleValue() / sg.lossnReg.longValue() +
            " (" + sg.lossReg.doubleValue() + " / " + sg.lossnReg.longValue() + ")")

          sg.flush()
        }.persist(intermediateRDDStorageLevel)

        cached += emb

        if (checkpointInterval > 0 && (checkpointIter + 1) % checkpointInterval == 0) {
          emb = checkpoint(emb, checkpointPath + "/" + curEpoch + "_" + (pI + 1))(sparkContext)
          cached.foreach(_.unpersist())
          cached.clear()
        }
        checkpointIter += 1
      }
    }

    emb
  }

  def fit(dataset: DataFrame): RDD[ItemData]
}
