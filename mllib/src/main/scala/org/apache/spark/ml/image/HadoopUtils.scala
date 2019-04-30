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

package org.apache.spark.ml.image

import scala.util.Random

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.sql.SparkSession

private object RecursiveFlag {
  /**
   * Sets the spark recursive flag and then restores it.
   *
   * @param value Value to set
   * @param spark Existing spark session
   * @param f The function to evaluate after setting the flag
   * @return Returns the evaluation result T of the function
   */
  def withRecursiveFlag[T](value: Boolean, spark: SparkSession)(f: => T): T = {
    val flagName = FileInputFormat.INPUT_DIR_RECURSIVE
    // scalastyle:off hadoopconfiguration
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
    val old = Option(hadoopConf.get(flagName))
    hadoopConf.set(flagName, value.toString)
    try f finally {
      // avoid false positive of DLS_DEAD_LOCAL_STORE_IN_RETURN by SpotBugs
      if (old.isDefined) {
        hadoopConf.set(flagName, old.get)
      } else {
        hadoopConf.unset(flagName)
      }
    }
  }
}

/**
 * Filter that allows loading a fraction of HDFS files.
 */
private class SamplePathFilter extends Configured with PathFilter {
  val random = new Random()

  // Ratio of files to be read from disk
  var sampleRatio: Double = 1

  override def setConf(conf: Configuration): Unit = {
    if (conf != null) {
      sampleRatio = conf.getDouble(SamplePathFilter.ratioParam, 1)
      val seed = conf.getLong(SamplePathFilter.seedParam, 0)
      random.setSeed(seed)
    }
  }

  override def accept(path: Path): Boolean = {
    // Note: checking fileSystem.isDirectory is very slow here, so we use basic rules instead
    !SamplePathFilter.isFile(path) || random.nextDouble() < sampleRatio
  }
}

private object SamplePathFilter {
  val ratioParam = "sampleRatio"
  val seedParam = "seed"

  def isFile(path: Path): Boolean = FilenameUtils.getExtension(path.toString) != ""

  /**
   * Sets the HDFS PathFilter flag and then restores it.
   * Only applies the filter if sampleRatio is less than 1.
   *
   * @param sampleRatio Fraction of the files that the filter picks
   * @param spark Existing Spark session
   * @param seed Random number seed
   * @param f The function to evaluate after setting the flag
   * @return Returns the evaluation result T of the function
   */
  def withPathFilter[T](
      sampleRatio: Double,
      spark: SparkSession,
      seed: Long)(f: => T): T = {
    val sampleImages = sampleRatio < 1
    if (sampleImages) {
      val flagName = FileInputFormat.PATHFILTER_CLASS
      // scalastyle:off hadoopconfiguration
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // scalastyle:on hadoopconfiguration
      val old = hadoopConf.getClass(flagName, null)
      hadoopConf.setDouble(SamplePathFilter.ratioParam, sampleRatio)
      hadoopConf.setLong(SamplePathFilter.seedParam, seed)
      hadoopConf.setClass(flagName, classOf[SamplePathFilter], classOf[PathFilter])
      try f finally {
        hadoopConf.unset(SamplePathFilter.ratioParam)
        hadoopConf.unset(SamplePathFilter.seedParam)
        old match {
          case null => hadoopConf.unset(flagName)
          case v => hadoopConf.setClass(flagName, v, classOf[PathFilter])
        }
      }
    } else {
      f
    }
  }
}
