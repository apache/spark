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

import scala.language.existentials
import scala.util.Random

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.sql.SparkSession

private object RecursiveFlag {

  /**
   * Sets a value of spark recursive flag.
   * If value is a None, it unsets the flag.
   *
   * @param value value to set
   * @param spark existing spark session
   * @return previous value of this flag
   */
  def setRecursiveFlag(value: Option[String], spark: SparkSession): Option[String] = {
    val flagName = FileInputFormat.INPUT_DIR_RECURSIVE
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val old = Option(hadoopConf.get(flagName))

    value match {
      case Some(v) => hadoopConf.set(flagName, v)
      case None => hadoopConf.unset(flagName)
    }

    old
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
    }
  }

  override def accept(path: Path): Boolean = {
    // Note: checking fileSystem.isDirectory is very slow here, so we use basic rules instead
    !SamplePathFilter.isFile(path) || random.nextDouble() < sampleRatio
  }
}

private object SamplePathFilter {
  val ratioParam = "sampleRatio"

  def isFile(path: Path): Boolean = FilenameUtils.getExtension(path.toString) != ""

  /**
   * Sets HDFS PathFilter
   *
   * @param value Filter class that is passed to HDFS
   * @param sampleRatio Fraction of the files that the filter picks
   * @param spark Existing Spark session
   * @return Returns the previous HDFS path filter
   */
  def setPathFilter(value: Option[Class[_]],
                    sampleRatio: Double,
                    spark: SparkSession): Option[Class[_]] = {
    val flagName = FileInputFormat.PATHFILTER_CLASS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val old = Option(hadoopConf.getClass(flagName, null))
    hadoopConf.setDouble(SamplePathFilter.ratioParam, sampleRatio)

    value match {
      case Some(v) => hadoopConf.setClass(flagName, v, classOf[PathFilter])
      case None => hadoopConf.unset(flagName)
    }
    old
  }

  /**
   * Unsets HDFS PathFilter
   *
   * @param value Filter class to restore to HDFS
   * @param spark Existing Spark session
   */
  def unsetPathFilter(value: Option[Class[_]], spark: SparkSession): Unit = {
    val flagName = FileInputFormat.PATHFILTER_CLASS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.unset(SamplePathFilter.ratioParam)

    value match {
      case Some(v) => hadoopConf.setClass(flagName, v, classOf[PathFilter])
      case None => hadoopConf.unset(flagName)
    }
  }
}
