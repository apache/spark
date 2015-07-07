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
package org.apache.spark.shuffle.parquet

import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv}

object ParquetShuffleConfig {
  private val sparkManagerConfKey = "spark.shuffle.manager"
  private val parquetManagerAlias = "parquet"
  private val namespace = "spark.shuffle.parquet."
  private val compressionKey = namespace + "compression"
  private val blocksizeKey = namespace + "blocksize"
  private val pagesizeKey = namespace + "pagesize"
  private val enableDictionaryKey = namespace + "enabledictionary"
  private[parquet] val fallbackShuffleManager = namespace + "fallback"

  def isParquetShuffleEnabled: Boolean = {
    isParquetShuffleEnabled(SparkEnv.get.conf)
  }

  def isParquetShuffleEnabled(conf: SparkConf): Boolean = {
    val confValue = conf.get(sparkManagerConfKey, "")
    confValue == parquetManagerAlias || confValue == classOf[ParquetShuffleManager].getName
  }

  def enableParquetShuffle(): Unit = {
    enableParquetShuffle(SparkEnv.get.conf)
  }

  def enableParquetShuffle(conf: SparkConf): Unit = {
    conf.set(ParquetShuffleConfig.sparkManagerConfKey, classOf[ParquetShuffleManager].getName)
  }

  def getCompression: CompressionCodecName = {
    getCompression(SparkEnv.get.conf)
  }

  def getCompression(conf: SparkConf): CompressionCodecName = {
    val confValue = conf.get(compressionKey, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME
    } else {
      CompressionCodecName.fromConf(confValue)
    }
  }

  def getBlockSize: Int = {
    getBlockSize(SparkEnv.get.conf)
  }

  def getBlockSize(conf: SparkConf): Int = {
    val confValue = conf.get(blocksizeKey, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_BLOCK_SIZE
    } else {
      confValue.toInt
    }
  }

  def getPageSize: Int = {
    getPageSize(SparkEnv.get.conf)
  }

  def getPageSize(conf: SparkConf): Int = {
    val confValue = conf.get(pagesizeKey, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_PAGE_SIZE
    } else {
      confValue.toInt
    }
  }

  def isDictionaryEnabled: Boolean = {
    isDictionaryEnabled(SparkEnv.get.conf)
  }

  def isDictionaryEnabled(conf: SparkConf): Boolean = {
    val confValue = conf.get(enableDictionaryKey, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED
    } else {
      confValue.toBoolean
    }
  }

  def setFallbackShuffleManager(managerName: String): Unit = {
    setFallbackShuffleManager(SparkEnv.get.conf, managerName)
  }

  def setFallbackShuffleManager(conf: SparkConf, managerName: String): Unit = {
    conf.set(fallbackShuffleManager, managerName)
  }

  def getFallbackShuffleManager: ShuffleManager = {
    getFallbackShuffleManager(SparkEnv.get.conf)
  }

  def getFallbackShuffleManager(conf: SparkConf): ShuffleManager = {
    val confValue = conf.get(fallbackShuffleManager, null)
    if (confValue == null) {
      new ErrorShuffleManager
    } else {
      val fullName = SparkEnv.shuffleManagerAliases.getOrElse(confValue, confValue)
      val cls = Utils.classForName(fullName)
      cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[ShuffleManager]
    }
  }

}
