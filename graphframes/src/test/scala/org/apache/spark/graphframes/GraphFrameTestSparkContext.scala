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

package org.apache.spark.graphframes

import java.io.File
import java.nio.file.Files

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.util.SparkFileUtils

trait GraphFrameTestSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _
  @transient var sparkMajorVersion: Int = _
  @transient var sparkMinorVersion: Int = _

  // Inspired by https://stackoverflow.com/a/59377177
  protected def sparkSession: SparkSession = spark
  protected lazy val sqlImplicits: SQLImplicits = self.sparkSession.implicits

  /** Check if current spark version is at least of the provided minimum version */
  def isLaterVersion(minVersion: String): Boolean = {
    val (minMajorVersion, minMinorVersion) = TestUtils.majorMinorVersion(minVersion)
    if (sparkMajorVersion != minMajorVersion) {
      return sparkMajorVersion > minMajorVersion
    } else {
      return sparkMinorVersion >= minMinorVersion
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("GraphFramesUnitTest")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    val checkpointDir = Files.createTempDirectory(this.getClass.getName).toString
    spark.sparkContext.setCheckpointDir(checkpointDir)
    sc = spark.sparkContext
    sqlContext = spark.sqlContext

    val (verMajor, verMinor) = TestUtils.majorMinorVersion(sc.version)
    sparkMajorVersion = verMajor
    sparkMinorVersion = verMinor
  }

  override def afterAll(): Unit = {
    val checkpointDir = sc.getCheckpointDir
    if (spark != null) {
      spark.stop()
    }
    spark = null
    sc = null

    checkpointDir.foreach { dir =>
      SparkFileUtils.deleteQuietly(new File(dir))
    }
    super.afterAll()
  }
}
