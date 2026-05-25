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

package org.apache.spark.sql.hive.client

import java.io.{File, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.{MavenUtils, Utils}

class IsolatedClientLoaderIvySettingsSuite extends SparkFunSuite {
  override protected val enableAutoThreadAudit = false

  test("SPARK-56867: respect spark.jars.ivySettings when downloading Hive metastore jars") {
    val ivyPath = Utils.createTempDir(namePrefix = "ivy-settings-test")
    val ivySettingsFile = new File(ivyPath, "ivysettings.xml")
    val writer = new PrintWriter(ivySettingsFile)
    try {
      writer.write(
        s"""<ivysettings>
           |  <settings defaultResolver="main"/>
           |  <resolvers>
           |    <chain name="main">
           |      <ibiblio name="central" m2compatible="true"
           |               root="https://repo1.maven.org/maven2/"/>
           |    </chain>
           |  </resolvers>
           |</ivysettings>""".stripMargin)
    } finally {
      writer.close()
    }

    try {
      val sparkConf = new SparkConf()
      sparkConf.set(MavenUtils.JAR_IVY_SETTING_PATH_KEY, ivySettingsFile.getCanonicalPath)
      val hadoopConf = new Configuration()
      hadoopConf.set("datanucleus.schema.autoCreateAll", "true")
      hadoopConf.set("hive.metastore.schema.verification", "false")

      val loader = IsolatedClientLoader.forVersion(
        hiveMetastoreVersion = "2.3",
        hadoopVersion = VersionInfo.getVersion,
        sparkConf = sparkConf,
        hadoopConf = hadoopConf,
        config = HiveClientBuilder.buildConf(Map.empty),
        ivyPath = Some(ivyPath.getCanonicalPath))
      // Verify that the client was created successfully using the custom Ivy settings
      assert(loader.createClient() != null)
    } finally {
      Utils.deleteRecursively(ivyPath)
    }
  }
}
