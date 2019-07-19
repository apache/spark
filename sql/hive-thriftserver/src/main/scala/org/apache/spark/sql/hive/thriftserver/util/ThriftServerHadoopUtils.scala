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

package org.apache.spark.sql.hive.thriftserver.util

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf

private[hive] object ThriftServerHadoopUtils {

  private val FILESYSTEMS_TO_ACCESS = "spark.yarn.access.hadoopFileSystems"

  private val STAGING_DIR = "spark.yarn.stagingDir"

  def hadoopFSsToAccess(sparkConf: SparkConf,
                        hadoopConf: Configuration): Set[FileSystem] = {
    val filesystemsToAccess = sparkConf.getOption(FILESYSTEMS_TO_ACCESS)
    val requestAllDelegationTokens = filesystemsToAccess.isEmpty

    val stagingFS = sparkConf.getOption(STAGING_DIR)
      .map(new Path(_).getFileSystem(hadoopConf))
      .getOrElse(FileSystem.get(hadoopConf))

    // Add the list of available namenodes for all namespaces in HDFS federation.
    // If ViewFS is enabled, this is skipped as ViewFS already handles delegation tokens for its
    // namespaces.
    val hadoopFilesystems = if (!requestAllDelegationTokens || stagingFS.getScheme == "viewfs") {
      filesystemsToAccess.map(new Path(_).getFileSystem(hadoopConf)).toSet
    } else {
      val nameservices = hadoopConf.getTrimmedStrings("dfs.nameservices")
      // Retrieving the filesystem for the nameservices where HA is not enabled
      val filesystemsWithoutHA = nameservices.flatMap { ns =>
        Option(hadoopConf.get(s"dfs.namenode.rpc-address.$ns")).map { nameNode =>
          new Path(s"hdfs://$nameNode").getFileSystem(hadoopConf)
        }
      }
      // Retrieving the filesystem for the nameservices where HA is enabled
      val filesystemsWithHA = nameservices.flatMap { ns =>
        Option(hadoopConf.get(s"dfs.ha.namenodes.$ns")).map { _ =>
          new Path(s"hdfs://$ns").getFileSystem(hadoopConf)
        }
      }
      (filesystemsWithoutHA ++ filesystemsWithHA).toSet
    }

    hadoopFilesystems + stagingFS
  }

  def doAs[T](ugi: UserGroupInformation)(f: () => T): T = {
    ugi.doAs(new PrivilegedExceptionAction[T] {
      override def run(): T = f()
    })
  }

}