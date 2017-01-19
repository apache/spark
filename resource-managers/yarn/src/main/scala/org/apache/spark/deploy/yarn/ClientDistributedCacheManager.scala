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

package org.apache.spark.deploy.yarn

import java.net.URI

import scala.collection.mutable.{HashMap, ListBuffer, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging

private case class CacheEntry(
  uri: URI,
  size: Long,
  modTime: Long,
  visibility: LocalResourceVisibility,
  resType: LocalResourceType)

/** Client side methods to setup the Hadoop distributed cache */
private[spark] class ClientDistributedCacheManager() extends Logging {

  private val distCacheEntries = new ListBuffer[CacheEntry]()

  /**
   * Add a resource to the list of distributed cache resources. This list can
   * be sent to the ApplicationMaster and possibly the executors so that it can
   * be downloaded into the Hadoop distributed cache for use by this application.
   * Adds the LocalResource to the localResources HashMap passed in and saves
   * the stats of the resources to they can be sent to the executors and verified.
   *
   * @param fs FileSystem
   * @param conf Configuration
   * @param destPath path to the resource
   * @param localResources localResource hashMap to insert the resource into
   * @param resourceType LocalResourceType
   * @param link link presented in the distributed cache to the destination
   * @param statCache cache to store the file/directory stats
   * @param appMasterOnly Whether to only add the resource to the app master
   */
  def addResource(
      fs: FileSystem,
      conf: Configuration,
      destPath: Path,
      localResources: HashMap[String, LocalResource],
      resourceType: LocalResourceType,
      link: String,
      statCache: Map[URI, FileStatus],
      appMasterOnly: Boolean = false): Unit = {
    val destStatus = fs.getFileStatus(destPath)
    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    amJarRsrc.setType(resourceType)
    val visibility = getVisibility(conf, destPath.toUri(), statCache)
    amJarRsrc.setVisibility(visibility)
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(destPath))
    amJarRsrc.setTimestamp(destStatus.getModificationTime())
    amJarRsrc.setSize(destStatus.getLen())
    require(link != null && link.nonEmpty, "You must specify a valid link name.")
    localResources(link) = amJarRsrc

    if (!appMasterOnly) {
      val uri = destPath.toUri()
      val pathURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, link)
      distCacheEntries += CacheEntry(pathURI, destStatus.getLen(), destStatus.getModificationTime(),
        visibility, resourceType)
    }
  }

  /**
   * Writes down information about cached files needed in executors to the given configuration.
   */
  def updateConfiguration(conf: SparkConf): Unit = {
    conf.set(CACHED_FILES, distCacheEntries.map(_.uri.toString))
    conf.set(CACHED_FILES_SIZES, distCacheEntries.map(_.size))
    conf.set(CACHED_FILES_TIMESTAMPS, distCacheEntries.map(_.modTime))
    conf.set(CACHED_FILES_VISIBILITIES, distCacheEntries.map(_.visibility.name()))
    conf.set(CACHED_FILES_TYPES, distCacheEntries.map(_.resType.name()))
  }

  /**
   * Returns the local resource visibility depending on the cache file permissions
   * @return LocalResourceVisibility
   */
  private[yarn] def getVisibility(
      conf: Configuration,
      uri: URI,
      statCache: Map[URI, FileStatus]): LocalResourceVisibility = {
    if (isPublic(conf, uri, statCache)) {
      LocalResourceVisibility.PUBLIC
    } else {
      LocalResourceVisibility.PRIVATE
    }
  }

  /**
   * Returns a boolean to denote whether a cache file is visible to all (public)
   * @return true if the path in the uri is visible to all, false otherwise
   */
  private def isPublic(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]): Boolean = {
    val fs = FileSystem.get(uri, conf)
    val current = new Path(uri.getPath())
    // the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, current, FsAction.READ, statCache)) {
      return false
    }
    ancestorsHaveExecutePermissions(fs, current.getParent(), statCache)
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse
   * the directory hierarchy to the given path)
   * @return true if all ancestors have the 'execute' permission set for all users
   */
  private def ancestorsHaveExecutePermissions(
      fs: FileSystem,
      path: Path,
      statCache: Map[URI, FileStatus]): Boolean = {
    var current = path
    while (current != null) {
      // the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
        return false
      }
      current = current.getParent()
    }
    true
  }

  /**
   * Checks for a given path whether the Other permissions on it
   * imply the permission in the passed FsAction
   * @return true if the path in the uri is visible to all, false otherwise
   */
  private def checkPermissionOfOther(
      fs: FileSystem,
      path: Path,
      action: FsAction,
      statCache: Map[URI, FileStatus]): Boolean = {
    val status = getFileStatus(fs, path.toUri(), statCache)
    val perms = status.getPermission()
    val otherAction = perms.getOtherAction()
    otherAction.implies(action)
  }

  /**
   * Checks to see if the given uri exists in the cache, if it does it
   * returns the existing FileStatus, otherwise it stats the uri, stores
   * it in the cache, and returns the FileStatus.
   * @return FileStatus
   */
  private[yarn] def getFileStatus(
      fs: FileSystem,
      uri: URI,
      statCache: Map[URI, FileStatus]): FileStatus = {
    val stat = statCache.get(uri) match {
      case Some(existstat) => existstat
      case None =>
        val newStat = fs.getFileStatus(new Path(uri))
        statCache.put(uri, newStat)
        newStat
    }
    stat
  }
}
