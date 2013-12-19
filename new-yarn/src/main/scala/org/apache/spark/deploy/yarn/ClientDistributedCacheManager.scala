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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.util.{Records, ConverterUtils}

import org.apache.spark.Logging 

import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Map


/** Client side methods to setup the Hadoop distributed cache */
class ClientDistributedCacheManager() extends Logging {
  private val distCacheFiles: Map[String, Tuple3[String, String, String]] = 
    LinkedHashMap[String, Tuple3[String, String, String]]()
  private val distCacheArchives: Map[String, Tuple3[String, String, String]] = 
    LinkedHashMap[String, Tuple3[String, String, String]]()


  /**
   * Add a resource to the list of distributed cache resources. This list can
   * be sent to the ApplicationMaster and possibly the workers so that it can 
   * be downloaded into the Hadoop distributed cache for use by this application.
   * Adds the LocalResource to the localResources HashMap passed in and saves 
   * the stats of the resources to they can be sent to the workers and verified.
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
      appMasterOnly: Boolean = false) = {
    val destStatus = fs.getFileStatus(destPath)
    val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    amJarRsrc.setType(resourceType)
    val visibility = getVisibility(conf, destPath.toUri(), statCache)
    amJarRsrc.setVisibility(visibility)
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(destPath))
    amJarRsrc.setTimestamp(destStatus.getModificationTime())
    amJarRsrc.setSize(destStatus.getLen())
    if (link == null || link.isEmpty()) throw new Exception("You must specify a valid link name")
    localResources(link) = amJarRsrc
    
    if (appMasterOnly == false) {
      val uri = destPath.toUri()
      val pathURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, link)
      if (resourceType == LocalResourceType.FILE) {
        distCacheFiles(pathURI.toString()) = (destStatus.getLen().toString(), 
          destStatus.getModificationTime().toString(), visibility.name())
      } else {
        distCacheArchives(pathURI.toString()) = (destStatus.getLen().toString(), 
          destStatus.getModificationTime().toString(), visibility.name())
      }
    }
  }

  /**
   * Adds the necessary cache file env variables to the env passed in
   * @param env
   */
  def setDistFilesEnv(env: Map[String, String]) = {
    val (keys, tupleValues) = distCacheFiles.unzip
    val (sizes, timeStamps, visibilities) = tupleValues.unzip3

    if (keys.size > 0) {
      env("SPARK_YARN_CACHE_FILES") = keys.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_FILES_TIME_STAMPS") = 
        timeStamps.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_FILES_FILE_SIZES") = 
        sizes.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_FILES_VISIBILITIES") = 
        visibilities.reduceLeft[String] { (acc,n) => acc + "," + n }
    }
  }

  /**
   * Adds the necessary cache archive env variables to the env passed in
   * @param env
   */
  def setDistArchivesEnv(env: Map[String, String]) = {
    val (keys, tupleValues) = distCacheArchives.unzip
    val (sizes, timeStamps, visibilities) = tupleValues.unzip3

    if (keys.size > 0) {
      env("SPARK_YARN_CACHE_ARCHIVES") = keys.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS") = 
        timeStamps.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES") =
        sizes.reduceLeft[String] { (acc,n) => acc + "," + n }
      env("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES") = 
        visibilities.reduceLeft[String] { (acc,n) => acc + "," + n }
    }
  }

  /**
   * Returns the local resource visibility depending on the cache file permissions
   * @param conf
   * @param uri
   * @param statCache
   * @return LocalResourceVisibility
   */
  def getVisibility(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]):
      LocalResourceVisibility = {
    if (isPublic(conf, uri, statCache)) {
      return LocalResourceVisibility.PUBLIC 
    } 
    return LocalResourceVisibility.PRIVATE
  }

  /**
   * Returns a boolean to denote whether a cache file is visible to all(public)
   * or not
   * @param conf
   * @param uri
   * @param statCache
   * @return true if the path in the uri is visible to all, false otherwise
   */
  def isPublic(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]): Boolean = {
    val fs = FileSystem.get(uri, conf)
    val current = new Path(uri.getPath())
    //the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, current, FsAction.READ, statCache)) {
      return false
    }
    return ancestorsHaveExecutePermissions(fs, current.getParent(), statCache)
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse
   * the directory heirarchy to the given path)
   * @param fs
   * @param path
   * @param statCache
   * @return true if all ancestors have the 'execute' permission set for all users
   */
  def ancestorsHaveExecutePermissions(fs: FileSystem, path: Path, 
      statCache: Map[URI, FileStatus]): Boolean =  {
    var current = path
    while (current != null) {
      //the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
        return false
      }
      current = current.getParent()
    }
    return true
  }

  /**
   * Checks for a given path whether the Other permissions on it 
   * imply the permission in the passed FsAction
   * @param fs
   * @param path
   * @param action
   * @param statCache
   * @return true if the path in the uri is visible to all, false otherwise
   */
  def checkPermissionOfOther(fs: FileSystem, path: Path,
      action: FsAction, statCache: Map[URI, FileStatus]): Boolean = {
    val status = getFileStatus(fs, path.toUri(), statCache)
    val perms = status.getPermission()
    val otherAction = perms.getOtherAction()
    if (otherAction.implies(action)) {
      return true
    }
    return false
  }

  /**
   * Checks to see if the given uri exists in the cache, if it does it 
   * returns the existing FileStatus, otherwise it stats the uri, stores
   * it in the cache, and returns the FileStatus.
   * @param fs
   * @param uri
   * @param statCache
   * @return FileStatus
   */
  def getFileStatus(fs: FileSystem, uri: URI, statCache: Map[URI, FileStatus]): FileStatus = {
    val stat = statCache.get(uri) match {
      case Some(existstat) => existstat
      case None => 
        val newStat = fs.getFileStatus(new Path(uri))
        statCache.put(uri, newStat)
        newStat
    }
    return stat
  }
}
