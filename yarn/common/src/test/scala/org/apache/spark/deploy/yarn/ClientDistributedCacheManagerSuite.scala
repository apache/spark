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

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.when

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.util.{Records, ConverterUtils}

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map


class ClientDistributedCacheManagerSuite extends FunSuite with MockitoSugar {

  class MockClientDistributedCacheManager extends ClientDistributedCacheManager {
    override def getVisibility(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]): 
        LocalResourceVisibility = {
      return LocalResourceVisibility.PRIVATE
    }
  }
  
  test("test getFileStatus empty") {
    val distMgr = new ClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val uri = new URI("/tmp/testing")
    when(fs.getFileStatus(new Path(uri))).thenReturn(new FileStatus())
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    val stat = distMgr.getFileStatus(fs, uri, statCache)
    assert(stat.getPath() === null)
  }

  test("test getFileStatus cached") {
    val distMgr = new ClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val uri = new URI("/tmp/testing")
    val realFileStatus = new FileStatus(10, false, 1, 1024, 10, 10, null, "testOwner", 
      null, new Path("/tmp/testing"))
    when(fs.getFileStatus(new Path(uri))).thenReturn(new FileStatus())
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus](uri -> realFileStatus)
    val stat = distMgr.getFileStatus(fs, uri, statCache)
    assert(stat.getPath().toString() === "/tmp/testing")
  }

  test("test addResource") {
    val distMgr = new MockClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.invalid.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    when(fs.getFileStatus(destPath)).thenReturn(new FileStatus())

    distMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.FILE, "link", 
      statCache, false)
    val resource = localResources("link")
    assert(resource.getVisibility() === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(resource.getResource()) === destPath)
    assert(resource.getTimestamp() === 0)
    assert(resource.getSize() === 0)
    assert(resource.getType() === LocalResourceType.FILE)

    val env = new HashMap[String, String]()
    distMgr.setDistFilesEnv(env)
    assert(env("SPARK_YARN_CACHE_FILES") === "file:/foo.invalid.com:8080/tmp/testing#link")
    assert(env("SPARK_YARN_CACHE_FILES_TIME_STAMPS") === "0")
    assert(env("SPARK_YARN_CACHE_FILES_FILE_SIZES") === "0")
    assert(env("SPARK_YARN_CACHE_FILES_VISIBILITIES") === LocalResourceVisibility.PRIVATE.name())

    distMgr.setDistArchivesEnv(env)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES") === None)

    //add another one and verify both there and order correct
    val realFileStatus = new FileStatus(20, false, 1, 1024, 10, 30, null, "testOwner", 
      null, new Path("/tmp/testing2"))
    val destPath2 = new Path("file:///foo.invalid.com:8080/tmp/testing2")
    when(fs.getFileStatus(destPath2)).thenReturn(realFileStatus)
    distMgr.addResource(fs, conf, destPath2, localResources, LocalResourceType.FILE, "link2", 
      statCache, false)
    val resource2 = localResources("link2")
    assert(resource2.getVisibility() === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(resource2.getResource()) === destPath2)
    assert(resource2.getTimestamp() === 10)
    assert(resource2.getSize() === 20)
    assert(resource2.getType() === LocalResourceType.FILE)

    val env2 = new HashMap[String, String]()
    distMgr.setDistFilesEnv(env2)
    val timestamps = env2("SPARK_YARN_CACHE_FILES_TIME_STAMPS").split(',')
    val files = env2("SPARK_YARN_CACHE_FILES").split(',') 
    val sizes = env2("SPARK_YARN_CACHE_FILES_FILE_SIZES").split(',')
    val visibilities = env2("SPARK_YARN_CACHE_FILES_VISIBILITIES") .split(',')
    assert(files(0) === "file:/foo.invalid.com:8080/tmp/testing#link")
    assert(timestamps(0)  === "0")
    assert(sizes(0)  === "0")
    assert(visibilities(0) === LocalResourceVisibility.PRIVATE.name())

    assert(files(1) === "file:/foo.invalid.com:8080/tmp/testing2#link2")
    assert(timestamps(1)  === "10")
    assert(sizes(1)  === "20")
    assert(visibilities(1) === LocalResourceVisibility.PRIVATE.name())
  }

  test("test addResource link null") {
    val distMgr = new MockClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.invalid.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    when(fs.getFileStatus(destPath)).thenReturn(new FileStatus())

    intercept[Exception] {
      distMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.FILE, null, 
        statCache, false)
    }
    assert(localResources.get("link") === None)
    assert(localResources.size === 0)
  }

  test("test addResource appmaster only") {
    val distMgr = new MockClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.invalid.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    val realFileStatus = new FileStatus(20, false, 1, 1024, 10, 30, null, "testOwner", 
      null, new Path("/tmp/testing"))
    when(fs.getFileStatus(destPath)).thenReturn(realFileStatus)

    distMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.ARCHIVE, "link", 
      statCache, true)
    val resource = localResources("link")
    assert(resource.getVisibility() === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(resource.getResource()) === destPath)
    assert(resource.getTimestamp() === 10)
    assert(resource.getSize() === 20)
    assert(resource.getType() === LocalResourceType.ARCHIVE)

    val env = new HashMap[String, String]()
    distMgr.setDistFilesEnv(env)
    assert(env.get("SPARK_YARN_CACHE_FILES") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_TIME_STAMPS") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_FILE_SIZES") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_VISIBILITIES") === None)

    distMgr.setDistArchivesEnv(env)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES") === None)
    assert(env.get("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES") === None)
  }

  test("test addResource archive") {
    val distMgr = new MockClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.invalid.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    val realFileStatus = new FileStatus(20, false, 1, 1024, 10, 30, null, "testOwner", 
      null, new Path("/tmp/testing"))
    when(fs.getFileStatus(destPath)).thenReturn(realFileStatus)

    distMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.ARCHIVE, "link", 
      statCache, false)
    val resource = localResources("link")
    assert(resource.getVisibility() === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(resource.getResource()) === destPath)
    assert(resource.getTimestamp() === 10)
    assert(resource.getSize() === 20)
    assert(resource.getType() === LocalResourceType.ARCHIVE)

    val env = new HashMap[String, String]()

    distMgr.setDistArchivesEnv(env)
    assert(env("SPARK_YARN_CACHE_ARCHIVES") === "file:/foo.invalid.com:8080/tmp/testing#link")
    assert(env("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS") === "10")
    assert(env("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES") === "20")
    assert(env("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES") === LocalResourceVisibility.PRIVATE.name())

    distMgr.setDistFilesEnv(env)
    assert(env.get("SPARK_YARN_CACHE_FILES") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_TIME_STAMPS") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_FILE_SIZES") === None)
    assert(env.get("SPARK_YARN_CACHE_FILES_VISIBILITIES") === None)
  }


}
