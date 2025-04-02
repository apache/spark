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

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.config._

class ClientDistributedCacheManagerSuite extends SparkFunSuite with MockitoSugar {

  class MockClientDistributedCacheManager extends ClientDistributedCacheManager {
    override def getVisibility(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]):
        LocalResourceVisibility = {
      LocalResourceVisibility.PRIVATE
    }
  }

  test("SPARK-44272: test addResource added FileStatus to statCache and getVisibility can read" +
    " from statCache") {
    val distMgr = new ClientDistributedCacheManager() {
      override private[yarn] def getFileStatus(fs: FileSystem, uri: URI,
        statCache: mutable.Map[URI, FileStatus]): FileStatus = {
        statCache.getOrElseUpdate(uri, new FileStatus())
      }
    }
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPathA = new Path("file:///foo.invalid.com:8080/tmp/A")
    val localResources = HashMap[String, LocalResource]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    distMgr.addResource(fs, conf, destPathA, localResources, LocalResourceType.FILE, "link",
      statCache, false)
    assert(statCache.size === 2)
    assert(statCache.contains(destPathA.toUri))
    assert(statCache.contains(destPathA.getParent.toUri))

    val destPathB = new Path("file:///foo.invalid.com:8080/tmp/B")
    distMgr.addResource(fs, conf, destPathB, localResources, LocalResourceType.FILE, "link",
      statCache, false)
    assert(statCache.size === 3)
    assert(statCache.contains(destPathB.toUri))

    val destPathC = new Path("file:///foo.invalid.com:8080/root/C")
    distMgr.addResource(fs, conf, destPathC, localResources, LocalResourceType.FILE, "link",
      statCache, false)
    assert(statCache.size === 5)
    assert(statCache.contains(destPathC.toUri))
    assert(statCache.contains(destPathC.getParent.toUri))
  }

  test("SPARK-44272: test getParentURI") {
    val distMgr = new ClientDistributedCacheManager()
    val scheme = "file"
    val userInfo = "user"
    val host = "foo.com"
    val port = 8080
    val path = "/tmp/testing"
    val uri = new URI(scheme, userInfo, host, port, path, null, null)
    val parentURI = distMgr.getParentURI(uri)
    assert(uri.getScheme === parentURI.getScheme)
    assert(uri.getUserInfo === parentURI.getUserInfo)
    assert(uri.getHost === parentURI.getHost)
    assert(uri.getPort === parentURI.getPort)
    assert(new Path(uri.getPath).getParent.toString === parentURI.getPath)

    val rootPath = "/"
    val parentRootURI = distMgr.getParentURI(
      new URI(scheme, userInfo, host, port, rootPath, null, null))
    assert(parentRootURI === null)
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
    assert(resource.getResource().toPath === destPath)
    assert(resource.getTimestamp() === 0)
    assert(resource.getSize() === 0)
    assert(resource.getType() === LocalResourceType.FILE)

    val sparkConf = new SparkConf(false)
    distMgr.updateConfiguration(sparkConf)
    assert(sparkConf.get(CACHED_FILES) === Seq("file:/foo.invalid.com:8080/tmp/testing#link"))
    assert(sparkConf.get(CACHED_FILES_TIMESTAMPS) === Seq(0L))
    assert(sparkConf.get(CACHED_FILES_SIZES) === Seq(0L))
    assert(sparkConf.get(CACHED_FILES_VISIBILITIES) === Seq(LocalResourceVisibility.PRIVATE.name()))
    assert(sparkConf.get(CACHED_FILES_TYPES) === Seq(LocalResourceType.FILE.name()))

    // add another one and verify both there and order correct
    val realFileStatus = new FileStatus(20, false, 1, 1024, 10, 30, null, "testOwner",
      null, new Path("/tmp/testing2"))
    val destPath2 = new Path("file:///foo.invalid.com:8080/tmp/testing2")
    when(fs.getFileStatus(destPath2)).thenReturn(realFileStatus)
    distMgr.addResource(fs, conf, destPath2, localResources, LocalResourceType.FILE, "link2",
      statCache, false)
    val resource2 = localResources("link2")
    assert(resource2.getVisibility() === LocalResourceVisibility.PRIVATE)
    assert(resource2.getResource().toPath === destPath2)
    assert(resource2.getTimestamp() === 10)
    assert(resource2.getSize() === 20)
    assert(resource2.getType() === LocalResourceType.FILE)

    val sparkConf2 = new SparkConf(false)
    distMgr.updateConfiguration(sparkConf2)

    val files = sparkConf2.get(CACHED_FILES)
    val sizes = sparkConf2.get(CACHED_FILES_SIZES)
    val timestamps = sparkConf2.get(CACHED_FILES_TIMESTAMPS)
    val visibilities = sparkConf2.get(CACHED_FILES_VISIBILITIES)

    assert(files(0) === "file:/foo.invalid.com:8080/tmp/testing#link")
    assert(timestamps(0)  === 0)
    assert(sizes(0)  === 0)
    assert(visibilities(0) === LocalResourceVisibility.PRIVATE.name())

    assert(files(1) === "file:/foo.invalid.com:8080/tmp/testing2#link2")
    assert(timestamps(1)  === 10)
    assert(sizes(1)  === 20)
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
    assert(resource.getResource().toPath === destPath)
    assert(resource.getTimestamp() === 10)
    assert(resource.getSize() === 20)
    assert(resource.getType() === LocalResourceType.ARCHIVE)

    val sparkConf = new SparkConf(false)
    distMgr.updateConfiguration(sparkConf)
    assert(sparkConf.get(CACHED_FILES) === Nil)
    assert(sparkConf.get(CACHED_FILES_TIMESTAMPS) === Nil)
    assert(sparkConf.get(CACHED_FILES_SIZES) === Nil)
    assert(sparkConf.get(CACHED_FILES_VISIBILITIES) === Nil)
    assert(sparkConf.get(CACHED_FILES_TYPES) === Nil)
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
    assert(resource.getResource().toPath === destPath)
    assert(resource.getTimestamp() === 10)
    assert(resource.getSize() === 20)
    assert(resource.getType() === LocalResourceType.ARCHIVE)

    val sparkConf = new SparkConf(false)
    distMgr.updateConfiguration(sparkConf)
    assert(sparkConf.get(CACHED_FILES) === Seq("file:/foo.invalid.com:8080/tmp/testing#link"))
    assert(sparkConf.get(CACHED_FILES_SIZES) === Seq(20L))
    assert(sparkConf.get(CACHED_FILES_TIMESTAMPS) === Seq(10L))
    assert(sparkConf.get(CACHED_FILES_VISIBILITIES) === Seq(LocalResourceVisibility.PRIVATE.name()))
    assert(sparkConf.get(CACHED_FILES_TYPES) === Seq(LocalResourceType.ARCHIVE.name()))
  }

}
