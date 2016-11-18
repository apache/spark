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

package org.apache.spark.cloud

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants._
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem

import org.apache.spark.SparkFunSuite

/**
 * Force load in Hadoop Swift classes and some dependencies.
 * Dependency problems should be picked up at compile time; runtime may
 * identify problems with transitive libraries and FileSystem service registration
 * (i.e. the mapping from `"swift://"` to the `SwiftNativeFileSystem` instance").
 **/
private[cloud] class SwiftInstantiationSuite extends SparkFunSuite {

  test("Create Swift native FS class") {
    new SwiftNativeFileSystem()
  }

  test("Instantiate Swift FS") {
    // create a spoof swift endpoint configuration
    val conf = new Configuration()

    def opt(key: String, value: String): Unit = {
      conf.set(s"fs.swift.service.example$key", value)
    }
    def opts(options: Seq[(String, String)]): Unit = {
      options.foreach(e => opt(e._1, e._2))
    }

    opts(Seq(
      (DOT_USERNAME, "user"),
      (DOT_PASSWORD, "passwd"),
      (DOT_AUTH_URL, "http://example.org/v2.0/")
    ))
    val fs = FileSystem.newInstance(new URI("swift://test.example"), conf)
    logInfo(s"Loaded FS $fs")
    assert(fs.isInstanceOf[SwiftNativeFileSystem], s"Not a swift FS: $fs")
  }

}
