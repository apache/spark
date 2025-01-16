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

package org.apache.spark.deploy

import org.apache.spark.util.SslTestUtils

/**
 * This suite creates an external shuffle server and routes all shuffle fetches through it.
 * Note that failures in this suite may arise due to changes in Spark that invalidate expectations
 * set up in `ExternalShuffleBlockHandler`, such as changing the format of shuffle files or how
 * we hash files into folders.
 */
class SslExternalShuffleServiceSuite extends ExternalShuffleServiceSuite {

  override def beforeAll(): Unit = {
    SslTestUtils.updateWithSSLConfig(conf)
    super.beforeAll()
  }
}
