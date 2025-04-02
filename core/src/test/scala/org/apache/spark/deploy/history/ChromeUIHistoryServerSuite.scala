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

package org.apache.spark.deploy.history

import org.openqa.selenium.WebDriver
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}

import org.apache.spark.internal.config.History.HybridStoreDiskBackend
import org.apache.spark.tags.{ChromeUITest, ExtendedLevelDBTest, WebBrowserTest}


/**
 * Tests for HistoryServer with Chrome.
 */
abstract class ChromeUIHistoryServerSuite
  extends RealBrowserUIHistoryServerSuite("webdriver.chrome.driver") {

  override var webDriver: WebDriver = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val chromeOptions = new ChromeOptions
    chromeOptions.addArguments("--headless", "--disable-gpu")
    webDriver = new ChromeDriver(chromeOptions)
  }

  override def afterAll(): Unit = {
    try {
      if (webDriver != null) {
        webDriver.quit()
      }
    } finally {
      super.afterAll()
    }
  }
}

@WebBrowserTest
@ChromeUITest
@ExtendedLevelDBTest
class LevelDBBackendChromeUIHistoryServerSuite extends ChromeUIHistoryServerSuite {
  override protected def diskBackend: HybridStoreDiskBackend.Value = HybridStoreDiskBackend.LEVELDB
}

@WebBrowserTest
@ChromeUITest
class RocksDBBackendChromeUIHistoryServerSuite extends ChromeUIHistoryServerSuite {
  override protected def diskBackend: HybridStoreDiskBackend.Value = HybridStoreDiskBackend.ROCKSDB
}
