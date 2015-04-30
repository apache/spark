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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.mockito.Mockito.{when}
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import org.apache.spark.ui.SparkUI

class HistoryServerSuite extends FunSuite with Matchers with MockitoSugar {

  test("generate history page with relative links") {
    val historyServer = mock[HistoryServer]
    val request = mock[HttpServletRequest]
    val ui = mock[SparkUI]
    val link = "/history/app1"
    val info = new ApplicationHistoryInfo("app1", "app1", 0, 2, 1, "xxx", true)
    when(historyServer.getApplicationList()).thenReturn(Seq(info))
    when(ui.basePath).thenReturn(link)
    when(historyServer.getProviderConfig()).thenReturn(Map[String, String]())
    val page = new HistoryPage(historyServer)

    // when
    val response = page.render(request)

    // then
    val links = response \\ "a"
    val justHrefs = for {
      l <- links
      attrs <- l.attribute("href")
    } yield (attrs.toString)
    justHrefs should contain(link)
  }
}
