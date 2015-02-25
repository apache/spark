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

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import javax.servlet.http.HttpServletRequest
import org.mockito.Mockito.{when}
import org.apache.hadoop.fs.Path
import org.apache.spark.ui.SparkUI
import scala.collection.mutable

class HistoryServerSuite extends FunSuite with ShouldMatchers with MockitoSugar {
	
  val historyServer = mock[HistoryServer]
  val request = mock[HttpServletRequest]
  
  test("generate history page with relative links") { 
    val ui = mock[SparkUI]
    val link = "/history/app1.html"
    val info = new ApplicationHistoryInfo("1", "app1", 0, 2, 1, "xxx", new Path("/tmp"), ui )
    val sampleHistory = mutable.HashMap("app1" -&gt; info)
    when(historyServer.appIdToInfo).thenReturn(sampleHistory)
    when(ui.basePath).thenReturn(link)
    when(historyServer.getAddress).thenReturn("http://localhost:123")
    val page = new HistoryPage(historyServer)
    
    //when
    val response = page.render(request)
    
    //then
   val expectedLink = response \\ "a"
   expectedLink.size should equal(1)
   val hrefAttr = expectedLink(0).attribute("href")
   hrefAttr.get.toString should equal(link)
  }
}
