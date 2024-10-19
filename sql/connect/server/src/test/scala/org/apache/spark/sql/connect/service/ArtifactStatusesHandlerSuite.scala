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
package org.apache.spark.sql.connect.service

import java.util.UUID

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver
import org.apache.commons.codec.digest.DigestUtils.sha256Hex

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ArtifactStatusesResponse
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

private class DummyStreamObserver(p: Promise[ArtifactStatusesResponse])
    extends StreamObserver[ArtifactStatusesResponse] {
  override def onNext(v: ArtifactStatusesResponse): Unit = p.success(v)
  override def onError(throwable: Throwable): Unit = throw throwable
  override def onCompleted(): Unit = {}
}

class ArtifactStatusesHandlerSuite extends SharedSparkSession with ResourceHelper {

  val sessionId = UUID.randomUUID().toString

  def getStatuses(names: Seq[String], exist: Set[String]): ArtifactStatusesResponse = {
    val promise = Promise[ArtifactStatusesResponse]()
    val handler = new SparkConnectArtifactStatusesHandler(new DummyStreamObserver(promise)) {
      override protected def cacheExists(
          userId: String,
          sessionId: String,
          previoslySeenSessionId: Option[String],
          hash: String): Boolean = {
        exist.contains(hash)
      }
    }
    val context = proto.UserContext
      .newBuilder()
      .setUserId("user1")
      .build()
    val request = proto.ArtifactStatusesRequest
      .newBuilder()
      .setUserContext(context)
      .setSessionId(sessionId)
      .addAllNames(names.asJava)
      .build()
    handler.handle(request)
    ThreadUtils.awaitResult(promise.future, 5.seconds)
  }

  private def id(name: String): String = "cache/" + sha256Hex(name)

  test("non-existent artifact") {
    val response = getStatuses(names = Seq(id("name1")), exist = Set.empty)
    assert(response.getStatusesCount === 1)
    assert(response.getStatusesMap.get(id("name1")).getExists === false)
  }

  test("single artifact") {
    val response = getStatuses(names = Seq(id("name1")), exist = Set(sha256Hex("name1")))
    assert(response.getStatusesCount === 1)
    assert(response.getStatusesMap.get(id("name1")).getExists)
  }

  test("multiple artifacts") {
    val response = getStatuses(
      names = Seq("name1", "name2", "name3").map(id),
      exist = Set("name2", "name3").map(sha256Hex))
    assert(response.getStatusesCount === 3)
    assert(!response.getStatusesMap.get(id("name1")).getExists)
    assert(response.getStatusesMap.get(id("name2")).getExists)
    assert(response.getStatusesMap.get(id("name3")).getExists)
  }
}
