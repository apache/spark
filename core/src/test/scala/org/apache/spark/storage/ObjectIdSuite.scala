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

package org.apache.spark.storage

import org.scalatest.FunSuite
import java.io.File

class ObjectIdSuite extends FunSuite {
  def assertSame(id1: ObjectId, id2: ObjectId) {
    assert(id1.id === id2.id)
    assert(id1.hashCode === id2.hashCode)
    assert(id1 === id2)
  }

  def assertDifferent(id1: ObjectId, id2: ObjectId) {
    assert(id1.id != id2.id)
    assert(id1.hashCode != id2.hashCode)
    assert(id1 != id2)
  }

  test("file object id") {
    val file = new File("/tmpfile")
    val id = FileObjectId(file)
    assertSame(id, FileObjectId(new File("/tmpfile")))
    assertDifferent(id, FileObjectId(new File("/tmpfile2")))
    assert(id.id === file.getName)
    assert(id.file === file)
  }

  test("as instance") {
    val file = new File("/tmpfile")
    val fid = FileObjectId(file)
    val oid: ObjectId = fid
    val fid1 = FileObjectId.toFileObjectId(fid).get
    assertSame(fid, fid1)
    val fid2 = FileObjectId.toFileObjectId(oid).get
    assertSame(fid, fid2)
  }
}
