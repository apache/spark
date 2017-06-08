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

package org.apache.spark.api.r

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.SparkFunSuite

class RBackendSuite extends SparkFunSuite {
  test("close() clears jvmObjectTracker") {
    val backend = new RBackend
    val tracker = backend.jvmObjectTracker
    val id = tracker.addAndGetId(new Object)
    backend.close()
    assert(tracker.get(id) === None)
    assert(tracker.size === 0)
  }

  test("read and write bigint in the buffer") {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val tracker = new JVMObjectTracker
    SerDe.writeObject(dos, 1380742793415240L.asInstanceOf[Object],
      tracker)
    val buf = bos.toByteArray
    val bis = new ByteArrayInputStream(buf)
    val dis = new DataInputStream(bis)
    val data = SerDe.readObject(dis, tracker)
    assert(data.asInstanceOf[Double] === 1380742793415240L)
    bos.close()
    bis.close()
    dos.close()
    dis.close()
  }
}
