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
package org.apache.spark.streaming.eventhubs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

/**
 * An DFS based OffsetStore implementation
 */
@SerialVersionUID(1L)
class DfsBasedOffsetStore(
    directory: String,
    namespace: String,
    name: String,
    partition: String) extends OffsetStore {

  var path: Path = null
  var fs: FileSystem = null

  override def open(): Unit = {
    if(fs == null) {
      path = new Path(directory + "/" + namespace + "/" + name + "/" + partition)
      fs = path.getFileSystem(new Configuration())
    }
  }

  override def write(offset: String): Unit = {
    val stream = fs.create(path, true)
    stream.writeUTF(offset)
    stream.close()
  }

  override def read(): String = {
    var offset:String = "-1"
    if(fs.exists(path)) {
      val stream = fs.open(path)
      offset = stream.readUTF()
      stream.close()
    }
    offset
  }

  override def close(): Unit = {
    if(fs != null) {
      fs.close()
      fs = null
    }
  }
}
