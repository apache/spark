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

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.{LocalFileSystem => HadoopLocalFileSystem}
import org.apache.hadoop.fs.{Path, PathFilter, RemoteIterator, LocatedFileStatus}

/**
 * FileSystem for Spark that takes file ordering into account. This is because
 * Hadoop MapReduce doesn't care about file ordering and HDFS may provide the
 * file parts in order, but native filesystems may not. As Spark has a notion of
 * ordering in RDDs (e.g. sortByKey), reading partitions out of order destroys
 * these notions.
 *
 * We only need to override listLocatedStatus as this is called from
 * FileInputFormat.singleThreadedListStatus and LocatedFileStatusFetcher.
 */
class LocalFileSystem extends HadoopLocalFileSystem {
  override 
  def listLocatedStatus(path: Path) : RemoteIterator[LocatedFileStatus] = {
    val listing = super.listLocatedStatus(path)
    val builder = new ArrayBuffer[LocatedFileStatus]()
    while(listing.hasNext) {
      builder += listing.next
    }
    val sorted = builder.toArray.sortWith{ (lhs, rhs) => {
      lhs.getPath().compareTo(rhs.getPath()) < 0
    }}
    new RemoteIterator[LocatedFileStatus] {
      var i = 0
      val arr = sorted

      def hasNext = i < arr.length

      def next = {
        val ret = arr(i)
        i = i + 1
        ret
      }
    }
  }
}
