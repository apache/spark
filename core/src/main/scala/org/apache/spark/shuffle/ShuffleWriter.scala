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

package org.apache.spark.shuffle

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.scheduler.MapStatus

/**
 * Obtained inside a map task to write out records to the shuffle system.
 */
private[spark] abstract class ShuffleWriter[K, V] {
  /**
   * Write a sequence of records to this task's output.  This should write all data
   * to temporary files, but return (temporaryFile, destinationFile) pairs for each
   * file written.  The temporary files will get moved to their destination or deleted
   * by the [[ShuffleOutputCoordinator]].  Note that for the ShuffleOutputCoordinator
   * to work correctly, each attempt *must* have the exact same set of destination files.
   * If the temporary file is empty, the ShuffleWriter does not have to create the file -- however
   * it *must* still be in the result Seq, just pointing to a non-existent file.
   */
  @throws[IOException]
  def write(records: Iterator[Product2[K, V]]): Seq[TmpDestShuffleFile]

  /** Close this writer, passing along whether the map completed */
  def stop(success: Boolean): Option[MapStatus]

  /**
   * Returns the file with a random UUID appended.  Useful for getting a tmp file in the same
   * dir which can be atomically renamed to final destination file.
   */
  def tmpShuffleFile(file: File): File = ShuffleWriter.tmpShuffleFile(file)
}


private[spark] object ShuffleWriter {
  /**
    * Returns the file with a random UUID appended.  Useful for getting a tmp file in the same
    * dir which can be atomically renamed to final destination file.
    */
  def tmpShuffleFile(file: File): File = new File(file.getAbsolutePath + "." + UUID.randomUUID())
}

/**
 * The location of one shuffle file written by a [[ShuffleWriter]].  Holds both the temporary
 * file, which is written to by the ShuffleWriter itself, and the destination file, where the
 * file should get moved by the [[ShuffleOutputCoordinator]].  The ShuffleWriter is responsible
 * for specifying both locations, though it only writes the temp file.
 */
private[shuffle] case class TmpDestShuffleFile(
  val tmpFile: File,
  val dstFile: File
)
