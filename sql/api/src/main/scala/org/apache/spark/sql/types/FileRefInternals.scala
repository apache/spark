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
package org.apache.spark.sql.types

/**
 * Access to the inline bytes of a [[FileRef]], which its public API does not expose. Spark
 * carries them so that a FILE value converts to a FileRef and back without loss.
 */
private[sql] object FileRefInternals {

  /** Creates a FileRef with all fields of FILE, including the inline bytes. */
  def create(
      uri: String,
      offset: java.lang.Long,
      size: java.lang.Long,
      contentType: String,
      checksum: String,
      inline: Array[Byte]): FileRef =
    new FileRef(uri, offset, size, contentType, checksum, inline)

  /** Returns the inline bytes of `file`, or null. */
  def inline(file: FileRef): Array[Byte] = file.inline()
}
