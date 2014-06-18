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

import java.io.File

/**
 * Represent an external object written by specific BlockStore
 */

abstract class ObjectId {
  def id: String
  override def toString = "(block object %d)".format(id)
  override def hashCode = id.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: ObjectId => getClass == o.getClass && id.equals(o.id)
    case _ => false
  }
}

case class FileObjectId(file: File) extends ObjectId {
  def id = file.getName
  override def toString = "(File block object %d)".format(id)
}

object FileObjectId {
  def toFileObjectId(id: ObjectId): Option[FileObjectId] = {
    if (id.isInstanceOf[FileObjectId]) {
      Some(id.asInstanceOf[FileObjectId])
    } else {
      None
    }
  }
}
