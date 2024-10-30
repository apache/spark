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
package org.apache.spark.sql.connect.client

import java.net.URL
import java.nio.file.Paths

import ammonite.repl.api.Session
import ammonite.runtime.SpecialClassLoader

import org.apache.spark.sql.Artifact

/**
 * A special [[ClassFinder]] for the Ammonite REPL to handle in-memory class files.
 *
 * @param session
 */
class AmmoniteClassFinder(session: Session) extends ClassFinder {

  override def findClasses(): Iterator[Artifact] = {
    session.frames.iterator.flatMap { frame =>
      val classloader = frame.classloader.asInstanceOf[SpecialClassLoader]
      val signatures: Seq[(Either[String, URL], Long)] = classloader.classpathSignature
      signatures.iterator.collect { case (Left(name), _) =>
        val parts = name.split('.')
        parts(parts.length - 1) += ".class"
        val path = Paths.get(parts.head, parts.tail: _*)
        val bytes = classloader.newFileDict(name)
        Artifact.newClassArtifact(path, new Artifact.InMemory(bytes))
      }
    }
  }
}
