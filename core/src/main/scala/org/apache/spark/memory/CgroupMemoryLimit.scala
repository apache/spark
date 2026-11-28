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

package org.apache.spark.memory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Reads a bounded Linux cgroup memory limit, if one is visible to the process. */
private[spark] object CgroupMemoryLimit {
  private val ProcSelfCgroup = Paths.get("/proc/self/cgroup")
  private val V1Root = Paths.get("/sys/fs/cgroup/memory")
  private val V2Root = Paths.get("/sys/fs/cgroup")
  private val UnlimitedV1Threshold = Long.MaxValue / 2

  def currentLimitBytes(): Option[Long] = {
    try {
      val lines = Files.readAllLines(ProcSelfCgroup, StandardCharsets.UTF_8).asScala.toSeq
      v2Path(lines).flatMap(path => readLimit(resolve(V2Root, path, "memory.max")))
        .orElse(v1MemoryPath(lines).flatMap { path =>
          readLimit(resolve(V1Root, path, "memory.limit_in_bytes"))
        })
        .orElse(readLimit(V2Root.resolve("memory.max")))
        .orElse(readLimit(V1Root.resolve("memory.limit_in_bytes")))
    } catch {
      case NonFatal(_) => None
    }
  }

  private[memory] def parseLimit(value: String): Option[Long] = {
    try {
      if (value.trim == "max") {
        None
      } else {
        val limit = value.trim.toLong
        if (limit > 0L && limit < UnlimitedV1Threshold) Some(limit) else None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private[memory] def exceedsLimit(heapMax: Long, offHeapSize: Long, limit: Long): Boolean =
    heapMax > 0L && offHeapSize > 0L && heapMax > limit - offHeapSize

  private[memory] def v2Path(lines: Seq[String]): Option[String] =
    lines.collectFirst { case line if line.startsWith("0::") => line.substring(3) }

  private[memory] def v1MemoryPath(lines: Seq[String]): Option[String] =
    lines.iterator.flatMap { line =>
      line.split(":", 3) match {
        case Array(_, controllers, path) if controllers.split(",").contains("memory") =>
          Some(path)
        case _ => None
      }
    }.toSeq.headOption

  private def readLimit(path: Path): Option[Long] = {
    if (!Files.isRegularFile(path)) return None
    parseLimit(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
  }

  private def resolve(root: Path, cgroupPath: String, file: String): Path =
    root.resolve(cgroupPath.stripPrefix("/")).resolve(file)
}
