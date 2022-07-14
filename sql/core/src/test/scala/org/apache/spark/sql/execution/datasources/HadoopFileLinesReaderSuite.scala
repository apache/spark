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

package org.apache.spark.sql.execution.datasources

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.test.SharedSparkSession

class HadoopFileLinesReaderSuite extends SharedSparkSession {
  def getLines(
      path: File,
      text: String,
      ranges: Seq[(Long, Long)],
      delimiter: Option[String] = None,
      conf: Option[Configuration] = None): Seq[String] = {
    val delimOpt = delimiter.map(_.getBytes(StandardCharsets.UTF_8))
    Files.write(path.toPath, text.getBytes(StandardCharsets.UTF_8))

    val lines = ranges.flatMap { case (start, length) =>
      val file = PartitionedFile(InternalRow.empty, path.getCanonicalPath, start, length)
      val hadoopConf = conf.getOrElse(spark.sessionState.newHadoopConf())
      val reader = new HadoopFileLinesReader(file, delimOpt, hadoopConf)

      reader.map(_.toString)
    }

    lines
  }

  test("A split ends at the delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a\r\nb", ranges = Seq((0, 1), (1, 3)))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split cuts the delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a\r\nb", ranges = Seq((0, 2), (2, 2)))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split ends at the end of the delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a\r\nb", ranges = Seq((0, 3), (3, 1)))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split covers two lines") {
    withTempPath { path =>
      val lines = getLines(path, text = "a\r\nb", ranges = Seq((0, 4), (4, 1)))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split ends at the custom delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a^_^b", ranges = Seq((0, 1), (1, 4)), Some("^_^"))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split slices the custom delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a^_^b", ranges = Seq((0, 2), (2, 3)), Some("^_^"))
      assert(lines == Seq("a", "b"))
    }
  }

  test("The first split covers the first line and the custom delimiter") {
    withTempPath { path =>
      val lines = getLines(path, text = "a^_^b", ranges = Seq((0, 4), (4, 1)), Some("^_^"))
      assert(lines == Seq("a", "b"))
    }
  }

  test("A split cuts the first line") {
    withTempPath { path =>
      val lines = getLines(path, text = "abc,def", ranges = Seq((0, 1)), Some(","))
      assert(lines == Seq("abc"))
    }
  }

  test("The split cuts both lines") {
    withTempPath { path =>
      val lines = getLines(path, text = "abc,def", ranges = Seq((2, 2)), Some(","))
      assert(lines == Seq("def"))
    }
  }

  test("io.file.buffer.size is less than line length") {
    withSQLConf("io.file.buffer.size" -> "2") {
      withTempPath { path =>
        val lines = getLines(path, text = "abcdef\n123456", ranges = Seq((4, 4), (8, 5)))
        assert(lines == Seq("123456"))
      }
    }
  }

  test("line cannot be longer than line.maxlength") {
    withSQLConf("mapreduce.input.linerecordreader.line.maxlength" -> "5") {
      withTempPath { path =>
        val lines = getLines(path, text = "abcdef\n1234", ranges = Seq((0, 15)))
        assert(lines == Seq("1234"))
      }
    }
  }

  test("default delimiter is 0xd or 0xa or 0xd0xa") {
    withTempPath { path =>
      val lines = getLines(path, text = "1\r2\n3\r\n4", ranges = Seq((0, 3), (3, 5)))
      assert(lines == Seq("1", "2", "3", "4"))
    }
  }
}
