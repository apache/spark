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
import java.io.{IOException, FileOutputStream, OutputStream, File}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Test various code paths in DiskBlockObjectWriter
 */
class DiskBlockObjectWriterSuite extends FunSuite {

  private val conf = new SparkConf
  private val BUFFER_SIZE = 32 * 1024

  private def tempFile(): File = {
    val file = File.createTempFile("temp_", "block")
    // We dont want file to exist ! Just need a temp file name
    file.delete()
    file
  }

  private def createWriter(file: File = tempFile()) :
      (File, DiskBlockObjectWriter) = {
    file.deleteOnExit()

    (file, new DiskBlockObjectWriter(BlockId("test_1"), file,
      new JavaSerializer(conf), BUFFER_SIZE, (out: OutputStream) => out, true))
  }


  test("write after close should throw IOException") {
    val (file, bow) = createWriter()
    bow.write("test")
    bow.write("test1")
    assert(file.exists() && file.isFile)

    bow.commitAndClose()

    intercept[IOException] {
      bow.write("test2")
    }

    file.delete()
  }

  test("write after revert should throw IOException") {
    val (file, bow) = createWriter()
    bow.write("test")
    bow.write("test1")
    assert(file.exists() && file.isFile)

    bow.revertPartialWritesAndClose()

    intercept[IOException] {
      bow.write("test2")
    }

    file.delete()
  }

  test("create even if directory does not exist") {
    val dir = File.createTempFile("temp_", "dir")
    dir.delete()

    val file = new File(dir, "temp.file")
    file.deleteOnExit()

    val bow = new DiskBlockObjectWriter(BlockId("test_1"), file, new JavaSerializer(conf),
      BUFFER_SIZE, (out: OutputStream) => out, true)

    bow.write("test")
    assert(file.exists() && file.isFile)
    bow.commitAndClose()
    Utils.deleteRecursively(dir)
  }

  test("revert of new file should delete it") {
    val (file, bow) = createWriter()
    bow.write("test")
    bow.write("test1")
    assert(file.exists() && file.isFile)

    bow.revertPartialWritesAndClose()
    assert(! file.exists())
    // file.delete()
  }

  test("revert of existing file should revert it to previous state") {
    val (file, bow1) = createWriter()

    bow1.write("test")
    bow1.write("test1")
    assert(file.exists() && file.isFile)

    bow1.commitAndClose()
    val length = file.length()

    // reopen same file.
    val bow2 = createWriter(file)._2

    bow2.write("test3")
    bow2.write("test4")

    assert(file.exists() && file.isFile)

    bow2.revertPartialWritesAndClose()
    assert(file.exists())
    assert(length == file.length())
    file.delete()
  }

  test("revert of writer after close should delete if it did not exist earlier") {
    val (file, bow) = createWriter(tempFile())

    bow.write("test")
    bow.write("test1")
    assert(file.exists() && file.isFile)

    bow.commitAndClose()
    val length = file.length()

    assert(file.exists() && file.isFile)
    assert(length > 0)

    // Now revert the file, after it has been closed : should delete the file
    // since it did not exist earlier.
    bow.revertPartialWritesAndClose()
    assert(! file.exists())
    file.delete()
  }

  test("revert of writer after close should revert it to previous state") {
    val (file, bow1) = createWriter()

    bow1.write("test")
    bow1.write("test1")
    assert(file.exists() && file.isFile)

    bow1.commitAndClose()
    val length = file.length()

    // reopen same file.
    val bow2 = createWriter(file)._2

    bow2.write("test3")
    bow2.write("test4")

    bow2.commitAndClose()

    assert(file.exists() && file.isFile)
    assert(file.length() > length)

    // Now revert it : should get reverted back to previous state - after bow1
    bow2.revertPartialWritesAndClose()
    assert(file.exists())
    assert(length == file.length())
    file.delete()
  }

  test("If file changes before the open, throw exception instead of corrupting file") {
    val (file, bow) = createWriter()

    // Now modify the file from under bow
    val strm = new FileOutputStream(file, true)
    strm.write(0)
    strm.close()

    // try to write - must throw exception.
    intercept[IOException] {
      bow.write("test")
    }
    bow.revertPartialWritesAndClose()
    // must not have deleted the file : since we never opened it.
    assert(file.exists())
    file.delete()
  }

  test("If exception is thrown while close, must throw exception") {

    val file = tempFile()
    file.deleteOnExit()

    var disallowWrite = false
    class TestOutputStream(delegate: OutputStream) extends OutputStream {
      override def write(b: Int) {
        if (disallowWrite) throw new IOException("disallowed by config")
        delegate.write(b)
      }

      override def flush() {
        if (disallowWrite) throw new IOException("disallowed by config")
        delegate.flush()
      }

      override def close() {
        delegate.close()
      }
    }
    var customCompressor = (out: OutputStream) => new TestOutputStream(out)

    val bow = new DiskBlockObjectWriter(BlockId("test_1"), file,
        new JavaSerializer(conf), BUFFER_SIZE, customCompressor, true)

    bow.write("test1")
    bow.write("test2")

    var afterWrite = false
    // try to write - must throw exception at close
    intercept[IOException] {
      bow.write("test3")
      afterWrite = true
      disallowWrite = true
      // The buffer should mean we dont need to write
      // bow.write("test4")
      bow.commitAndClose()
    }

    assert(afterWrite)
    bow.revertPartialWritesAndClose()
    assert(! file.exists())
  }

  test("If exception is thrown while revert, must still revert successfully") {

    val file = tempFile()
    file.deleteOnExit()

    var disallowWrite = false
    class TestOutputStream(delegate: OutputStream) extends OutputStream {
      override def write(b: Int) {
        if (disallowWrite) throw new IOException("disallowed by config")
        delegate.write(b)
      }

      override def flush() {
        if (disallowWrite) throw new IOException("disallowed by config")
        delegate.flush()
      }

      override def close() {
        delegate.close()
      }
    }
    var customCompressor = (out: OutputStream) => new TestOutputStream(out)

    val bow1 = new DiskBlockObjectWriter(BlockId("test_1"), file,
      new JavaSerializer(conf), BUFFER_SIZE, customCompressor, true)


    bow1.write("test1")
    bow1.write("test2")

    bow1.commitAndClose()
    val length = file.length()

    val bow2 = new DiskBlockObjectWriter(BlockId("test_1"), file,
      new JavaSerializer(conf), BUFFER_SIZE, customCompressor, true)

    bow2.write("test3")
    bow2.write("test4")
    // must cause exception to be raised, but should be handled gracefully and reverted.
    // Note, it depends on current impl which does a flush within code.
    disallowWrite = true
    // The buffer should mean we dont need to write
    // bow1.write("test4")
    bow2.revertPartialWritesAndClose()

    assert(file.exists())
    assert(file.length() == length)
    file.delete()
  }
}
