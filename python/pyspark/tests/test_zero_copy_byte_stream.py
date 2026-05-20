# -*- encoding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import threading
import unittest

from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream


class ZeroCopyByteStreamTests(unittest.TestCase):
    """Tests for ZeroCopyByteStream."""

    # ---- Basic single-chunk reads (zero-copy fast path) ----

    def test_read_exact_chunk(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"hello"))
        result = stream.read(5)
        self.assertEqual(bytes(result), b"hello")

    def test_read_partial_chunk(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"hello world"))
        stream.finish()
        r1 = stream.read(5)
        r2 = stream.read(6)
        self.assertEqual(bytes(r1), b"hello")
        self.assertEqual(bytes(r2), b" world")
        # Check EOF read
        with self.assertRaises(EOFError):
            stream.read(1)

    def test_read_multiple_chunks_sequentially(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"aaa"))
        stream.add_next_chunk(memoryview(b"bbb"))
        stream.add_next_chunk(memoryview(b"ccc"))

        self.assertEqual(bytes(stream.read(3)), b"aaa")
        self.assertEqual(bytes(stream.read(3)), b"bbb")
        self.assertEqual(bytes(stream.read(3)), b"ccc")

    # ---- Cross-boundary reads (slow path with copy) ----

    def test_read_across_two_chunks(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"aaa"))
        stream.add_next_chunk(memoryview(b"bbb"))

        result = stream.read(6)
        self.assertEqual(bytes(result), b"aaabbb")

    def test_read_across_three_chunks(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"ab"))
        stream.add_next_chunk(memoryview(b"cd"))
        stream.add_next_chunk(memoryview(b"ef"))

        result = stream.read(6)
        self.assertEqual(bytes(result), b"abcdef")

    def test_read_partial_then_cross_boundary(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"aabb"))
        stream.add_next_chunk(memoryview(b"ccdd"))

        # Read first 2 bytes from chunk 1 (zero-copy)
        r1 = stream.read(2)
        self.assertEqual(bytes(r1), b"aa")

        # Read 4 bytes crossing chunk boundary
        r2 = stream.read(4)
        self.assertEqual(bytes(r2), b"bbcc")

        # Read remaining 2 bytes from chunk 2 (zero-copy)
        r3 = stream.read(2)
        self.assertEqual(bytes(r3), b"dd")

    def test_cross_boundary_read_consumes_full_middle_chunk(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"aa"))
        stream.add_next_chunk(memoryview(b"bb"))
        stream.add_next_chunk(memoryview(b"cc"))

        # Read 1 byte to offset into first chunk
        stream.read(1)

        # Read 5 bytes: 1 from chunk1 + 2 from chunk2 + 2 from chunk3
        result = stream.read(5)
        self.assertEqual(bytes(result), b"abbcc")

    # ---- EOF handling ----

    def test_eof_throws_eof_error(self):
        stream = ZeroCopyByteStream()
        stream.finish()

        self.assertTrue(stream.finished)
        with self.assertRaises(EOFError):
            stream.read(1)

    def test_eof_after_consuming_all_data(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"data"))
        stream.finish()

        self.assertFalse(stream.finished)

        result = stream.read(4)
        self.assertEqual(bytes(result), b"data")

        self.assertTrue(stream.finished)
        with self.assertRaises(EOFError):
            stream.read(1)

    def test_eof_with_out_of_bounds_read(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"data"))
        stream.finish()

        result = stream.read(3)
        self.assertEqual(bytes(result), b"dat")

        self.assertFalse(stream.finished)
        with self.assertRaises(EOFError):
            stream.read(2)

    def test_eof_during_cross_boundary_read(self):
        """EOF mid-cross-boundary read returns None."""
        stream = ZeroCopyByteStream(initial_view=memoryview(b"ab"))
        stream.add_next_chunk(memoryview(b"cd"))
        stream.finish()

        # Request more bytes than available; after consuming "abcd",
        # _try_read_bytes hits EOF and throws
        with self.assertRaises(EOFError):
            stream.read(5)

    def test_finished_property(self):
        stream = ZeroCopyByteStream(initial_view=memoryview(b"x"))
        self.assertFalse(stream.finished)

        stream.read(1)
        self.assertFalse(stream.finished)  # not yet marked EOF

        stream.finish()
        self.assertTrue(stream.finished)

    # ---- Threading / blocking behavior ----

    def test_read_blocks_until_chunk_available(self):
        stream = ZeroCopyByteStream()
        result = None

        def reader():
            nonlocal result
            result = stream.read(3)

        t = threading.Thread(target=reader)
        t.start()

        # Give reader time to block
        t.join(timeout=2)
        self.assertTrue(t.is_alive(), "Reader should be blocked waiting for data")

        stream.add_next_chunk(memoryview(b"abc"))
        t.join(timeout=2)
        self.assertFalse(t.is_alive(), "Reader should have unblocked")
        self.assertEqual(bytes(result), b"abc")

    def test_read_blocks_until_eof(self):
        stream = ZeroCopyByteStream()
        read_raised_eof = False
        read_called = threading.Event()

        def reader():
            nonlocal read_raised_eof
            read_called.set()
            try:
                stream.read(1)
            except EOFError:
                read_raised_eof = True

        t = threading.Thread(target=reader)
        t.start()
        read_called.wait()

        # Give reader time to block
        t.join(timeout=2)
        self.assertTrue(t.is_alive())

        stream.finish()
        t.join(timeout=2)
        self.assertFalse(t.is_alive())
        self.assertTrue(read_raised_eof)

    def test_cross_boundary_read_blocks_for_next_chunk(self):
        """Cross-boundary read blocks when second chunk isn't available yet."""
        stream = ZeroCopyByteStream(initial_view=memoryview(b"aa"))
        result = None

        def reader():
            nonlocal result
            result = stream.read(4)

        t = threading.Thread(target=reader)
        t.start()

        # Reader consumed "aa" but needs 2 more bytes
        t.join(timeout=2)
        self.assertTrue(t.is_alive(), "Reader should block waiting for more data")

        stream.add_next_chunk(memoryview(b"bb"))
        t.join(timeout=2)
        self.assertFalse(t.is_alive())
        self.assertEqual(bytes(result), b"aabb")

    def test_read_of_zero_bytes_succeeds_without_data(self):
        """Reading zero bytes should immediately return, even if no data is present"""
        stream = ZeroCopyByteStream(initial_view=None)

        res = stream.read(0)

        self.assertEqual(res, memoryview(b""))

    def test_negative_read_throws_value_error(self):
        stream = ZeroCopyByteStream(initial_view=None)

        with self.assertRaises(ValueError):
            stream.read(-1)

    # ---- add_next_chunk assertions ----

    def test_add_none_chunk_raises(self):
        stream = ZeroCopyByteStream()
        with self.assertRaises(TypeError):
            stream.add_next_chunk(None)

    def test_add_chunk_after_finish_raises(self):
        stream = ZeroCopyByteStream()
        stream.finish()
        with self.assertRaises(ValueError):
            stream.add_next_chunk(memoryview(b"data"))

    # ---- Initial view ----

    def test_no_initial_view(self):
        stream = ZeroCopyByteStream()
        stream.add_next_chunk(memoryview(b"hello"))
        result = stream.read(5)
        self.assertEqual(bytes(result), b"hello")

    def test_initial_view_none(self):
        stream = ZeroCopyByteStream(initial_view=None)
        stream.add_next_chunk(memoryview(b"test"))
        result = stream.read(4)
        self.assertEqual(bytes(result), b"test")

    def test_invalid_initial_view(self):
        with self.assertRaises(TypeError):
            ZeroCopyByteStream(initial_view=5)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
