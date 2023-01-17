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
import unittest
from pyspark.url import sanitize_url


class UrlTests(unittest.TestCase):
    def test_sanitize_url_valid(self):
        self.assertEqual(sanitize_url("https://domain:9000/path/?qpk1=qpv1"), "https://domain:9000/path/")
        self.assertEqual(sanitize_url("https://domain:9000/path"), "https://domain:9000/path")
        self.assertEqual(sanitize_url("https://domain:9000"), "https://domain:9000")
        self.assertEqual(sanitize_url("www.domain.com"), "www.domain.com")
        self.assertEqual(sanitize_url("foo"), "foo")

    def test_sanitize_url_invalid(self):
        self.assertRaises(AssertionError, lambda: sanitize_url(None))
        self.assertRaises(AssertionError, lambda: sanitize_url(2))
