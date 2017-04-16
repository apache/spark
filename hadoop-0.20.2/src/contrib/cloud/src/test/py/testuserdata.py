# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tempfile
import unittest

from hadoop.cloud.cluster import InstanceUserData

class TestInstanceUserData(unittest.TestCase):

  def test_replacement(self):
    file = tempfile.NamedTemporaryFile()
    file.write("Contents go here")
    file.flush()
    self.assertEqual("Contents go here",
                     InstanceUserData(file.name, {}).read())
    self.assertEqual("Contents were here",
                     InstanceUserData(file.name, { "go": "were"}).read())
    self.assertEqual("Contents  here",
                     InstanceUserData(file.name, { "go": None}).read())
    file.close()

  def test_read_file_url(self):
    file = tempfile.NamedTemporaryFile()
    file.write("Contents go here")
    file.flush()
    self.assertEqual("Contents go here",
                     InstanceUserData("file://%s" % file.name, {}).read())
    file.close()

if __name__ == '__main__':
  unittest.main()
