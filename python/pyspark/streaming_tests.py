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

"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.

This file will merged to tests.py. But for now, this file is separated to
focus to streaming test case

"""
from fileinput import input
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *


SPARK_HOME = os.environ["SPARK_HOME"]


class PySparkStreamingTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.ssc = StreamingContext(appName=class_name, duration=Seconds(1))

    def tearDown(self):
        self.ssc.stop()
        sys.path = self._old_sys_path


if __name__ == "__main__":
    unittest.main()
