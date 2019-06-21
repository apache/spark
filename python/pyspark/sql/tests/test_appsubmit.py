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

import os
import subprocess
import tempfile

import py4j

from pyspark import SparkContext
from pyspark.tests.test_appsubmit import SparkSubmitTests


class HiveSparkSubmitTests(SparkSubmitTests):

    @classmethod
    def setUpClass(cls):
        # get a SparkContext to check for availability of Hive
        sc = SparkContext('local[4]', cls.__name__)
        cls.hive_available = True
        try:
            sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.hive_available = False
        except TypeError:
            cls.hive_available = False
        finally:
            # we don't need this SparkContext for the test
            sc.stop()

    def setUp(self):
        super(HiveSparkSubmitTests, self).setUp()
        if not self.hive_available:
            self.skipTest("Hive is not available.")

    def test_hivecontext(self):
        # This test checks that HiveContext is using Hive metastore (SPARK-16224).
        # It sets a metastore url and checks if there is a derby dir created by
        # Hive metastore. If this derby dir exists, HiveContext is using
        # Hive metastore.
        metastore_path = os.path.join(tempfile.mkdtemp(), "spark16224_metastore_db")
        metastore_URL = "jdbc:derby:;databaseName=" + metastore_path + ";create=true"
        hive_site_dir = os.path.join(self.programDir, "conf")
        hive_site_file = self.createTempFile("hive-site.xml", ("""
            |<configuration>
            |  <property>
            |  <name>javax.jdo.option.ConnectionURL</name>
            |  <value>%s</value>
            |  </property>
            |</configuration>
            """ % metastore_URL).lstrip(), "conf")
        script = self.createTempFile("test.py", """
            |import os
            |
            |from pyspark.conf import SparkConf
            |from pyspark.context import SparkContext
            |from pyspark.sql import HiveContext
            |
            |conf = SparkConf()
            |sc = SparkContext(conf=conf)
            |hive_context = HiveContext(sc)
            |print(hive_context.sql("show databases").collect())
            """)
        proc = subprocess.Popen(
            self.sparkSubmit + ["--master", "local-cluster[1,1,1024]",
                                "--driver-class-path", hive_site_dir, script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("default", out.decode('utf-8'))
        self.assertTrue(os.path.exists(metastore_path))


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_appsubmit import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
