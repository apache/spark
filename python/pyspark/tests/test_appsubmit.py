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
import re
import shutil
import subprocess
import tempfile
import unittest
import zipfile

from pyspark.testing.sqlutils import SPARK_HOME


class SparkSubmitTests(unittest.TestCase):
    def setUp(self):
        self.programDir = tempfile.mkdtemp()
        tmp_dir = tempfile.gettempdir()
        self.sparkSubmit = [
            os.path.join(SPARK_HOME, "bin", "spark-submit"),
            "--conf",
            "spark.driver.extraJavaOptions=-Djava.io.tmpdir={0}".format(tmp_dir),
            "--conf",
            "spark.executor.extraJavaOptions=-Djava.io.tmpdir={0}".format(tmp_dir),
            "--conf",
            "spark.python.unix.domain.socket.enabled=false",
        ]

    def tearDown(self):
        shutil.rmtree(self.programDir)

    def createTempFile(self, name, content, dir=None):
        """
        Create a temp file with the given name and content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r"^ *\|", re.MULTILINE)
        content = re.sub(pattern, "", content.strip())
        if dir is None:
            path = os.path.join(self.programDir, name)
        else:
            os.makedirs(os.path.join(self.programDir, dir))
            path = os.path.join(self.programDir, dir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def createFileInZip(self, name, content, ext=".zip", dir=None, zip_name=None):
        """
        Create a zip archive containing a file with the given content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r"^ *\|", re.MULTILINE)
        content = re.sub(pattern, "", content.strip())
        if dir is None:
            path = os.path.join(self.programDir, name + ext)
        else:
            path = os.path.join(self.programDir, dir, zip_name + ext)
        zip = zipfile.ZipFile(path, "w")
        zip.writestr(name, content)
        zip.close()
        return path

    def create_spark_package(self, artifact_name):
        group_id, artifact_id, version = artifact_name.split(":")
        self.createTempFile(
            "%s-%s.pom" % (artifact_id, version),
            (
                """
            |<?xml version="1.0" encoding="UTF-8"?>
            |<project xmlns="http://maven.apache.org/POM/4.0.0"
            |       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            |       xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
            |       http://maven.apache.org/xsd/maven-4.0.0.xsd">
            |   <modelVersion>4.0.0</modelVersion>
            |   <groupId>%s</groupId>
            |   <artifactId>%s</artifactId>
            |   <version>%s</version>
            |</project>
            """
                % (group_id, artifact_id, version)
            ).lstrip(),
            os.path.join(group_id, artifact_id, version),
        )
        self.createFileInZip(
            "%s.py" % artifact_id,
            """
            |def myfunc(x):
            |    return x + 1
            """,
            ".jar",
            os.path.join(group_id, artifact_id, version),
            "%s-%s" % (artifact_id, version),
        )

    def test_single_script(self):
        """Submit and test a single script file"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect())
            """,
        )
        proc = subprocess.Popen(self.sparkSubmit + [script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out.decode("utf-8"))

    def test_script_with_local_functions(self):
        """Submit and test a single script file calling a global function"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 3
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(foo).collect())
            """,
        )
        proc = subprocess.Popen(self.sparkSubmit + [script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[3, 6, 9]", out.decode("utf-8"))

    def test_module_dependency(self):
        """Submit and test a script with a dependency on another module"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """,
        )
        zip = self.createFileInZip(
            "mylib.py",
            """
            |def myfunc(x):
            |    return x + 1
            """,
        )
        proc = subprocess.Popen(
            self.sparkSubmit + ["--py-files", zip, script], stdout=subprocess.PIPE
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode("utf-8"))

    def test_module_dependency_on_cluster(self):
        """Submit and test a script with a dependency on another module on a cluster"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """,
        )
        zip = self.createFileInZip(
            "mylib.py",
            """
            |def myfunc(x):
            |    return x + 1
            """,
        )
        proc = subprocess.Popen(
            self.sparkSubmit + ["--py-files", zip, "--master", "local-cluster[1,1,1024]", script],
            stdout=subprocess.PIPE,
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode("utf-8"))

    def test_package_dependency(self):
        """Submit and test a script with a dependency on a Spark Package"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """,
        )
        self.create_spark_package("a:mylib:0.1")
        proc = subprocess.Popen(
            self.sparkSubmit
            + ["--packages", "a:mylib:0.1", "--repositories", "file:" + self.programDir, script],
            stdout=subprocess.PIPE,
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode("utf-8"))

    def test_package_dependency_on_cluster(self):
        """Submit and test a script with a dependency on a Spark Package on a cluster"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """,
        )
        self.create_spark_package("a:mylib:0.1")
        proc = subprocess.Popen(
            self.sparkSubmit
            + [
                "--packages",
                "a:mylib:0.1",
                "--repositories",
                "file:" + self.programDir,
                "--master",
                "local-cluster[1,1,1024]",
                script,
            ],
            stdout=subprocess.PIPE,
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode("utf-8"))

    def test_single_script_on_cluster(self):
        """Submit and test a single script on a cluster"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 2
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(foo).collect())
            """,
        )
        # this will fail if you have different spark.executor.memory
        # in conf/spark-defaults.conf
        proc = subprocess.Popen(
            self.sparkSubmit + ["--master", "local-cluster[1,1,1024]", script],
            stdout=subprocess.PIPE,
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out.decode("utf-8"))

    def test_user_configuration(self):
        """Make sure user configuration is respected (SPARK-19307)"""
        script = self.createTempFile(
            "test.py",
            """
            |from pyspark import SparkConf, SparkContext
            |
            |conf = SparkConf().set("spark.test_config", "1")
            |sc = SparkContext(conf = conf)
            |try:
            |    if sc._conf.get("spark.test_config") != "1":
            |        raise RuntimeError("Cannot find spark.test_config in SparkContext's conf.")
            |finally:
            |    sc.stop()
            """,
        )
        proc = subprocess.Popen(
            self.sparkSubmit + ["--master", "local", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode, msg="Process failed with error:\n {0}".format(out))


if __name__ == "__main__":
    from pyspark.tests.test_appsubmit import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
