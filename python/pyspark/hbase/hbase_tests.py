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

import sys

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

# from pyspark.conf import SparkConf
# from pyspark.context import SparkContext
# from pyspark.files import SparkFiles
# from pyspark.serializers import read_int, BatchedSerializer, MarshalSerializer, PickleSerializer
#
# from pyspark.tests import PySparkTestCase

# class HBaseUnitTests(PySparkTestCase):

from pyspark.hbase.hbase import *


class HBaseTest:

    def __init__(self):
        from pyspark.context import SparkContext
        sc = SparkContext('local[4]', 'PythonTest')
        self._hbctx = HBaseSQLContext(sc)

        self.staging_table = "StagingTable"
        self.hb_staging_table = "HbStagingTable"
        self.test_table = "TestTable"
        self.hb_test_table = "HbTestTable"
        self.load_file = "sql/hbase/src/test/resources/testTable.csv"

    def _ctx(self):
        return self._hbctx

    def create_test_tables(self):
        create_sql = """CREATE TABLE %s(strcol STRING, bytecol String, shortcol String,intcol String,
        longcol string, floatcol string, doublecol string, PRIMARY KEY(doublecol, strcol, intcol))
        MAPPED BY (%s, COLS=[bytecol=cf1.hbytecol,
        shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])""" \
        % (self.staging_table, self.hb_staging_table)
        print("%s" % create_sql)
        self._ctx().sql(create_sql).toRdd().collect()
        print("Created tables %s and %s" % (self.staging_table, self.hb_staging_table))

        create_sql = """CREATE TABLE %s(strcol STRING, bytecol Byte, shortcol short, intcol int,
            longcol long, floatcol float, doublecol double, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY (%s, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])""" \
        % (self.test_table, self.hb_test_table)
        print("%s" % create_sql)
        self._ctx().sql(create_sql).toRdd().collect()
        print("Created tables %s and %s" % (self.test_table, self.hb_test_table))

        self._ctx().sql(create_sql)

    # def load_data(self , staging_table, test_table, load_file):
    def load_data(self, load_file):
        sql = "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s" % (load_file, self.staging_table)
        print("%s" % sql)
        self._ctx().sql(sql).toRdd().collect()
        print("Loaded data into %s" % (self.staging_table))

        sql = """insert into %s select cast(strcol as string),
            cast(bytecol as tinyint), cast(shortcol as smallint), cast(intcol as int),
            cast (longcol as bigint), cast(floatcol as float), cast(doublecol as double)
            from %s""" % (self.test_table, self.staging_table)
        print("%s" % sql)
        self._ctx().sql(sql).toRdd().collect()
        print("Inserted data from %s into %s" % (self.staging_table, self.test_table))

    def _test(self):
        self.create_test_tables()
        self.load_data(self.load_file)

    if __name__ == "__main__":
        test = HBaseTest()
        test._test()
