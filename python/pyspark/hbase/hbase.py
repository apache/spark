#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from py4j.protocol import Py4JError
import traceback

from pyspark.sql import *
from py4j.java_gateway import java_import, JavaGateway, GatewayClient

__all__ = ["StringType", "BinaryType", "BooleanType", "TimestampType", "DecimalType",
           "DoubleType", "FloatType", "ByteType", "IntegerType", "LongType",
           "ShortType", "ArrayType", "MapType", "StructField", "StructType",
           "SQLContext", "HBaseSQLContext", "SchemaRDD", "Row"]


class HBaseSQLContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in Hive.

    Configuration for Hive is read from hive-site.xml on the classpath.
    It supports running both SQL and HiveQL commands.
    """

    def __init__(self, sparkContext):
        """Create a new HiveContext.

    @param sparkContext: The SparkContext to wrap.
    @param hbaseContext: An optional JVM Scala HBaseSQLContext. If set, we do not instatiate a new
    HBaseSQLContext in the JVM, instead we make all calls to this object.
    """
    SQLContext.__init__(self, sparkContext)

    # if hbaseContext:
    #  self._scala_HBaseSQLContext = hbaseContext
    # else:
    self._scala_HBaseSQLContext = self._get_hbase_ctx()
    print("HbaseContext is %s" % self._scala_HBaseSQLContext)

    @property
    def _ssql_ctx(self):
        # try:
        if self._scala_HBaseSQLContext is None:
            # if not hasattr(self, '_scala_HBaseSQLContext'):
            print ("loading hbase context ..")
            self._scala_HBaseSQLContext = self._get_hbase_ctx()

        if self._scala_SQLContext is None:
            # self._jvm.SQLContext(self._jsc.sc())
            self._scala_SQLContext = self._scala_HBaseSQLContext
        else:
            print("We already have hbase context")

        print vars(self)
        return self._scala_HBaseSQLContext
        # except Py4JError as e:
        # import sys
        # traceback.print_stack(file=sys.stdout)
        #     print ("Nice error .. %s " %e)
        #     print(e)
        #     raise Exception(""
        #                     "HbaseSQLContext not found: You must build Spark with Hbase.", e)

    def _get_hbase_ctx(self):
        print("sc=%s conf=%s" % (self._jsc.sc(), self._jsc.sc().configuration))
    java_import(self._sc._gateway.jvm, 'org.apache.spark.sql.hbase.*')
    # java_import(self._sc._gateway.jvm,'org.apache.hadoop.conf.Configuration')
    # java_import(self._sc._gateway.jvm,'org.apache.hadoop.hbase.*')
    # java_import(self._sc._gateway.jvm,'org.apache.hadoop.hbase.util.*')
    # java_import(self._sc._gateway.jvm,'org.apache.hadoop.hbase.client.*')
    # java_import(self._sc._gateway.jvm,'org.apache.hadoop.hbase.filter.*')
    return self._jvm.HBaseSQLContext(self._jsc.sc())
    # return self._jvm.SQLContext(self._jsc.sc())

    # class HBaseSchemaRDD(SchemaRDD):
    #   def createTable(self, tableName, overwrite=False):
    #     """Inserts the contents of this SchemaRDD into the specified table.
    #
    #     Optionally overwriting any existing data.
    #     """
    #     self._jschema_rdd.createTable(tableName, overwrite)
