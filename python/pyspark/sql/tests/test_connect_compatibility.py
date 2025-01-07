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
import inspect
import functools

from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.sql.classic.dataframe import DataFrame as ClassicDataFrame
from pyspark.sql.classic.column import Column as ClassicColumn
from pyspark.sql.session import SparkSession as ClassicSparkSession
from pyspark.sql.catalog import Catalog as ClassicCatalog
from pyspark.sql.readwriter import DataFrameReader as ClassicDataFrameReader
from pyspark.sql.readwriter import DataFrameWriter as ClassicDataFrameWriter
from pyspark.sql.readwriter import DataFrameWriterV2 as ClassicDataFrameWriterV2
from pyspark.sql.window import Window as ClassicWindow
from pyspark.sql.window import WindowSpec as ClassicWindowSpec
import pyspark.sql.functions as ClassicFunctions
from pyspark.sql.group import GroupedData as ClassicGroupedData
import pyspark.sql.avro.functions as ClassicAvro
import pyspark.sql.protobuf.functions as ClassicProtobuf
from pyspark.sql.streaming.query import StreamingQuery as ClassicStreamingQuery
from pyspark.sql.streaming.query import StreamingQueryManager as ClassicStreamingQueryManager
from pyspark.sql.streaming.readwriter import DataStreamReader as ClassicDataStreamReader
from pyspark.sql.streaming.readwriter import DataStreamWriter as ClassicDataStreamWriter

if should_test_connect:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.column import Column as ConnectColumn
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
    from pyspark.sql.connect.catalog import Catalog as ConnectCatalog
    from pyspark.sql.connect.readwriter import DataFrameReader as ConnectDataFrameReader
    from pyspark.sql.connect.readwriter import DataFrameWriter as ConnectDataFrameWriter
    from pyspark.sql.connect.readwriter import DataFrameWriterV2 as ConnectDataFrameWriterV2
    from pyspark.sql.connect.window import Window as ConnectWindow
    from pyspark.sql.connect.window import WindowSpec as ConnectWindowSpec
    import pyspark.sql.connect.functions as ConnectFunctions
    from pyspark.sql.connect.group import GroupedData as ConnectGroupedData
    import pyspark.sql.connect.avro.functions as ConnectAvro
    import pyspark.sql.connect.protobuf.functions as ConnectProtobuf
    from pyspark.sql.connect.streaming.query import StreamingQuery as ConnectStreamingQuery
    from pyspark.sql.connect.streaming.query import (
        StreamingQueryManager as ConnectStreamingQueryManager,
    )
    from pyspark.sql.connect.streaming.readwriter import DataStreamReader as ConnectDataStreamReader
    from pyspark.sql.connect.streaming.readwriter import DataStreamWriter as ConnectDataStreamWriter


class ConnectCompatibilityTestsMixin:
    def get_public_methods(self, cls):
        """Get public methods of a class."""
        methods = {}
        for name, method in inspect.getmembers(cls):
            if (
                inspect.isfunction(method) or isinstance(method, functools._lru_cache_wrapper)
            ) and not name.startswith("_"):
                if getattr(method, "_remote_only", False):
                    methods[name] = None
                else:
                    methods[name] = method
        return methods

    def get_public_properties(self, cls):
        """Get public properties of a class."""
        return {
            name: member
            for name, member in inspect.getmembers(cls)
            if (isinstance(member, property) or isinstance(member, functools.cached_property))
            and not name.startswith("_")
        }

    def compare_method_signatures(self, classic_cls, connect_cls, cls_name):
        """Compare method signatures between classic and connect classes."""
        classic_methods = self.get_public_methods(classic_cls)
        connect_methods = self.get_public_methods(connect_cls)

        common_methods = set(classic_methods.keys()) & set(connect_methods.keys())

        for method in common_methods:
            # Skip non-callable, Spark Connect-specific methods
            if classic_methods[method] is None or connect_methods[method] is None:
                continue

            classic_signature = inspect.signature(classic_methods[method])
            connect_signature = inspect.signature(connect_methods[method])

            # Cannot support RDD arguments from Spark Connect
            has_rdd_arguments = ("createDataFrame", "xml", "json")
            if method not in has_rdd_arguments:
                self.assertEqual(
                    classic_signature,
                    connect_signature,
                    f"Signature mismatch in {cls_name} method '{method}'\n"
                    f"Classic: {classic_signature}\n"
                    f"Connect: {connect_signature}",
                )

    def compare_property_lists(
        self,
        classic_cls,
        connect_cls,
        cls_name,
        expected_missing_connect_properties,
        expected_missing_classic_properties,
    ):
        """Compare properties between classic and connect classes."""
        classic_properties = self.get_public_properties(classic_cls)
        connect_properties = self.get_public_properties(connect_cls)

        # Identify missing properties
        classic_only_properties = set(classic_properties.keys()) - set(connect_properties.keys())
        connect_only_properties = set(connect_properties.keys()) - set(classic_properties.keys())

        # Compare the actual missing properties with the expected ones
        self.assertEqual(
            classic_only_properties,
            expected_missing_connect_properties,
            f"{cls_name}: Unexpected missing properties in Connect: {classic_only_properties}",
        )

        # Reverse compatibility check
        self.assertEqual(
            connect_only_properties,
            expected_missing_classic_properties,
            f"{cls_name}: Unexpected missing properties in Classic: {connect_only_properties}",
        )

    def check_missing_methods(
        self,
        classic_cls,
        connect_cls,
        cls_name,
        expected_missing_connect_methods,
        expected_missing_classic_methods,
    ):
        """Check for expected missing methods between classic and connect classes."""
        classic_methods = self.get_public_methods(classic_cls)
        connect_methods = self.get_public_methods(connect_cls)

        # Identify missing methods
        classic_only_methods = {
            name
            for name, method in classic_methods.items()
            if name not in connect_methods or method is None
        }
        connect_only_methods = set(connect_methods.keys()) - set(classic_methods.keys())

        # Compare the actual missing methods with the expected ones
        self.assertEqual(
            classic_only_methods,
            expected_missing_connect_methods,
            f"{cls_name}: Unexpected missing methods in Connect: {classic_only_methods}",
        )

        # Reverse compatibility check
        self.assertEqual(
            connect_only_methods,
            expected_missing_classic_methods,
            f"{cls_name}: Unexpected missing methods in Classic: {connect_only_methods}",
        )

    def check_compatibility(
        self,
        classic_cls,
        connect_cls,
        cls_name,
        expected_missing_connect_properties,
        expected_missing_classic_properties,
        expected_missing_connect_methods,
        expected_missing_classic_methods,
    ):
        """
        Main method for checking compatibility between classic and connect.

        This method performs the following checks:
        - API signature comparison between classic and connect classes.
        - Property comparison, identifying any missing properties between classic and connect.
        - Method comparison, identifying any missing methods between classic and connect.

        Parameters
        ----------
        classic_cls : type
            The classic class to compare.
        connect_cls : type
            The connect class to compare.
        cls_name : str
            The name of the class.
        expected_missing_connect_properties : set
            A set of properties expected to be missing in the connect class.
        expected_missing_classic_properties : set
            A set of properties expected to be missing in the classic class.
        expected_missing_connect_methods : set
            A set of methods expected to be missing in the connect class.
        expected_missing_classic_methods : set
            A set of methods expected to be missing in the classic class.
        """
        self.compare_method_signatures(classic_cls, connect_cls, cls_name)
        self.compare_property_lists(
            classic_cls,
            connect_cls,
            cls_name,
            expected_missing_connect_properties,
            expected_missing_classic_properties,
        )
        self.check_missing_methods(
            classic_cls,
            connect_cls,
            cls_name,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_dataframe_compatibility(self):
        """Test DataFrame compatibility between classic and connect."""
        expected_missing_connect_properties = {"sql_ctx"}
        expected_missing_classic_properties = {"is_cached"}
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataFrame,
            ConnectDataFrame,
            "DataFrame",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_column_compatibility(self):
        """Test Column compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = {"to_plan"}
        self.check_compatibility(
            ClassicColumn,
            ConnectColumn,
            "Column",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_spark_session_compatibility(self):
        """Test SparkSession compatibility between classic and connect."""
        expected_missing_connect_properties = {"sparkContext"}
        expected_missing_classic_properties = {"is_stopped", "session_id"}
        expected_missing_connect_methods = {
            "addArtifact",
            "addArtifacts",
            "clearProgressHandlers",
            "copyFromLocalToFs",
            "interruptOperation",
            "newSession",
            "registerProgressHandler",
            "removeProgressHandler",
        }
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicSparkSession,
            ConnectSparkSession,
            "SparkSession",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_catalog_compatibility(self):
        """Test Catalog compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicCatalog,
            ConnectCatalog,
            "Catalog",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_dataframe_reader_compatibility(self):
        """Test DataFrameReader compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataFrameReader,
            ConnectDataFrameReader,
            "DataFrameReader",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_dataframe_writer_compatibility(self):
        """Test DataFrameWriter compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataFrameWriter,
            ConnectDataFrameWriter,
            "DataFrameWriter",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_dataframe_writer_v2_compatibility(self):
        """Test DataFrameWriterV2 compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataFrameWriterV2,
            ConnectDataFrameWriterV2,
            "DataFrameWriterV2",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_window_compatibility(self):
        """Test Window compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicWindow,
            ConnectWindow,
            "Window",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_window_spec_compatibility(self):
        """Test WindowSpec compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicWindowSpec,
            ConnectWindowSpec,
            "WindowSpec",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_functions_compatibility(self):
        """Test Functions compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = {"check_dependencies"}
        self.check_compatibility(
            ClassicFunctions,
            ConnectFunctions,
            "Functions",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_grouping_compatibility(self):
        """Test Grouping compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = {"transformWithStateInPandas"}
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicGroupedData,
            ConnectGroupedData,
            "Grouping",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_avro_compatibility(self):
        """Test Avro compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        # The current supported Avro functions are only `from_avro` and `to_avro`.
        # The missing methods belows are just util functions that imported to implement them.
        expected_missing_connect_methods = {
            "try_remote_avro_functions",
            "cast",
            "get_active_spark_context",
        }
        expected_missing_classic_methods = {"lit", "check_dependencies"}
        self.check_compatibility(
            ClassicAvro,
            ConnectAvro,
            "Avro",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_streaming_query_compatibility(self):
        """Test Streaming Query compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicStreamingQuery,
            ConnectStreamingQuery,
            "StreamingQuery",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_protobuf_compatibility(self):
        """Test Protobuf compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        # The current supported Avro functions are only `from_protobuf` and `to_protobuf`.
        # The missing methods belows are just util functions that imported to implement them.
        expected_missing_connect_methods = {
            "cast",
            "try_remote_protobuf_functions",
            "get_active_spark_context",
        }
        expected_missing_classic_methods = {"lit", "check_dependencies"}
        self.check_compatibility(
            ClassicProtobuf,
            ConnectProtobuf,
            "Protobuf",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_streaming_query_manager_compatibility(self):
        """Test Streaming Query Manager compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = {"close"}
        self.check_compatibility(
            ClassicStreamingQueryManager,
            ConnectStreamingQueryManager,
            "StreamingQueryManager",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_streaming_reader_compatibility(self):
        """Test Data Stream Reader compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataStreamReader,
            ConnectDataStreamReader,
            "DataStreamReader",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )

    def test_streaming_writer_compatibility(self):
        """Test Data Stream Writer compatibility between classic and connect."""
        expected_missing_connect_properties = set()
        expected_missing_classic_properties = set()
        expected_missing_connect_methods = set()
        expected_missing_classic_methods = set()
        self.check_compatibility(
            ClassicDataStreamWriter,
            ConnectDataStreamWriter,
            "DataStreamWriter",
            expected_missing_connect_properties,
            expected_missing_classic_properties,
            expected_missing_connect_methods,
            expected_missing_classic_methods,
        )


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ConnectCompatibilityTests(ConnectCompatibilityTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_connect_compatibility import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
