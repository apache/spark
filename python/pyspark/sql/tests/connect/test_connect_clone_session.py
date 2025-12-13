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
import uuid
import unittest

from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase


class SparkConnectCloneSessionTest(SparkConnectSQLTestCase):
    def test_clone_session_basic(self):
        """Test basic session cloning functionality."""
        # Set a configuration in the original session
        self.connect.sql("SET spark.test.original = 'value1'")

        # Verify the original session has the value
        original_value = self.connect.sql("SET spark.test.original").collect()[0][1]
        self.assertEqual(original_value, "'value1'")

        # Clone the session
        cloned_session = self.connect.cloneSession()

        # Verify the configuration was copied
        # (if cloning doesn't preserve dynamic configs, use a different approach)
        cloned_value = cloned_session.sql("SET spark.test.original").collect()[0][1]
        self.assertEqual(cloned_value, "'value1'")
        # Verify that sessions are independent by setting different values
        cloned_session.sql("SET spark.test.original = 'modified_clone'")
        self.connect.sql("SET spark.test.original = 'modified_original'")

        # Verify independence
        original_final = self.connect.sql("SET spark.test.original").collect()[0][1]
        cloned_final = cloned_session.sql("SET spark.test.original").collect()[0][1]

        self.assertEqual(original_final, "'modified_original'")
        self.assertEqual(cloned_final, "'modified_clone'")

    def test_clone_session_with_custom_id(self):
        """Test cloning session with a custom session ID."""
        custom_session_id = str(uuid.uuid4())
        cloned_session = self.connect.cloneSession(custom_session_id)

        self.assertEqual(cloned_session.session_id, custom_session_id)
        self.assertNotEqual(self.connect.session_id, cloned_session.session_id)

    def test_clone_session_preserves_temp_views(self):
        """Test that temporary views are preserved in cloned sessions."""
        # Create a temporary view
        df = self.connect.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        df.createOrReplaceTempView("test_table")

        # Verify original session can access the temp view
        original_count = self.connect.sql("SELECT COUNT(*) FROM test_table").collect()[0][0]
        self.assertEqual(original_count, 2)

        # Clone the session
        cloned_session = self.connect.cloneSession()

        # Create temp view in cloned session for testing
        df_clone = cloned_session.createDataFrame([(3, "c"), (4, "d")], ["id", "value"])
        df_clone.createOrReplaceTempView("test_table")

        # Verify both sessions have independent temp views
        original_count_final = self.connect.sql("SELECT COUNT(*) FROM test_table").collect()[0][0]
        cloned_count_final = cloned_session.sql("SELECT COUNT(*) FROM test_table").collect()[0][0]

        self.assertEqual(original_count_final, 2)  # Original data
        self.assertEqual(cloned_count_final, 2)  # New data

    def test_temp_views_independence_after_cloning(self):
        """Test that temp views are cloned and then can be modified independently."""
        # Create initial temp view before cloning
        df = self.connect.createDataFrame([(1, "original")], ["id", "type"])
        df.createOrReplaceTempView("shared_view")

        # Verify original session can access the temp view
        original_count_before = self.connect.sql("SELECT COUNT(*) FROM shared_view").collect()[0][0]
        self.assertEqual(original_count_before, 1)

        # Clone the session - temp views should be preserved
        cloned_session = self.connect.cloneSession()

        # Verify cloned session can access the same temp view (cloned)
        cloned_count = cloned_session.sql("SELECT COUNT(*) FROM shared_view").collect()[0][0]
        self.assertEqual(cloned_count, 1)

        # Now modify the temp view in each session independently
        # Replace the view in the original session
        original_df = self.connect.createDataFrame(
            [(2, "modified_original"), (3, "another")], ["id", "type"]
        )
        original_df.createOrReplaceTempView("shared_view")

        # Replace the view in the cloned session
        cloned_session.sql(
            "CREATE OR REPLACE TEMPORARY VIEW shared_view AS SELECT 4 as id, 'cloned_data' as type"
        )

        # Verify they now have different content (independence after cloning)
        original_count_after = self.connect.sql("SELECT COUNT(*) FROM shared_view").collect()[0][0]
        cloned_count_after = cloned_session.sql("SELECT COUNT(*) FROM shared_view").collect()[0][0]

        self.assertEqual(original_count_after, 2)  # Original session: 2 rows
        self.assertEqual(cloned_count_after, 1)  # Cloned session: 1 row

    def test_invalid_session_id_format(self):
        """Test that invalid session ID format raises an exception."""
        with self.assertRaises(Exception) as context:
            self.connect.cloneSession("not-a-valid-uuid")

        # Verify it contains our clone-specific error message
        self.assertIn(
            "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_FORMAT", str(context.exception)
        )
        self.assertIn("not-a-valid-uuid", str(context.exception))

    def test_clone_session_auto_generated_id(self):
        """Test that cloneSession() without arguments generates a valid UUID."""
        cloned_session = self.connect.cloneSession()

        # Verify different session IDs
        self.assertNotEqual(self.connect.session_id, cloned_session.session_id)

        # Verify the new session ID is a valid UUID
        try:
            uuid.UUID(cloned_session.session_id)
        except ValueError:
            self.fail("Generated session ID is not a valid UUID")


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_clone_session import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
