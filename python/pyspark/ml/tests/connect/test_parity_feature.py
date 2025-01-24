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

from pyspark.ml.tests.test_feature import FeatureTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class FeatureParityTests(FeatureTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Need to support.")
    def test_idf(self):
        super().test_idf()

    @unittest.skip("Need to support.")
    def test_ngram(self):
        super().test_ngram()

    @unittest.skip("Need to support.")
    def test_count_vectorizer_with_binary(self):
        super().test_count_vectorizer_with_binary()

    @unittest.skip("Need to support.")
    def test_count_vectorizer_with_maxDF(self):
        super().test_count_vectorizer_with_maxDF()

    @unittest.skip("Need to support.")
    def test_count_vectorizer_from_vocab(self):
        super().test_count_vectorizer_from_vocab()

    @unittest.skip("Need to support.")
    def test_rformula_force_index_label(self):
        super().test_rformula_force_index_label()

    @unittest.skip("Need to support.")
    def test_rformula_string_indexer_order_type(self):
        super().test_rformula_string_indexer_order_type()

    @unittest.skip("Need to support.")
    def test_string_indexer_handle_invalid(self):
        super().test_string_indexer_handle_invalid()

    @unittest.skip("Need to support.")
    def test_string_indexer_from_labels(self):
        super().test_string_indexer_from_labels()

    @unittest.skip("Need to support.")
    def test_target_encoder_binary(self):
        super().test_target_encoder_binary()

    @unittest.skip("Need to support.")
    def test_target_encoder_continuous(self):
        super().test_target_encoder_continuous()

    @unittest.skip("Need to support.")
    def test_vector_size_hint(self):
        super().test_vector_size_hint()

    @unittest.skip("Need to support.")
    def test_apply_binary_term_freqs(self):
        super().test_apply_binary_term_freqs()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_parity_feature import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
