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


from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql import functions as sf
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase


class NearestByJoinTestsMixin:
    """Mixin run against both classic (`ReusedSQLTestCase`) and Connect
    (`ReusedConnectTestCase`) to ensure parity between the two paths."""

    @property
    def users(self):
        return self.spark.createDataFrame(
            [(1, 10.0), (2, 20.0), (3, 30.0)], ["user_id", "score"]
        )

    @property
    def products(self):
        return self.spark.createDataFrame(
            [("A", 11.0), ("B", 22.0), ("C", 5.0)], ["product", "pscore"]
        )

    def test_inner_similarity_k1(self):
        users, products = self.users, self.products
        result = users.nearestByJoin(
            products,
            -sf.abs(users.score - products.pscore),
            numResults=1,
            mode="approx",
            direction="similarity",
        ).select("user_id", "product").orderBy("user_id")
        assertDataFrameEqual(
            result,
            [Row(user_id=1, product="A"), Row(user_id=2, product="B"), Row(user_id=3, product="B")],
        )

    def test_inner_distance_k2(self):
        users, products = self.users, self.products
        result = users.nearestByJoin(
            products,
            sf.abs(users.score - products.pscore),
            numResults=2,
            mode="approx",
            direction="distance",
        ).select("user_id", "product").orderBy("user_id", "product")
        assertDataFrameEqual(
            result,
            [
                Row(user_id=1, product="A"),
                Row(user_id=1, product="C"),
                Row(user_id=2, product="A"),
                Row(user_id=2, product="B"),
                Row(user_id=3, product="A"),
                Row(user_id=3, product="B"),
            ],
        )

    def test_left_outer_with_empty_right(self):
        users, products = self.users, self.products
        empty = products.filter(sf.lit(False))
        result = users.nearestByJoin(
            empty,
            -sf.abs(users.score - empty.pscore),
            numResults=1,
            mode="exact",
            direction="similarity",
            joinType="leftouter",
        ).select("user_id", "product").orderBy("user_id")
        assertDataFrameEqual(
            result,
            [Row(user_id=1, product=None), Row(user_id=2, product=None), Row(user_id=3, product=None)],
        )

    def test_select_star_schema_has_no_internal_columns(self):
        users, products = self.users, self.products
        result = users.nearestByJoin(
            products,
            -sf.abs(users.score - products.pscore),
            numResults=1,
            mode="exact",
            direction="similarity",
        )
        # No `__qid`, `__nearest_matches__`, or other rewrite-internal columns leak through.
        assert sorted(result.columns) == ["product", "pscore", "score", "user_id"]

    def test_invalid_num_results_low(self):
        users, products = self.users, self.products
        with self.assertRaises(AnalysisException) as pe:
            users.nearestByJoin(
                products,
                -sf.abs(users.score - products.pscore),
                numResults=0,
                mode="approx",
                direction="similarity",
            )
        self.check_error(
            exception=pe.exception,
            errorClass="NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
            messageParameters={"numResults": "0", "min": "1", "max": "100000"},
        )

    def test_invalid_num_results_high(self):
        users, products = self.users, self.products
        with self.assertRaises(AnalysisException) as pe:
            users.nearestByJoin(
                products,
                -sf.abs(users.score - products.pscore),
                numResults=200000,
                mode="approx",
                direction="similarity",
            )
        self.check_error(
            exception=pe.exception,
            errorClass="NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
            messageParameters={"numResults": "200000", "min": "1", "max": "100000"},
        )

    def test_invalid_join_type(self):
        users, products = self.users, self.products
        with self.assertRaises(AnalysisException) as pe:
            users.nearestByJoin(
                products,
                -sf.abs(users.score - products.pscore),
                numResults=1,
                mode="approx",
                direction="similarity",
                joinType="outer",
            )
        self.check_error(
            exception=pe.exception,
            errorClass="NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE",
            messageParameters={"joinType": "outer", "supported": "'INNER', 'LEFT OUTER'"},
        )

    def test_invalid_mode(self):
        users, products = self.users, self.products
        with self.assertRaises(AnalysisException) as pe:
            users.nearestByJoin(
                products,
                -sf.abs(users.score - products.pscore),
                numResults=1,
                mode="bogus",
                direction="similarity",
            )
        self.check_error(
            exception=pe.exception,
            errorClass="NEAREST_BY_JOIN.UNSUPPORTED_MODE",
            messageParameters={"mode": "bogus", "supported": "'approx', 'exact'"},
        )

    def test_invalid_direction(self):
        users, products = self.users, self.products
        with self.assertRaises(AnalysisException) as pe:
            users.nearestByJoin(
                products,
                -sf.abs(users.score - products.pscore),
                numResults=1,
                mode="approx",
                direction="elsewhere",
            )
        self.check_error(
            exception=pe.exception,
            errorClass="NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION",
            messageParameters={
                "direction": "elsewhere",
                "supported": "'distance', 'similarity'",
            },
        )


class NearestByJoinTests(NearestByJoinTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
