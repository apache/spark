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
Python datasets for the hello_world pipeline.

- `customers` is a materialized view computed from a small in-line batch dataset.
- `orders` is a streaming table that incrementally ingests the JSON files in `data/orders/`.
  Each run processes only the files that arrived since the previous run.
"""

from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

# SDP also injects the session as `spark`; assigning it explicitly keeps linters happy.
spark = SparkSession.active()

ORDERS_DIR = (Path(__file__).parent.parent / "data" / "orders").resolve().as_uri()


@dp.materialized_view
def customers() -> DataFrame:
    return spark.createDataFrame(
        [(1, "Alice", "US"), (2, "Bob", "DE"), (3, "Carol", "US")],
        "customer_id INT, name STRING, country STRING",
    )


@dp.table(comment="Raw orders, ingested incrementally from data/orders/.")
def orders() -> DataFrame:
    return spark.readStream.schema(
        "order_id INT, customer_id INT, amount DOUBLE, order_ts TIMESTAMP"
    ).json(ORDERS_DIR)
